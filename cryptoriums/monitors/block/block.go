package block

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"cosmossdk.io/log"
	abci "github.com/cometbft/cometbft/abci/types"
	rpchttp "github.com/cometbft/cometbft/rpc/client/http"
	coretypes "github.com/cometbft/cometbft/rpc/core/types"

	ctypes "github.com/cometbft/cometbft/types"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/tellor-io/layer/cryptoriums"
	"github.com/tellor-io/layer/x/oracle/types"
)

const (
	ComponentName     = "block_monitor"
	MetricErrCount    = "errors_total"
	MetricReportCount = "reports_total"
	TableName         = "reports"
)

type SQLDB struct{ *sql.DB }

func (s SQLDB) Exec(ctx context.Context, q string, args ...any) (sql.Result, error) {
	return s.DB.ExecContext(ctx, q, args...)
}
func (s SQLDB) Query(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	return s.DB.QueryContext(ctx, q, args...)
}
func (s SQLDB) Prepare(ctx context.Context, q string) (*sql.Stmt, error) {
	return s.DB.PrepareContext(ctx, q)
}

type Db interface {
	Exec(context.Context, string, ...any) (sql.Result, error)
	Query(context.Context, string, ...any) (*sql.Rows, error)
	Prepare(context.Context, string) (*sql.Stmt, error)
}

type DisputeEventHandler interface {
	HandleDisputeEvent(abci.Event)
}

type Config struct {
	Nodes  []string `yaml:"nodes"`
	InitDB bool     `yaml:"initDB"`
}

type Monitor struct {
	cfg         Config
	logger      log.Logger
	db          Db
	errCount    *prometheus.CounterVec
	reportCount prometheus.Counter
	subsActive  prometheus.Gauge
	mtx         sync.Mutex

	processedHeights map[int64]struct{}
	heightQueue      []int64

	disputeEventHandler DisputeEventHandler

	readyCh chan struct{}
}

func New(logger log.Logger, cfg Config, reg prometheus.Registerer, db Db, dh DisputeEventHandler) (*Monitor, error) {
	errCount := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Namespace: cryptoriums.MetricsNamespace,
		Subsystem: ComponentName,
		Name:      MetricErrCount,
		Help:      "Errors in " + ComponentName,
	}, []string{"error"},
	)
	reportCount := promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Namespace: cryptoriums.MetricsNamespace,
		Subsystem: ComponentName,
		Name:      MetricReportCount,
		Help:      "Reports inserted into ClickHouse",
	})
	subsActive := promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Namespace: cryptoriums.MetricsNamespace,
		Subsystem: ComponentName,
		Name:      "subscriptions_active",
		Help:      "Current number of active block subscriptions",
	})
	monitor := &Monitor{
		cfg:                 cfg,
		logger:              logger.With("component", ComponentName),
		db:                  db,
		errCount:            errCount,
		reportCount:         reportCount,
		subsActive:          subsActive,
		processedHeights:    make(map[int64]struct{}),
		readyCh:             make(chan struct{}, len(cfg.Nodes)),
		disputeEventHandler: dh,
	}

	return monitor, nil
}

func (m *Monitor) Start(ctx context.Context) error {

	if m.cfg.InitDB {
		if err := m.initDBTables(ctx); err != nil {
			return err
		}
	}
	var wg sync.WaitGroup
	for _, node := range m.cfg.Nodes {
		wg.Add(1)
		go func(node string) {
			defer wg.Done()
			m.subscribeNode(ctx, node)
		}(node)
	}
	<-ctx.Done()
	wg.Wait() // Wait for all subscription goroutines to exit
	m.logger.Info("Monitor stopped")
	return nil
}

func (m *Monitor) IsReady() {
	var count int
	for range m.readyCh {
		count++
		if count == len(m.cfg.Nodes) {
			return
		}
	}
}

func (m *Monitor) subscribeNode(ctx context.Context, node string) {
	retryInterval := time.Second * 2
	ticker := time.NewTicker(retryInterval)
	defer ticker.Stop()

retryLoop:
	for {
		select {
		case <-ctx.Done():
			return
		default:
			client, err := rpchttp.New(node, "/websocket")
			if err != nil {
				m.logger.Error("failed to create CometBFT RPC client", "node", node, "error", err)
				<-ticker.C
				continue
			}

			nodeCtx, nodeCancel := context.WithCancel(ctx)

			if err := client.Start(); err != nil {
				m.logger.Error("failed to start CometBFT RPC client", "node", node, "error", err)
				nodeCancel()
				<-ticker.C
				continue
			}
			m.logger.Info("subscribed", "node", node+"/websocket")

			query := ctypes.QueryForEvent(ctypes.EventNewBlockEvents).String()
			eventCh, err := client.Subscribe(nodeCtx, ComponentName, query)
			if err != nil {
				m.logger.Error("failed to subscribe to NewBlock", "node", node, "error", err)
				client.Stop()
				nodeCancel()
				<-ticker.C
				continue
			}

			if m.runSubscription(ctx, node, nodeCtx, nodeCancel, client, eventCh, query) {
				<-ticker.C
				continue retryLoop
			}
			return
		}
	}
}

func (m *Monitor) runSubscription(
	ctx context.Context,
	node string,
	nodeCtx context.Context,
	nodeCancel context.CancelFunc,
	client *rpchttp.HTTP,
	eventCh <-chan coretypes.ResultEvent,
	query string,
) bool {
	var readyOnce sync.Once
	nodeLogger := m.logger.With("node", node)
	m.subsActive.Inc()

	defer m.subsActive.Dec()
	defer func() {
		unsubscribeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		if err := client.Unsubscribe(unsubscribeCtx, ComponentName, query); err != nil {
			nodeLogger.Error("unsubscribing the client", "err", err)
		}
		cancel()
		if err := client.Stop(); err != nil {
			nodeLogger.Error("stopping client", "err", err)
		}
	}()

	healthTicker := time.NewTicker(time.Second)
	defer healthTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			nodeCancel()
			return false
		case <-nodeCtx.Done():
			if ctx.Err() != nil {
				return false
			}
			return true
		case <-healthTicker.C:
			statusCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			_, err := client.Status(statusCtx)
			readyOnce.Do(func() {
				m.readyCh <- struct{}{}
			})
			cancel()
			if err != nil {
				nodeLogger.Error("subscription health check failed", "error", err)
				nodeCancel()
				return true
			}
		case msg, ok := <-eventCh:
			nodeLogger.Debug("new event", "event", msg)
			if !ok {
				nodeLogger.Error("event channel closed")
				nodeCancel()
				return true
			}
			blockEv, ok := msg.Data.(ctypes.EventDataNewBlockEvents)
			if !ok {
				nodeLogger.Error("unexpected event type", "type", fmt.Sprintf("%T", msg.Data))
				m.errCount.WithLabelValues("wrongEventType").Inc()
				continue
			}
			height := blockEv.Height

			if !m.shouldProcess(height) {
				continue
			}

			m.handleNewBlock(ctx, nodeLogger, blockEv, query)
		}
	}
}

func (m *Monitor) handleNewBlock(ctx context.Context, logger log.Logger, blockEv ctypes.EventDataNewBlockEvents, query string) {
	for _, ev := range blockEv.Events {
		switch {
		case ev.Type == "new_report":
			report, err := DecodeReportEvent(blockEv.Height, ev)
			if err != nil {
				logger.Error("failed to decode report event", "error", err)
				m.errCount.WithLabelValues("reportDecode").Inc()
				continue
			}
			if err := m.storeReport(ctx, *report); err == nil {
				logger.Debug("stored report", "query", query, "vals", report)
				continue
			}
			logger.Error("failed to store report", "error", err)
			m.errCount.WithLabelValues("reportInsert").Inc()
		case ev.Type == "new_dispute":
			if m.disputeEventHandler != nil {
				m.disputeEventHandler.HandleDisputeEvent(ev)
			}

		default:
			continue
		}
	}
}

const processedHeightsLimit = 1000

func (m *Monitor) shouldProcess(height int64) bool {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if _, seen := m.processedHeights[height]; seen {
		return false
	}

	m.processedHeights[height] = struct{}{}
	m.heightQueue = append(m.heightQueue, height)
	if len(m.heightQueue) > processedHeightsLimit {
		oldest := m.heightQueue[0]
		m.heightQueue = m.heightQueue[1:]
		delete(m.processedHeights, oldest)
	}
	return true
}

const (
	reporterCol        = "reporter"
	powerCol           = "power"
	queryTypeCol       = "query_type"
	queryIdCol         = "query_id"
	aggregateMethodCol = "aggregate_method"
	valueCol           = "value"
	timestampCol       = "timestamp"
	cyclelistCol       = "cyclelist"
	blockNumberCol     = "block_number"
	metaIdCol          = "meta_id"
)

func (m *Monitor) initDBTables(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// If TableName is "db.table", create DB first.
	if dot := strings.IndexByte(TableName, '.'); dot > 0 {
		dbName := TableName[:dot]
		if _, err := m.db.Exec(ctx, fmt.Sprintf(`CREATE DATABASE IF NOT EXISTS %s`, dbName)); err != nil {
			return fmt.Errorf("create database: %w", err)
		}
	}

	columnDefs := []string{
		reporterCol + " LowCardinality(String)",
		powerCol + " UInt64",
		queryTypeCol + " LowCardinality(String)",
		queryIdCol + " String",
		aggregateMethodCol + " LowCardinality(String)",
		valueCol + " String",
		timestampCol + " DateTime64(3, 'UTC')",
		cyclelistCol + " UInt8",
		blockNumberCol + " UInt64",
		metaIdCol + " UInt64",
	}

	// %[1]s = TableName
	// %[2]s = joined column definitions
	// %[3]s = timestampCol (partition key)
	// %[4]s = blockNumberCol (sort key #1)
	// %[5]s = queryIdCol (sort key #2)
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %[1]s (
		%[2]s
		)
		ENGINE = MergeTree
		PARTITION BY toYYYYMM(%[3]s)     -- partition by time ( %[3]s )
		ORDER BY (%[4]s, %[5]s)          -- sort key: height then query id ( %[4]s, %[5]s )
		SETTINGS index_granularity = 8192
		`,
		TableName,
		strings.Join(columnDefs, ",\n  "),
		timestampCol,
		blockNumberCol,
		queryIdCol,
	)

	_, err := m.db.Exec(ctx, query)

	m.logger.Info("created db table", "query", query)

	return err
}

func (m *Monitor) storeReport(ctx context.Context, r types.MicroReport) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()

	var cycle uint8
	if r.Cyclelist {
		cycle = 1
	}

	type columnVal struct {
		name string
		val  any
	}
	queryIDHex, err := EncodeQueryID(r.QueryId)
	if err != nil {
		return err
	}

	cols := []columnVal{
		{name: reporterCol, val: r.Reporter},
		{name: powerCol, val: r.Power},
		{name: queryTypeCol, val: r.QueryType},
		{name: queryIdCol, val: queryIDHex},
		{name: aggregateMethodCol, val: r.AggregateMethod},
		{name: valueCol, val: r.Value},
		{name: timestampCol, val: r.Timestamp},
		{name: cyclelistCol, val: cycle},
		{name: blockNumberCol, val: r.BlockNumber},
		{name: metaIdCol, val: r.MetaId},
	}

	var colNames []string
	var vals []any
	for _, cv := range cols {
		colNames = append(colNames, cv.name)
		vals = append(vals, cv.val)
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		TableName,
		strings.Join(colNames, ", "),
		strings.TrimRight(strings.Repeat("?, ", len(colNames)), ", "),
	)

	if _, err := m.db.Exec(ctx, query, vals...); err != nil {
		return err
	}

	m.reportCount.Inc()

	return nil
}

// ParseTimestamp parses timestamps stored on chain into UTC time.
func ParseTimestamp(val string) (time.Time, error) {
	if ts, err := time.Parse(time.RFC3339Nano, val); err == nil {
		return ts.UTC(), nil
	}
	ms, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.UnixMilli(ms).UTC(), nil
}

// ParseReporterPower parses the reporter power column.
func ParseReporterPower(val string) (uint64, error) {
	return parseUint64(val)
}

// ParseBlockNumber parses the block_number column.
func ParseBlockNumber(val string) (uint64, error) {
	return parseUint64(val)
}

// ParseMetaID parses the meta_id column.
func ParseMetaID(val string) (uint64, error) {
	return parseUint64(val)
}

// parseUint64 converts decimal strings to uint64.
func parseUint64(val string) (uint64, error) {
	return strconv.ParseUint(val, 10, 64)
}

// EncodeQueryID converts 32-byte query IDs to a hex string.
func EncodeQueryID(queryID []byte) (string, error) {
	if len(queryID) != 32 {
		return "", fmt.Errorf("query_id must be 32 bytes, got %d", len(queryID))
	}
	return hex.EncodeToString(queryID), nil
}

// DecodeQueryID converts a hex string query ID to bytes.
func DecodeQueryID(val string) ([]byte, error) {
	if val == "" {
		return nil, fmt.Errorf("query_id is empty")
	}

	return hex.DecodeString(val)
}

// DecodeReportEvent extracts a MicroReport from a CometBFT event payload.
func DecodeReportEvent(height int64, ev abci.Event) (*types.MicroReport, error) {
	var report types.MicroReport

	for _, attr := range ev.Attributes {
		attrVal := attr.Value
		switch attr.Key {
		case reporterCol:
			report.Reporter = attrVal
		case powerCol, "reporter_power":
			power, err := ParseReporterPower(attrVal)
			if err != nil {
				return nil, fmt.Errorf("parse reporter power: %w", err)
			}
			report.Power = power
		case queryTypeCol:
			report.QueryType = attrVal
		case queryIdCol:
			queryIDBytes, err := DecodeQueryID(attrVal)
			if err != nil {
				return nil, fmt.Errorf("decode query_id: %w", err)
			}
			report.QueryId = queryIDBytes
		case aggregateMethodCol:
			report.AggregateMethod = attrVal
		case valueCol:
			report.Value = attrVal
		case timestampCol:
			ts, err := ParseTimestamp(attrVal)
			if err != nil {
				return nil, fmt.Errorf("parse timestamp: %w", err)
			}
			report.Timestamp = ts
		case cyclelistCol:
			report.Cyclelist = attrVal == "true"
		case blockNumberCol:
			blockNumber, err := ParseBlockNumber(attrVal)
			if err != nil {
				return nil, fmt.Errorf("parse block number: %w", err)
			}
			report.BlockNumber = blockNumber

			if report.BlockNumber == 0 && height > 0 {
				report.BlockNumber = uint64(height)
			}
		case metaIdCol:
			metaId, err := ParseMetaID(attrVal)
			if err != nil {
				return nil, fmt.Errorf("parse meta_id: %w", err)
			}
			report.MetaId = metaId
		}
	}

	return &report, nil
}
