package block

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	_ "github.com/chdb-io/chdb-go/chdb/driver"
	"github.com/cometbft/cometbft/abci/example/kvstore"
	abci "github.com/cometbft/cometbft/abci/types"
	rpctest "github.com/cometbft/cometbft/rpc/test"

	ctypes "github.com/cometbft/cometbft/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	cryptolog "github.com/tellor-io/layer/cryptoriums/log"
	"github.com/tellor-io/layer/x/oracle/types"
)

type noopDB struct{}

func (noopDB) Exec(context.Context, string, ...any) (sql.Result, error) { return nil, nil }
func (noopDB) Query(context.Context, string, ...any) (*sql.Rows, error) { return nil, nil }
func (noopDB) Prepare(context.Context, string) (*sql.Stmt, error)       { return nil, nil }

var _ Db = noopDB{}

// For all tests use only public module functions.
// For matching exp vs act, use the db or the prometheus metrics.

// When multiple nodes send block the monitor processes them in order.
func TestMissingBlock(t *testing.T) {

}

// Test deduplication with few separate nodes (independent blockchains)
// Each node emits the same blocks
// Deduplication ensures each height is processed only once (first node to emit wins)
// Even though the nodes emit the same events multiple times, deduplication processes each height only once
func TestDeduplication(t *testing.T) {

	ctx, cncl := context.WithCancel(context.Background())
	t.Cleanup(cncl)

	sqlDB, err := sql.Open("chdb", "")
	require.NoError(t, err)
	t.Cleanup(func() {
		if err = sqlDB.Close(); err != nil {
			t.Log("closing the db", "err", err)
		}
	})

	txsMap, startHeight := loadFixtureTxs(t)
	require.NotEmpty(t, txsMap)

	expectedReports := extractExpectedReports(t, txsMap)
	require.NotEmpty(t, expectedReports)
	expectedReportCount := 20
	require.Len(t, expectedReports, expectedReportCount, "fixtures should contain 20 total reports")

	// Create monitor configuration
	monitorCfg := Config{
		InitDB: true,
	}

	// Create 3 separate nodes for redundancy testing
	// Each node is an independent CometBFT instance with its own test app
	// Following the client's requirement: 3 different nodes with different RPC addresses
	var apps []*testApp
	for i := 0; i < 3; i++ {
		app := newTestApp(txsMap)
		apps = append(apps, app)

		cfg := rpctest.GetConfig(true) // force a fresh config/root dir
		genDoc, err := ctypes.GenesisDocFromFile(cfg.GenesisFile())
		require.NoError(t, err)

		genDoc.InitialHeight = startHeight
		err = genDoc.SaveAs(cfg.GenesisFile())
		require.NoError(t, err)

		n := rpctest.StartTendermint(app)
		t.Cleanup(func() { rpctest.StopTendermint(n) })

		monitorCfg.Nodes = append(monitorCfg.Nodes, n.Config().RPC.ListenAddress)
		t.Logf("Started node %d with RPC address: %s", i, n.Config().RPC.ListenAddress)
	}

	monitor, err := New(
		cryptolog.New(),
		monitorCfg,
		prometheus.NewRegistry(),
		SQLDB{DB: sqlDB},
	)
	require.NoError(t, err)
	go monitor.Start(ctx)

	monitor.IsReady()

	for _, app := range apps {
		app.AllowEvents()
	}

	// Wait for monitor to process all events from all 3 nodes
	// Use require.Eventually instead of time.Sleep
	// With multiple nodes, deduplication should ensure each block is processed only once
	var actualReportsCount int
	require.Eventually(t, func() bool {
		actualReports := fetchReportsFromDB(t, sqlDB)
		actualReportsCount = len(actualReports)
		return actualReportsCount == expectedReportCount
	}, 5*time.Second, 500*time.Millisecond, "should receive exactly %d reports, but received:%v", expectedReportCount, actualReportsCount)

	// Verify final report count
	actualReports := fetchReportsFromDB(t, sqlDB)
	t.Logf("Received %d total reports (expected %d)", len(actualReports), expectedReportCount)
	require.Equal(t, expectedReportCount, len(actualReports), "should have exactly %v reports in the db", len(actualReports))

	// Verify reports match expected fixture data
	actualReports = sortReports(t, actualReports)
	expectedReports = sortReports(t, expectedReports)
	require.Equal(t, expectedReports, actualReports, "db reports should match expected fixture reports")
}

type blockFixture struct {
	Height              string              `json:"height"`
	TxsResults          []abci.ExecTxResult `json:"txs_results"`
	FinalizeBlockEvents []abci.Event        `json:"finalize_block_events"`
}

func loadFixtureTxs(t *testing.T) (map[int64][]abci.ExecTxResult, int64) {
	t.Helper()

	path := fixturePath(t, "test_blocks.json")
	data, err := os.ReadFile(path)
	require.NoError(t, err)

	minHeight := int64(math.MaxInt64)

	var fixtures []blockFixture
	require.NoError(t, json.Unmarshal(data, &fixtures))

	out := make(map[int64][]abci.ExecTxResult)
	for _, fixture := range fixtures {
		height, err := ParseBlockNumber(fixture.Height)
		require.NoError(t, err)
		if height < uint64(minHeight) {
			minHeight = int64(height)
		}
		out[int64(height)] = append(out[int64(height)], fixture.TxsResults...)
	}

	return out, minHeight
}

func extractExpectedReports(t *testing.T, txsMap map[int64][]abci.ExecTxResult) []types.MicroReport {
	t.Helper()

	var reports []types.MicroReport
	for height, txs := range txsMap {
		for _, tx := range txs {
			for _, ev := range tx.Events {

				if ev.Type != "new_report" {
					continue
				}
				report, err := DecodeReportEvent(uint64(height), ev)
				require.NoError(t, err)

				reports = append(reports, *report)
			}
		}
	}

	return reports
}

func fetchReportsFromDB(t *testing.T, db *sql.DB) []types.MicroReport {
	t.Helper()

	query := fmt.Sprintf(
		"SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s FROM %s ORDER BY %s, %s",
		reporterCol,
		powerCol,
		queryTypeCol,
		queryIdCol,
		aggregateMethodCol,
		valueCol,
		timestampCol,
		cyclelistCol,
		blockNumberCol,
		metaIdCol,
		TableName,
		blockNumberCol,
		reporterCol,
	)

	ctx, cncl := context.WithTimeout(context.Background(), time.Second)
	defer cncl()
	rows, err := db.QueryContext(ctx, query)
	require.NoError(t, err)
	defer rows.Close()

	var reports []types.MicroReport
	for rows.Next() {
		var (
			reporter        string
			power           uint64
			queryType       string
			queryIDStr      string
			aggregateMethod string
			value           string
			ts              time.Time
			cycle           uint8
			blockNumber     uint64
			metaID          uint64
		)
		require.NoError(t, rows.Scan(&reporter, &power, &queryType, &queryIDStr, &aggregateMethod, &value, &ts, &cycle, &blockNumber, &metaID))

		queryID, err := DecodeQueryID(queryIDStr)
		require.NoError(t, err)

		reports = append(reports, types.MicroReport{
			Reporter:        reporter,
			Power:           power,
			QueryType:       queryType,
			QueryId:         queryID,
			AggregateMethod: aggregateMethod,
			Value:           value,
			Timestamp:       ts.UTC(),
			Cyclelist:       cycle == 1,
			BlockNumber:     blockNumber,
			MetaId:          metaID,
		})
	}
	require.NoError(t, rows.Err())

	return reports
}

func sortReports(t *testing.T, reports []types.MicroReport) []types.MicroReport {
	t.Helper()

	sort.Slice(reports, func(i, j int) bool {
		if reports[i].BlockNumber != reports[j].BlockNumber {
			return reports[i].BlockNumber < reports[j].BlockNumber
		}

		if reports[i].Reporter != reports[j].Reporter {
			return reports[i].Reporter < reports[j].Reporter
		}

		if reports[i].MetaId != reports[j].MetaId {
			return reports[i].MetaId < reports[j].MetaId
		}

		// Compare QueryId bytes
		return string(reports[i].QueryId) < string(reports[j].QueryId)
	})

	return reports
}

func fixturePath(t *testing.T, name string) string {
	t.Helper()

	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok, "failed to determine caller path")

	dir := filepath.Dir(filename)
	return filepath.Join(dir, name)
}

// testApp is a custom ABCI application that emits events from test fixtures
// at their original heights
type testApp struct {
	kvstore.Application
	txs   map[int64][]abci.ExecTxResult
	mu    sync.Mutex
	ready chan struct{}
}

func newTestApp(txs map[int64][]abci.ExecTxResult) *testApp {
	return &testApp{
		Application: *kvstore.NewInMemoryApplication(),
		txs:         txs,
		ready:       make(chan struct{}),
	}
}

func (app *testApp) AllowEvents() {
	select {
	case <-app.ready:
	default:
		close(app.ready)
	}
}

func (app *testApp) FinalizeBlock(ctx context.Context, req *abci.RequestFinalizeBlock) (*abci.ResponseFinalizeBlock, error) {
	<-app.ready

	app.mu.Lock()
	defer app.mu.Unlock()

	// Call parent implementation
	resp, err := app.Application.FinalizeBlock(ctx, req)
	if err != nil {
		return resp, err
	}

	if len(app.txs[req.Height]) > 0 {
		for _, tx := range app.txs[req.Height] {
			resp.Events = append(resp.Events, tx.Events...)
		}
	}

	return resp, nil
}
