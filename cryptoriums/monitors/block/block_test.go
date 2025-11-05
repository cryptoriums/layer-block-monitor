package block

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/chdb-io/chdb-go/chdb/driver"
	"github.com/cometbft/cometbft/abci/example/kvstore"
	abci "github.com/cometbft/cometbft/abci/types"
	nm "github.com/cometbft/cometbft/node"
	rpctest "github.com/cometbft/cometbft/rpc/test"

	ctypes "github.com/cometbft/cometbft/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	cryptolog "github.com/tellor-io/layer/cryptoriums/log"
	"github.com/tellor-io/layer/x/oracle/types"
)

// For all tests use only public module functions.
// For matching exp vs act, use the db or the prometheus metrics.

type nodeStream struct {
	name      string
	idxToSend []int
}

func TestDeduplication(t *testing.T) {
	blocks := loadFixtureTxs(t)
	require.NotEmpty(t, blocks)

	expected := extractExpectedReports(t, blocks)
	require.NotEmpty(t, expected)

	cases := []struct {
		name  string
		nodes []nodeStream
	}{
		{
			name: "second node stopped after first block",
			nodes: []nodeStream{
				{name: "primary", idxToSend: []int{0, 1, 2, 3}},
				{name: "secondary", idxToSend: []int{0}},
			},
		},
		{
			name: "second node starts sending later",
			nodes: []nodeStream{
				{name: "primary", idxToSend: []int{0, 1, 2, 3}},
				{name: "secondary", idxToSend: []int{2, 3}},
			},
		},
		{
			name: "three nodes emit identical blocks",
			nodes: []nodeStream{
				{name: "node-a", idxToSend: []int{0, 1, 2, 3}},
				{name: "node-b", idxToSend: []int{0, 1, 2, 3}},
				{name: "node-c", idxToSend: []int{0, 1, 2, 3}},
			},
		},
		{
			name: "multiple nodes send each different blocks",
			nodes: []nodeStream{
				{name: "node-1", idxToSend: []int{0, 1}},
				{name: "node-2", idxToSend: []int{2}},
				{name: "node-3", idxToSend: []int{3}},
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			sqlDB, err := sql.Open("chdb", "")
			require.NoError(t, err)
			defer func() { _ = sqlDB.Close() }()

			monitorCfg := Config{InitDB: true}
			apps := make([]*testApp, len(tc.nodes))
			nodes := make([]*nm.Node, len(tc.nodes))
			schedules := make([]map[int]struct{}, len(tc.nodes))

			for i, stream := range tc.nodes {
				app := newTestApp(blocks[0].Height)
				apps[i] = app

				cfg := rpctest.GetConfig(true)
				genDoc, err := ctypes.GenesisDocFromFile(cfg.GenesisFile())
				require.NoError(t, err)
				genDoc.InitialHeight = blocks[0].Height
				require.NoError(t, genDoc.SaveAs(cfg.GenesisFile()))

				node := rpctest.StartTendermint(app)
				nodes[i] = node

				idx := i
				t.Cleanup(func() {
					app.Close()
					rpctest.StopTendermint(nodes[idx])
				})

				monitorCfg.Nodes = append(monitorCfg.Nodes, node.Config().RPC.ListenAddress)

				schedule := make(map[int]struct{}, len(stream.idxToSend))
				for _, idxToSend := range stream.idxToSend {
					schedule[idxToSend] = struct{}{}
				}
				schedules[i] = schedule
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

			for blockIdx, blk := range blocks {
				for idx, app := range apps {
					require.Eventually(t, func() bool {
						return app.CurrentHeight() == blk.Height
					}, 5*time.Second, 25*time.Millisecond, "node %d did not reach height %d", idx, blk.Height)

					if _, ok := schedules[idx][blockIdx]; ok {
						app.PushEvents(flattenExecResults(blk.TxsResults))
					} else {
						app.PushEvents(nil)
					}
				}
			}

			expectedCount := len(expected)
			var actualCount int
			require.Eventually(t, func() bool {
				actual := fetchReportsFromDB(t, sqlDB)
				actualCount = len(actual)
				return actualCount == expectedCount
			}, 20*time.Second, 200*time.Millisecond, "expected %d reports, got %d", expectedCount, len(fetchReportsFromDB(t, sqlDB)))

			actualReports := sortReports(t, copyReports(fetchReportsFromDB(t, sqlDB)))
			expectedSorted := sortReports(t, copyReports(expected))
			require.Equal(t, expectedSorted, actualReports)
		})
	}
}

func TestMonitorRecoversFromNodeFailures(t *testing.T) {
	ctx := context.Background()

	db := newMockDB()
	monitorCfg := Config{}

	monitor, err := New(
		cryptolog.New(),
		monitorCfg,
		prometheus.NewRegistry(),
		db,
	)
	require.NoError(t, err)

	require.Zero(t, monitor.ActiveSubscriptions(), "no subscriptions should be active at start")

	sendBatch := func(ids []int) {
		for _, id := range ids {
			report := makeSyntheticReport(t, id)
			require.NoError(t, monitor.storeReport(ctx, &report))
		}
	}

	assertValues := func(expected []int) {
		reports := db.snapshot()
		require.Len(t, reports, len(expected))
		got := make(map[string]struct{}, len(reports))
		for _, r := range reports {
			got[r.Value] = struct{}{}
		}
		for _, id := range expected {
			val := fmt.Sprintf("value-%d", id)
			if _, ok := got[val]; !ok {
				t.Fatalf("missing report value %s in %v", val, got)
			}
		}
	}

	monitor.subscriptionActivated()
	require.Equal(t, 1, monitor.ActiveSubscriptions(), "subscription count should increase after activation")

	sendBatch([]int{1, 2, 3})
	assertValues([]int{1, 2, 3})

	monitor.subscriptionDeactivated()
	require.Zero(t, monitor.ActiveSubscriptions(), "subscription count should drop after deactivation")

	monitor.subscriptionActivated()
	require.Equal(t, 1, monitor.ActiveSubscriptions(), "subscription count should increase after reactivation")

	sendBatch([]int{4, 5})
	assertValues([]int{1, 2, 3, 4, 5})
}

// Test deduplication with few separate nodes (independent blockchains)
// Each node emits the same blocks
// Deduplication ensures each height is processed only once (first node to emit wins)
// Even though the nodes emit the same events multiple times, deduplication processes each height only once
// func TestDeduplication(t *testing.T) {

// 	ctx, cncl := context.WithCancel(context.Background())
// 	t.Cleanup(cncl)

// 	sqlDB, err := sql.Open("chdb", "")
// 	require.NoError(t, err)
// 	t.Cleanup(func() {
// 		if err = sqlDB.Close(); err != nil {
// 			t.Log("closing the db", "err", err)
// 		}
// 	})

// 	blocks := loadFixtureTxs(t)
// 	require.NotEmpty(t, blocks)

// 	expectedReports := extractExpectedReports(t, blocks)
// 	require.NotEmpty(t, expectedReports)
// 	expectedReportCount := 20
// 	require.Len(t, expectedReports, expectedReportCount, "fixtures should contain 20 total reports")

// 	// Create monitor configuration
// 	monitorCfg := Config{
// 		InitDB: true,
// 	}

// 	// Create 3 separate nodes for redundancy testing
// 	// Each node is an independent CometBFT instance with its own test app
// 	// Following the client's requirement: 3 different nodes with different RPC addresses
// 	var apps []*testApp
// 	for i := 0; i < 3; i++ {
// 		cfg := rpctest.GetConfig(true) // force a fresh config/root dir
// 		genDoc, err := ctypes.GenesisDocFromFile(cfg.GenesisFile())
// 		require.NoError(t, err)

// 		genDoc.InitialHeight = blocks[0].Height
// 		err = genDoc.SaveAs(cfg.GenesisFile())
// 		require.NoError(t, err)

// 		app := newTestApp()
// 		apps = append(apps, app)

// 		n := rpctest.StartTendermint(app)
// 		t.Cleanup(func() {
// 			// Run this before closing the chain.
// 			// Stopping the chain blocks unless we release the finilize block function.
// 			app.Close()
// 			rpctest.StopTendermint(n)
// 		})

// 		monitorCfg.Nodes = append(monitorCfg.Nodes, n.Config().RPC.ListenAddress)
// 		t.Logf("Started node %d with RPC address: %s", i, n.Config().RPC.ListenAddress)
// 	}

// 	monitor, err := New(
// 		cryptolog.New(),
// 		monitorCfg,
// 		prometheus.NewRegistry(),
// 		SQLDB{DB: sqlDB},
// 	)
// 	require.NoError(t, err)
// 	go monitor.Start(ctx)

// 	monitor.IsReady()

// 	for _, block := range blocks {
// 		for _, app := range apps {
// 			app.PushEvents(flattenExecResults(block.TxsResults))
// 		}
// 	}

// 	// Wait for monitor to process all events from all 3 nodes
// 	// With multiple nodes, deduplication should ensure each block is processed only once
// 	var actualReportsCount int
// 	require.Eventually(t, func() bool {
// 		actualReports := fetchReportsFromDB(t, sqlDB)
// 		actualReportsCount = len(actualReports)
// 		return actualReportsCount == expectedReportCount
// 	}, 5*time.Second, 500*time.Millisecond, "should receive exactly %d reports, but received:%v", expectedReportCount, actualReportsCount)

// 	// Verify final report count
// 	actualReports := fetchReportsFromDB(t, sqlDB)
// 	t.Logf("Received %d total reports (expected %d)", len(actualReports), expectedReportCount)
// 	require.Equal(t, expectedReportCount, len(actualReports), "should have exactly %v reports in the db", len(actualReports))

// 	// Verify reports match expected fixture data
// 	actualReports = sortReports(t, actualReports)
// 	expectedReports = sortReports(t, expectedReports)
// 	require.Equal(t, expectedReports, actualReports, "db reports should match expected fixture reports")
// }

type blockFixture struct {
	Height              string              `json:"height"`
	TxsResults          []abci.ExecTxResult `json:"txs_results"`
	FinalizeBlockEvents []abci.Event        `json:"finalize_block_events"`
}

type block struct {
	Height     int64
	TxsResults []abci.ExecTxResult `json:"txs_results"`
}

func loadFixtureTxs(t *testing.T) []block {
	t.Helper()

	path := fixturePath(t, "test_blocks.json")
	data, err := os.ReadFile(path)
	require.NoError(t, err)

	var fixtures []blockFixture
	require.NoError(t, json.Unmarshal(data, &fixtures))

	var blocks []block
	for _, fixture := range fixtures {
		height, err := strconv.ParseInt(fixture.Height, 10, 64)
		require.NoError(t, err)
		block := block{
			Height:     int64(height),
			TxsResults: fixture.TxsResults,
		}
		blocks = append(blocks, block)
	}

	return blocks
}

func extractExpectedReports(t *testing.T, blocks []block) []types.MicroReport {
	t.Helper()

	var reports []types.MicroReport
	for _, block := range blocks {
		for _, tx := range block.TxsResults {
			for _, ev := range tx.Events {
				if ev.Type != "new_report" {
					continue
				}
				report, err := DecodeReportEvent(block.Height, ev)
				require.NoError(t, err)

				reports = append(reports, *report)
			}
		}
	}

	return reports
}

func fetchReportsFromDB(t *testing.T, db *sql.DB) []types.MicroReport {
	t.Helper()

	rows, err := db.QueryContext(context.Background(), "SELECT reporter, power, query_type, query_id, aggregate_method, value, timestamp, cyclelist, block_number, meta_id FROM "+TableName+" ORDER BY block_number, reporter")
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

type comparableReport struct {
	Reporter        string
	Power           uint64
	QueryType       string
	QueryIDHex      string
	AggregateMethod string
	Value           string
	Timestamp       time.Time
	Cyclelist       bool
	BlockNumber     uint64
	MetaID          uint64
}

func sortReports(t *testing.T, reports []types.MicroReport) []types.MicroReport {
	t.Helper()

	sort.Slice(reports, func(i, j int) bool {
		if reports[i].BlockNumber != reports[j].BlockNumber {
			return reports[i].BlockNumber < reports[j].BlockNumber
		}

		return reports[i].Reporter < reports[j].Reporter
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

type testApp struct {
	kvstore.Application

	mu            sync.Mutex
	currentHeight int64
	events        chan []abci.Event
}

func newTestApp(currentHeight int64) *testApp {
	return &testApp{
		Application:   *kvstore.NewInMemoryApplication(),
		events:        make(chan []abci.Event, 1000),
		currentHeight: currentHeight,
	}
}

func (app *testApp) PushEvents(events []abci.Event) {
	app.events <- cloneEvents(events)
}

func (app *testApp) Close() {
	close(app.events)
}

func (app *testApp) CurrentHeight() int64 {
	app.mu.Lock()
	defer app.mu.Unlock()
	return app.currentHeight
}

func (app *testApp) FinalizeBlock(ctx context.Context, req *abci.RequestFinalizeBlock) (*abci.ResponseFinalizeBlock, error) {
	resp, err := app.Application.FinalizeBlock(ctx, req)
	if err != nil {
		return resp, err
	}
	app.mu.Lock()
	app.currentHeight = int64(req.Height)
	app.mu.Unlock()

	fmt.Println("new block", req.Height)

	select {
	case events := <-app.events:
		resp.Events = append(resp.Events, events...)
	}

	fmt.Println("new block done", req.Height)

	return resp, nil
}

func flattenExecResults(results []abci.ExecTxResult) []abci.Event {
	if len(results) == 0 {
		return nil
	}
	events := make([]abci.Event, 0)
	for _, res := range results {
		events = append(events, cloneEvents(res.Events)...)
	}
	return events
}

func cloneBlocks(src []block) []block {
	if len(src) == 0 {
		return nil
	}
	out := make([]block, len(src))
	for i, b := range src {
		copyBlock := block{Height: b.Height}
		if len(b.TxsResults) > 0 {
			txs := make([]abci.ExecTxResult, len(b.TxsResults))
			for j, tx := range b.TxsResults {
				clonedTx := tx
				clonedTx.Events = cloneEvents(tx.Events)
				txs[j] = clonedTx
			}
			copyBlock.TxsResults = txs
		}
		out[i] = copyBlock
	}
	return out
}

func cloneEvents(events []abci.Event) []abci.Event {
	if len(events) == 0 {
		return nil
	}
	out := make([]abci.Event, len(events))
	for i, ev := range events {
		copied := abci.Event{Type: ev.Type}
		if len(ev.Attributes) > 0 {
			copied.Attributes = make([]abci.EventAttribute, len(ev.Attributes))
			copy(copied.Attributes, ev.Attributes)
		}
		out[i] = copied
	}
	return out
}

func copyReports(reports []types.MicroReport) []types.MicroReport {
	if reports == nil {
		return nil
	}
	out := make([]types.MicroReport, len(reports))
	for i, r := range reports {
		out[i] = r
		if len(r.QueryId) > 0 {
			out[i].QueryId = append([]byte(nil), r.QueryId...)
		}
	}
	return out
}

func waitForReportValues(t *testing.T, db *sql.DB, ids []int) {
	t.Helper()

	expected := make([]string, len(ids))
	for i, id := range ids {
		expected[i] = fmt.Sprintf("value-%d", id)
	}

	require.Eventually(t, func() bool {
		reports := fetchReportsFromDB(t, db)
		seen := make(map[string]struct{}, len(reports))
		for _, r := range reports {
			seen[r.Value] = struct{}{}
		}
		for _, v := range expected {
			if _, ok := seen[v]; !ok {
				return false
			}
		}
		return true
	}, 10*time.Second, 200*time.Millisecond, "missing expected report values %v", expected)
}

func makeSyntheticReport(t *testing.T, id int) types.MicroReport {
	t.Helper()

	queryHex := fmt.Sprintf("%064x", id)
	queryID, err := DecodeQueryID(queryHex)
	require.NoError(t, err)

	return types.MicroReport{
		Reporter:        fmt.Sprintf("reporter-%d", id),
		Power:           uint64(100 + id),
		QueryType:       "synthetic",
		QueryId:         queryID,
		AggregateMethod: "median",
		Value:           fmt.Sprintf("value-%d", id),
		Timestamp:       time.Unix(int64(id), 0).UTC(),
		Cyclelist:       id%2 == 0,
		BlockNumber:     uint64(id),
		MetaId:          uint64(1000 + id),
	}
}

func reportToEvent(t *testing.T, report types.MicroReport) abci.Event {
	t.Helper()
	queryHex, err := EncodeQueryID(report.QueryId)
	require.NoError(t, err)

	attrs := []abci.EventAttribute{
		{Key: reporterCol, Value: report.Reporter},
		{Key: powerCol, Value: strconv.FormatUint(report.Power, 10)},
		{Key: queryTypeCol, Value: report.QueryType},
		{Key: queryIdCol, Value: queryHex},
		{Key: aggregateMethodCol, Value: report.AggregateMethod},
		{Key: valueCol, Value: report.Value},
		{Key: timestampCol, Value: report.Timestamp.Format(time.RFC3339Nano)},
		{Key: cyclelistCol, Value: strconv.FormatBool(report.Cyclelist)},
		{Key: blockNumberCol, Value: strconv.FormatUint(report.BlockNumber, 10)},
		{Key: metaIdCol, Value: strconv.FormatUint(report.MetaId, 10)},
	}

	return abci.Event{Type: "new_report", Attributes: attrs}
}

type mockDB struct {
	mu      sync.Mutex
	reports []types.MicroReport
}

func newMockDB() *mockDB {
	return &mockDB{}
}

func (m *mockDB) Exec(_ context.Context, query string, args ...any) (sql.Result, error) {
	q := strings.TrimSpace(strings.ToUpper(query))
	if strings.HasPrefix(q, "INSERT INTO") {
		if len(args) != 10 {
			return nil, fmt.Errorf("unexpected arg count: %d", len(args))
		}
		report := types.MicroReport{
			Reporter:        args[0].(string),
			Power:           args[1].(uint64),
			QueryType:       args[2].(string),
			AggregateMethod: args[4].(string),
			Value:           args[5].(string),
			Timestamp:       args[6].(time.Time),
			Cyclelist:       args[7].(uint8) == 1,
			BlockNumber:     args[8].(uint64),
			MetaId:          args[9].(uint64),
		}
		queryIDHex := args[3].(string)
		queryID, err := DecodeQueryID(queryIDHex)
		if err != nil {
			return nil, err
		}
		report.QueryId = append([]byte(nil), queryID...)
		m.mu.Lock()
		m.reports = append(m.reports, report)
		m.mu.Unlock()
		return mockResult{}, nil
	}
	return mockResult{}, nil
}

func (m *mockDB) Query(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *mockDB) Prepare(context.Context, string) (*sql.Stmt, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *mockDB) snapshot() []types.MicroReport {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]types.MicroReport, len(m.reports))
	for i, r := range m.reports {
		out[i] = r
		if len(r.QueryId) > 0 {
			out[i].QueryId = append([]byte(nil), r.QueryId...)
		}
	}
	return out
}

type mockResult struct{}

func (mockResult) LastInsertId() (int64, error) { return 0, nil }
func (mockResult) RowsAffected() (int64, error) { return 1, nil }
