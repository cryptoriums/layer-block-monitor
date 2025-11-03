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

// For all tests use only public module functions.
// For matching exp vs act, use the db or the prometheus metrics.

// The monitor continues working when reload the config.
func TestReloadCfg(t *testing.T) {

}

// When one node is failing it should continue receiving events from the other nodes.
func TestNodeFailing(t *testing.T) {

}

// When multiple nodes send block the monitor processes them in order.
func TestMissingBlock(t *testing.T) {

}

func TestDeduplication(t *testing.T) {
	// Test deduplication with 3 separate nodes (independent blockchains)
	// Each node emits the same 4 blocks at heights 5, 6, 7, 8
	// Deduplication ensures each height is processed only once (first node to emit wins)
	//
	// Fixtures contain 4 blocks:
	// - height 5: 10 "new_report" events (from fixture block 0, height 11042385)
	// - height 6: no events (from fixture block 1, height 11042386)
	// - height 7: 10 "new_report" events (from fixture block 2, height 11042387)
	// - height 8: no events (from fixture block 3, height 11042388)
	//
	// Expected: 20 reports total (10 from height 5 + 10 from height 7)
	// Even though 3 nodes emit events, deduplication processes each height only once

	ctx, cncl := context.WithCancel(context.Background())
	t.Cleanup(cncl)

	sqlDB, err := sql.Open("chdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })

	blocks := loadBlockFixtures(t)
	require.NotEmpty(t, blocks)
	require.Len(t, blocks, 4, "fixtures should contain 4 blocks")

	// Map original fixture heights to emit heights
	// Block 0 (11042385) → height 5
	// Block 1 (11042386) → height 6
	// Block 2 (11042387) → height 7
	// Block 3 (11042388) → height 8
	heightMapping := map[int64]int64{
		11042385: 5,
		11042386: 6,
		11042387: 7,
		11042388: 8,
	}
	expectedReports := extractExpectedReports(t, blocks, heightMapping)
	require.NotEmpty(t, expectedReports)
	require.Len(t, expectedReports, 20, "fixtures should contain 20 total reports")

	// Create monitor configuration
	monitorCfg := Config{
		InitDB: true,
	}

	// Create 3 separate nodes for redundancy testing
	// Each node is an independent CometBFT instance with its own test app
	// Following the client's requirement: 3 different nodes with different RPC addresses
	for i := 0; i < 3; i++ {
		app := newTestApp(blocks)
		n := rpctest.StartTendermint(app, rpctest.SuppressStdout, rpctest.RecreateConfig)
		defer rpctest.StopTendermint(n)
		require.True(t, n.IsRunning())
<<<<<<< HEAD
		nodes = append(nodes, n)
		monitorCfg.Nodes = append(monitorCfg.Nodes, n.Config().RPC.ListenAddress)
=======

		// Use n.Config().RPC.ListenAddress (not rpctest.GetConfig().RPC.ListenAddress)
		// This gets the correct RPC address for this specific node
		monitorCfg.Nodes = append(monitorCfg.Nodes, n.Config().RPC.ListenAddress)
		t.Logf("Started node %d with RPC address: %s", i, n.Config().RPC.ListenAddress)
>>>>>>> origin
	}

	monitor, err := New(
		cryptolog.New(),
		monitorCfg,
		prometheus.NewRegistry(),
		SQLDB{DB: sqlDB},
	)
	require.NoError(t, err)
	go monitor.Start(ctx)

	// Wait for monitor to process all events from all 3 nodes
	// Use require.Eventually instead of time.Sleep
	// Expected: 20 reports total (10 from height 5 + 10 from height 7)
	// With 3 nodes, deduplication should ensure each block is processed only once
	expectedReportCount := 20

	require.Eventually(t, func() bool {
		actualReports := fetchReportsFromDB(t, sqlDB)
		return len(actualReports) == expectedReportCount
	}, 10*time.Second, 500*time.Millisecond, "should receive exactly %d reports", expectedReportCount)

	// Verify final report count
	actualReports := fetchReportsFromDB(t, sqlDB)
	t.Logf("Received %d total reports (expected %d)", len(actualReports), expectedReportCount)
	require.Equal(t, expectedReportCount, len(actualReports), "should have exactly 20 reports (10 from each of 2 blocks with events)")

	// Verify reports are from the correct heights (5 and 7)
	// Note: Fixture data has events at heights 11042385 and 11042387 (blocks 0 and 2)
	heightCounts := make(map[uint64]int)
	for _, report := range actualReports {
		heightCounts[report.BlockNumber]++
	}

	require.Equal(t, 10, heightCounts[5], "should have 10 reports from height 5")
	require.Equal(t, 10, heightCounts[7], "should have 10 reports from height 7")
	require.Len(t, heightCounts, 2, "should have reports from exactly 2 heights")

	// Verify reports match expected fixture data
	actualReports = sortReports(t, actualReports)
	expectedReports = sortReports(t, expectedReports)
	require.Equal(t, expectedReports, actualReports, "db reports should match expected fixture reports")

	t.Logf("Successfully verified deduplication with 3 separate nodes")
}

func loadBlockFixtures(t *testing.T) []ctypes.EventDataNewBlockEvents {
	t.Helper()

	path := fixturePath(t, "test_blocks.json")
	data, err := os.ReadFile(path)
	require.NoError(t, err)

	var fixtures []blockFixture
	require.NoError(t, json.Unmarshal(data, &fixtures))

	out := make([]ctypes.EventDataNewBlockEvents, 0, len(fixtures))
	for _, fixture := range fixtures {
		height, err := strconv.ParseInt(fixture.Height, 10, 64)
		require.NoError(t, err)

		var events []abci.Event
		for _, txRes := range fixture.TxsResults {
			if len(txRes.Events) > 0 {
				events = append(events, txRes.Events...)
			}
		}

		// Almost every other block don't contain TxsResults. Should we skip those or use the FinalizeBlockEvents?
		// if len(fixture.FinalizeBlockEvents) > 0 {
		// 	events = append(events, fixture.FinalizeBlockEvents...)
		// }

		out = append(out, ctypes.EventDataNewBlockEvents{
			Height: height,
			Events: events,
			NumTxs: int64(len(fixture.TxsResults)),
		})
	}

	return out
}

func extractExpectedReports(t *testing.T, blocks []ctypes.EventDataNewBlockEvents, heightMapping map[int64]int64) []*types.MicroReport {
	t.Helper()

	var reports []*types.MicroReport
	for _, block := range blocks {
		for _, ev := range block.Events {
			if ev.Type != "new_report" {
				continue
			}

			// Use the mapped height instead of the original fixture height
			emitHeight := heightMapping[block.Height]
			report, err := DecodeReportEvent(emitHeight, ev)
			require.NoError(t, err)

			reports = append(reports, report)
		}
	}

	return reports
}

func fetchReportsFromDB(t *testing.T, db *sql.DB) []*types.MicroReport {
	t.Helper()

	rows, err := db.QueryContext(context.Background(), "SELECT reporter, power, query_type, query_id, aggregate_method, value, timestamp, cyclelist, block_number, meta_id FROM "+TableName+" ORDER BY block_number, reporter")
	require.NoError(t, err)
	defer rows.Close()

	var reports []*types.MicroReport
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

		reports = append(reports, &types.MicroReport{
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

func sortReports(t *testing.T, reports []*types.MicroReport) []*types.MicroReport {
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

type blockFixture struct {
	Height              string       `json:"height"`
	TxsResults          []txResult   `json:"txs_results"`
	FinalizeBlockEvents []abci.Event `json:"finalize_block_events"`
}

type txResult struct {
	Events []abci.Event `json:"events"`
}

// testApp is a custom ABCI application that emits events from test fixtures
// at their original heights
type testApp struct {
	kvstore.Application
	blocks []ctypes.EventDataNewBlockEvents
	mu     sync.Mutex
}

func newTestApp(blocks []ctypes.EventDataNewBlockEvents) *testApp {
	return &testApp{
		Application: *kvstore.NewInMemoryApplication(),
		blocks:      blocks,
	}
}

func (app *testApp) FinalizeBlock(ctx context.Context, req *abci.RequestFinalizeBlock) (*abci.ResponseFinalizeBlock, error) {
	app.mu.Lock()
	defer app.mu.Unlock()

	// Call parent implementation
	resp, err := app.Application.FinalizeBlock(ctx, req)
	if err != nil {
		return resp, err
	}

	// Emit events at low heights (5, 6, 7, 8) instead of original fixture heights
	// Fixtures contain 4 blocks:
	// - block 0 (originally height 11042385): 10 "new_report" events → emit at height 5
	// - block 1 (originally height 11042386): no txs → emit at height 6
	// - block 2 (originally height 11042387): no txs → emit at height 7
	// - block 3 (originally height 11042388): 10 "new_report" events → emit at height 8
	emitHeights := []int64{5, 6, 7, 8}
	for i, block := range app.blocks {
		if req.Height == emitHeights[i] {
			resp.Events = append(resp.Events, block.Events...)
			if len(block.Events) > 0 {
				println(fmt.Sprintf("TEST APP: Emitting %d events at height %d", len(block.Events), req.Height))
			}
			break
		}
	}

	return resp, nil
}
