package block

import (
	"context"
	"database/sql"
	"encoding/json"
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
	rpchttp "github.com/cometbft/cometbft/rpc/client/http"
	rpctest "github.com/cometbft/cometbft/rpc/test"

	ctypes "github.com/cometbft/cometbft/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
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
	ctx, cncl := context.WithCancel(context.Background())
	t.Cleanup(cncl)

	sqlDB, err := sql.Open("chdb", "")
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })

	blocks := loadBlockFixtures(t)
	require.NotEmpty(t, blocks)

	expectedReports := extractExpectedReports(t, blocks)
	require.NotEmpty(t, expectedReports)

	monitorCfg := Config{
		Nodes:  []string{},
		InitDB: true,
	}

	// Create test application that emits the events from fixtures
	testApp := newTestApp(blocks)
	n := rpctest.StartTendermint(testApp, rpctest.SuppressStdout, rpctest.RecreateConfig)
	defer rpctest.StopTendermint(n)
	require.True(t, n.IsRunning())
	monitorCfg.Nodes = append(monitorCfg.Nodes, rpctest.GetConfig().RPC.ListenAddress)

	// Create RPC client to send transactions BEFORE starting the monitor
	// This ensures we can control when blocks are created
	rpcAddr := rpctest.GetConfig().RPC.ListenAddress
	rpcClient, err := rpchttp.New(rpcAddr, "/websocket")
	require.NoError(t, err)
	err = rpcClient.Start()
	require.NoError(t, err)
	defer rpcClient.Stop()

	monitor, err := New(
		cryptolog.New(),
		monitorCfg,
		prometheus.NewRegistry(),
		SQLDB{DB: sqlDB},
	)
	require.NoError(t, err)
	go monitor.Start(ctx)

	// Wait for monitor to subscribe before creating any blocks
	time.Sleep(1 * time.Second)

	// Send a single transaction to trigger block creation
	// The testApp will emit all events from all fixture blocks in this one block
	_, err = rpcClient.BroadcastTxSync(ctx, []byte("dummy_tx"))
	require.NoError(t, err)

	// Wait for block to be created and processed
	time.Sleep(1 * time.Second)

	require.Eventually(t, func() bool { return int(testutil.ToFloat64(monitor.reportCount)) == len(expectedReports) },
		10*time.Second, 100*time.Millisecond,
		"expected reports count mismatch exp:%v, act:%v",
		len(expectedReports),
		int(testutil.ToFloat64(monitor.reportCount)),
	)

	// Query the db and test that it matches the reports from the fictures.
	actualReports := fetchReportsFromDB(t, sqlDB)
	require.Equal(t, len(expectedReports), len(actualReports), "db row count mismatch")

	// Since all events were emitted in block 2, update expected reports to have BlockNumber = 2
	for _, report := range expectedReports {
		report.BlockNumber = 2
	}

	expectedReports = sortReports(t, expectedReports)
	actualReports = sortReports(t, actualReports)

	require.Equal(t, expectedReports, actualReports, "db reports differ from expected")
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

func extractExpectedReports(t *testing.T, blocks []ctypes.EventDataNewBlockEvents) []*types.MicroReport {
	t.Helper()

	var reports []*types.MicroReport
	for _, block := range blocks {
		for _, ev := range block.Events {
			if ev.Type != "new_report" {
				continue
			}

			report, err := DecodeReportEvent(block.Height, ev)
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
type testApp struct {
	kvstore.Application
	blocks       []ctypes.EventDataNewBlockEvents
	currentBlock int
	mu           sync.Mutex
}

func newTestApp(blocks []ctypes.EventDataNewBlockEvents) *testApp {
	return &testApp{
		Application:  *kvstore.NewInMemoryApplication(),
		blocks:       blocks,
		currentBlock: 0,
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

	// Emit all events in the first transaction block (height 2)
	// This avoids timing issues with CometBFT creating empty blocks
	if req.Height == 2 {
		// Append events from all fixture blocks
		for _, block := range app.blocks {
			resp.Events = append(resp.Events, block.Events...)
		}
	}

	return resp, nil
}
