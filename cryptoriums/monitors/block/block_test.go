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
	"testing"
	"time"

	_ "github.com/chdb-io/chdb-go/chdb/driver"
	abci "github.com/cometbft/cometbft/abci/types"

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

	monitor, err := New(
		cryptolog.New(),
		monitorCfg,
		prometheus.NewRegistry(),
		SQLDB{DB: sqlDB},
	)
	require.NoError(t, err)

	// Initialize the monitor context so ProcessBlockEvent can use it
	monitor.mainCtx = ctx
	monitor.ctx = ctx
	monitor.cncl = cncl

	// Initialize DB tables
	if monitorCfg.InitDB {
		err = monitor.initDBTables()
		require.NoError(t, err)
	}

	// Process each block event directly, simulating multiple nodes sending the same blocks
	for _, block := range blocks {
		// Simulate 3 nodes sending the same block (testing deduplication)
		for i := 0; i < 3; i++ {
			err = monitor.ProcessBlockEvent(block)
			require.NoError(t, err)
		}
	}

	// Verify the report count metric
	require.Equal(t, len(expectedReports), int(testutil.ToFloat64(monitor.reportCount)),
		"expected reports count mismatch")

	// Query the db and test that it matches the reports from the fixtures.
	actualReports := fetchReportsFromDB(t, sqlDB)
	require.Equal(t, len(expectedReports), len(actualReports), "db row count mismatch")

	// Sort expected reports and convert to values for comparison
	expectedReports = sortReports(t, expectedReports)
	expectedReportsValues := make([]types.MicroReport, len(expectedReports))
	for i, r := range expectedReports {
		expectedReportsValues[i] = *r
	}

	// Sort actual reports the same way
	actualReports = sortReportsValues(t, actualReports)

	require.Equal(t, expectedReportsValues, actualReports, "db reports differ from expected")
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

func fetchReportsFromDB(t *testing.T, db *sql.DB) []types.MicroReport {
	t.Helper()

	rows, err := db.QueryContext(context.Background(), "SELECT reporter, power, query_type, query_id, aggregate_method, value, timestamp, cyclelist, block_number, meta_id FROM "+TableName+" ORDER BY block_number, reporter, query_id")
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

func sortReports(t *testing.T, reports []*types.MicroReport) []*types.MicroReport {
	t.Helper()

	sort.Slice(reports, func(i, j int) bool {
		if reports[i].BlockNumber != reports[j].BlockNumber {
			return reports[i].BlockNumber < reports[j].BlockNumber
		}
		if reports[i].Reporter != reports[j].Reporter {
			return reports[i].Reporter < reports[j].Reporter
		}
		// Sort by QueryId as tiebreaker
		return string(reports[i].QueryId) < string(reports[j].QueryId)
	})

	return reports
}

func sortReportsValues(t *testing.T, reports []types.MicroReport) []types.MicroReport {
	t.Helper()

	sort.Slice(reports, func(i, j int) bool {
		if reports[i].BlockNumber != reports[j].BlockNumber {
			return reports[i].BlockNumber < reports[j].BlockNumber
		}
		if reports[i].Reporter != reports[j].Reporter {
			return reports[i].Reporter < reports[j].Reporter
		}
		// Sort by QueryId as tiebreaker
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
