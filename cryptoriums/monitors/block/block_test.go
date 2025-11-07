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
	nm "github.com/cometbft/cometbft/node"
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
				app := newTestApp(t, blocks[0].Height)
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
				nil,
			)
			require.NoError(t, err)

			go monitor.Start(ctx)
			monitor.IsReady()

			for blockIdx, blk := range blocks {
				for idx, app := range apps {
					require.Eventually(t, func() bool {
						return app.CurrentHeight() >= blk.Height
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

func TestNodeFailure(t *testing.T) {
	cases := []struct {
		name      string
		nodeCount int
		plan      func(active *activeNodes, blockIdx int)
	}{
		{
			name:      "single node fails and recovers",
			nodeCount: 1,
			plan: func(active *activeNodes, blockIdx int) {
				if blockIdx == 1 {
					active.Stop(0)
					active.Start(0)
				}
			},
		},
		{
			name:      "two nodes fail together and recover",
			nodeCount: 2,
			plan: func(active *activeNodes, blockIdx int) {
				if blockIdx == 1 {
					active.Stop(0, 1)
					active.Start(0, 1)
				}
			},
		},
		{
			name:      "node2 fails, recovers, then node1 fails",
			nodeCount: 2,
			plan: func(active *activeNodes, blockIdx int) {
				switch blockIdx {
				case 0:
					active.Stop(1)
				case 2:
					active.StartIfStopped(1)
					active.Stop(0)
				}
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			blocks := makeSyntheticBlocks(t, 1, 2, 3, 4, 5)
			reports := blocksToReports(t, blocks)

			sqlDB, err := sql.Open("chdb", "")
			require.NoError(t, err)
			defer func() { _ = sqlDB.Close() }()

			monitorCfg := Config{InitDB: true}
			h := newNodeHarness(t, tc.nodeCount, blocks[0].Height)
			apps, nodes := h.apps, h.nodes

			for _, addr := range h.addresses {
				monitorCfg.Nodes = append(monitorCfg.Nodes, addr)
			}

			monitor, err := New(
				cryptolog.New(),
				monitorCfg,
				prometheus.NewRegistry(),
				SQLDB{DB: sqlDB},
				nil,
			)
			require.NoError(t, err)

			go monitor.Start(ctx)

			active := newActiveNodes(t, nodes, monitor)
			active.Wait(float64(tc.nodeCount))

			for idx, blk := range blocks {
				tc.plan(active, idx)
				for nodeIdx := range apps {
					if !active.IsRunning(nodeIdx) {
						continue
					}
					apps[nodeIdx].PushEvents(flattenExecResults(blk.TxsResults))
				}
			}

			var reportsAct []types.MicroReport
			require.Eventually(t, func() bool {
				reportsAct = fetchReportsFromDB(t, sqlDB)
				return len(reportsAct) == len(reports)
			}, 10*time.Second, 200*time.Millisecond, "missing expected report count mismatch exp:%v, act:%v", len(reports), len(reportsAct))

			require.Equal(t,
				sortReports(t, copyReports(reports)),
				sortReports(t, copyReports(reportsAct)),
			)
		})
	}
}

type nodeHarness struct {
	apps      []*testApp
	nodes     []*nm.Node
	addresses []string
}

func newNodeHarness(t *testing.T, count int, initialHeight int64) *nodeHarness {
	h := &nodeHarness{
		apps:      make([]*testApp, count),
		nodes:     make([]*nm.Node, count),
		addresses: make([]string, 0, count),
	}
	for i := 0; i < count; i++ {
		app := newTestApp(t, initialHeight)
		h.apps[i] = app

		cfg := rpctest.GetConfig(true)
		genDoc, err := ctypes.GenesisDocFromFile(cfg.GenesisFile())
		require.NoError(t, err)
		genDoc.InitialHeight = initialHeight
		require.NoError(t, genDoc.SaveAs(cfg.GenesisFile()))

		node := rpctest.StartTendermint(app)
		h.nodes[i] = node
		h.addresses = append(h.addresses, node.Config().RPC.ListenAddress)

		appRef := app
		nodeRef := node
		t.Cleanup(func() {
			appRef.Close()
			rpctest.StopTendermint(nodeRef)
		})
	}
	return h
}

type activeNodes struct {
	t       *testing.T
	nodes   []*nm.Node
	monitor *Monitor
	running map[int]bool
}

func newActiveNodes(t *testing.T, nodes []*nm.Node, monitor *Monitor) *activeNodes {
	a := &activeNodes{
		t:       t,
		nodes:   nodes,
		monitor: monitor,
		running: make(map[int]bool, len(nodes)),
	}
	for i := range nodes {
		a.running[i] = true
	}
	return a
}

func (a *activeNodes) IsRunning(idx int) bool {
	return a.running[idx]
}

func (a *activeNodes) Wait(expected float64) {
	require.Eventually(a.t, func() bool {
		return testutil.ToFloat64(a.monitor.subsActive) == expected
	}, 5*time.Second, 25*time.Millisecond, "expected %v active subscriptions", expected)
}

func (a *activeNodes) Stop(idxs ...int) {
	for _, idx := range idxs {
		if !a.running[idx] {
			continue
		}
		require.NoError(a.t, a.nodes[idx].Stop())
		a.running[idx] = false
	}
	a.Wait(a.count())
}

func (a *activeNodes) Start(idxs ...int) {
	for _, idx := range idxs {
		if a.running[idx] {
			continue
		}
		require.NoError(a.t, a.nodes[idx].Start())
		a.running[idx] = true
	}
	a.Wait(a.count())
}

func (a *activeNodes) StartIfStopped(idx int) {
	if a.running[idx] {
		return
	}
	a.Start(idx)
}

func (a *activeNodes) count() float64 {
	var count float64
	for _, ok := range a.running {
		if ok {
			count++
		}
	}
	return count
}

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
	t *testing.T

	mu            sync.Mutex
	currentHeight int64
	events        chan []abci.Event
}

func newTestApp(t *testing.T, currentHeight int64) *testApp {
	return &testApp{
		Application:   *kvstore.NewInMemoryApplication(),
		events:        make(chan []abci.Event, 1000),
		currentHeight: currentHeight,
		t:             t,
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

	app.t.Log("new block", req.Height)

	select {
	case events := <-app.events:
		resp.Events = append(resp.Events, events...)
	}

	app.t.Log("new block done", req.Height)

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

func makeSyntheticBlocks(t *testing.T, ids ...int) []block {
	t.Helper()

	blocks := make([]block, len(ids))
	for i, id := range ids {
		report := makeSyntheticReport(t, id)
		blocks[i] = block{
			Height: int64(report.BlockNumber),
			TxsResults: []abci.ExecTxResult{
				{Events: []abci.Event{reportToEvent(t, report)}},
			},
		}
	}
	return blocks
}

func blocksToReports(t *testing.T, blocks []block) []types.MicroReport {
	t.Helper()

	var reports []types.MicroReport
	for _, blk := range blocks {
		for _, tx := range blk.TxsResults {
			for _, ev := range tx.Events {
				if ev.Type != "new_report" {
					continue
				}
				report, err := DecodeReportEvent(blk.Height, ev)
				require.NoError(t, err)
				reports = append(reports, *report)
			}
		}
	}
	return reports
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
