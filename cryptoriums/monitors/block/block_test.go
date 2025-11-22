package block

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/chdb-io/chdb-go/chdb/driver"
	rpctest "github.com/cometbft/cometbft/rpc/test"
	ctypes "github.com/cometbft/cometbft/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	blockdb "github.com/tellor-io/layer/cryptoriums/db"
	cryptolog "github.com/tellor-io/layer/cryptoriums/log"
)

// For all tests use only public module functions.
// For matching exp vs act, use the db or the prometheus metrics.

func TestBackfill(t *testing.T) {
	fixtures := loadFixtures(t)
	require.NotEmpty(t, fixtures)

	expected := extractExpectedReports(t, fixtures)
	require.NotEmpty(t, expected)

	t.Run("backfill enabled processes all blocks", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sqlDB, err := sql.Open("chdb", "")
		require.NoError(t, err)
		defer func() { _ = sqlDB.Close() }()

		// Clean up tables at START of test to ensure isolation
		_, _ = sqlDB.Exec("DROP TABLE IF EXISTS txs")
		_, _ = sqlDB.Exec("DROP TABLE IF EXISTS blocks")

		wrappedDB, err := blockdb.New(ctx, sqlDB)
		require.NoError(t, err)

		// Setup node with all fixture blocks
		app := newTestApp(t)
		app.SetBatch(fixtures)

		cfg := rpctest.GetConfig(true)
		genDoc, err := ctypes.GenesisDocFromFile(cfg.GenesisFile())
		require.NoError(t, err)
		genDoc.InitialHeight = fixtureHeight(fixtures[0])
		require.NoError(t, genDoc.SaveAs(cfg.GenesisFile()))

		node := rpctest.StartTendermint(app)
		t.Cleanup(func() {
			rpctest.StopTendermint(node)
		})

		// Monitor with Backfill enabled
		rpcCfg := Config{
			Nodes:        []string{node.Config().RPC.ListenAddress},
			Backfill:     true,
			PollInterval: 200 * time.Millisecond,
		}

		monitor, err := New(
			ctx,
			cryptolog.New(),
			rpcCfg,
			prometheus.NewRegistry(),
			wrappedDB,
		)
		require.NoError(t, err)

		runErr := make(chan error, 1)
		go func() {
			runErr <- monitor.Run(ctx)
		}()
		t.Cleanup(func() {
			select {
			case err := <-runErr:
				if err != nil && err != context.Canceled {
					t.Fatalf("monitor run failed: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatalf("monitor did not stop")
			}
		})

		// Verify all reports are stored
		expectedCount := len(expected)
		require.Eventually(t, func() bool {
			actual := fetchReportsFromDB(t, sqlDB)
			return len(actual) == expectedCount
		}, 5*time.Second, 200*time.Millisecond, "backfill should store all %d reports, got %d", expectedCount, len(fetchReportsFromDB(t, sqlDB)))

		actualReports := sortReports(t, copyReports(fetchReportsFromDB(t, sqlDB)))
		expectedSorted := sortReports(t, copyReports(expected))
		require.Equal(t, expectedSorted, actualReports)

		// Clean up tables after test
		_, _ = sqlDB.Exec("DROP TABLE IF EXISTS txs")
		_, _ = sqlDB.Exec("DROP TABLE IF EXISTS blocks")
	})

	t.Run("backfill disabled skips historical blocks", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sqlDB, err := sql.Open("chdb", "")
		require.NoError(t, err)
		defer func() { _ = sqlDB.Close() }()

		// Clean up tables at START of test to ensure isolation
		_, _ = sqlDB.Exec("DROP TABLE IF EXISTS txs")
		_, _ = sqlDB.Exec("DROP TABLE IF EXISTS blocks")

		wrappedDB, err := blockdb.New(ctx, sqlDB)
		require.NoError(t, err)

		// Setup node with all fixture blocks
		app := newTestApp(t)
		app.SetBatch(fixtures)

		cfg := rpctest.GetConfig(true)
		genDoc, err := ctypes.GenesisDocFromFile(cfg.GenesisFile())
		require.NoError(t, err)
		genDoc.InitialHeight = fixtureHeight(fixtures[0])
		require.NoError(t, genDoc.SaveAs(cfg.GenesisFile()))

		node := rpctest.StartTendermint(app)
		t.Cleanup(func() {
			rpctest.StopTendermint(node)
		})

		// Monitor with Backfill disabled
		rpcCfg := Config{
			Nodes:        []string{node.Config().RPC.ListenAddress},
			Backfill:     false, // Disabled
			PollInterval: 200 * time.Millisecond,
		}

		monitor, err := New(
			ctx,
			cryptolog.New(),
			rpcCfg,
			prometheus.NewRegistry(),
			wrappedDB,
		)
		require.NoError(t, err)

		runErr := make(chan error, 1)
		go func() {
			runErr <- monitor.Run(ctx)
		}()
		t.Cleanup(func() {
			select {
			case err := <-runErr:
				if err != nil && err != context.Canceled {
					t.Fatalf("monitor run failed: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatalf("monitor did not stop")
			}
		})

		// With backfill disabled, monitor should skip historical blocks
		// and only process new blocks (which won't come in this test)
		// Verify that no reports are processed even after waiting
		require.Never(t, func() bool {
			actual := fetchReportsFromDB(t, sqlDB)
			return len(actual) > 0
		}, 2*time.Second, 200*time.Millisecond, "backfill disabled should not process any historical blocks")

		cancel() // Stop the monitor
	})
}

func TestDeduplication(t *testing.T) {
	type nodeStream struct {
		name      string
		idxToSend []int
	}
	fixtures := loadFixtures(t)
	require.NotEmpty(t, fixtures)

	expected := extractExpectedReports(t, fixtures)
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
			name: "mixed blocks from different nodes",
			nodes: []nodeStream{
				{name: "primary", idxToSend: []int{0, 1}},
				{name: "secondary", idxToSend: []int{1, 2, 3}},
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

			// Clean up tables at START of test to ensure isolation
			_, _ = sqlDB.Exec("DROP TABLE IF EXISTS txs")
			_, _ = sqlDB.Exec("DROP TABLE IF EXISTS blocks")

			wrappedDB, err := blockdb.New(ctx, sqlDB)
			require.NoError(t, err)

			rpcCfg := Config{Backfill: true, PollInterval: 200 * time.Millisecond}
			for _, nodeCfg := range tc.nodes {
				fixturesToSend := prepareBatchToSend(nodeCfg.idxToSend, fixtures)
				app := newTestApp(t)
				app.SetBatch(fixturesToSend)

				cfg := rpctest.GetConfig(true)
				genDoc, err := ctypes.GenesisDocFromFile(cfg.GenesisFile())
				require.NoError(t, err)
				genDoc.InitialHeight = fixtureHeight(fixturesToSend[0])
				require.NoError(t, genDoc.SaveAs(cfg.GenesisFile()))

				node := rpctest.StartTendermint(app)

				t.Cleanup(func() {
					rpctest.StopTendermint(node)
				})

				rpcCfg.Nodes = append(rpcCfg.Nodes, node.Config().RPC.ListenAddress)
			}

			monitor, err := New(
				ctx,
				cryptolog.New(),
				rpcCfg,
				prometheus.NewRegistry(),
				wrappedDB,
			)
			require.NoError(t, err)

			runErr := make(chan error, 1)
			go func() {
// 				time.Sleep(2 * time.Second)
				runErr <- monitor.Run(ctx)
			}()
			t.Cleanup(func() {
				select {
				case err := <-runErr:
					if err != nil && err != context.Canceled {
						t.Fatalf("monitor run failed: %v", err)
					}
				case <-time.After(2 * time.Second):
					t.Fatalf("monitor did not stop")
				}
			})

			expectedCount := len(expected)
			require.Eventually(t, func() bool {
				actual := fetchReportsFromDB(t, sqlDB)
				return len(actual) == expectedCount
			}, 5*time.Second, 200*time.Millisecond, "expected %d reports, got %d", expectedCount, len(fetchReportsFromDB(t, sqlDB)))

			actualReports := sortReports(t, copyReports(fetchReportsFromDB(t, sqlDB)))
			expectedSorted := sortReports(t, copyReports(expected))
			require.Equal(t, expectedSorted, actualReports)

			// Clean up tables after test
			_, _ = sqlDB.Exec("DROP TABLE IF EXISTS txs")
			_, _ = sqlDB.Exec("DROP TABLE IF EXISTS blocks")
		})
	}
}
