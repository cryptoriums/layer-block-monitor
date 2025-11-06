package trader

import (
	"context"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"cosmossdk.io/log"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	cryptolog "github.com/tellor-io/layer/cryptoriums/log"
)

const (
	testApiKey    = "3QuKuE8c1GZEpwD8p6oTi7wEVWDywpDIE6RRooMoGwpCKRbewpQ1gAnwvLO1HmwK"
	testSecretKey = "eH5IWVhkO4VjNfKOfZSj9hDL9CxHJhqy9WhkKqKVg2DvHgWeCGQ5moHPvtEIzHjK"
)

func TestHandleRewardIsNonBlocking(t *testing.T) {
	reporter := sdk.AccAddress([]byte("reporter-address-0001"))

	cfg := newTestConfig(t, reporter)
	tr, err := New(
		context.Background(),
		log.NewNopLogger(),
		prometheus.NewRegistry(),
		cfg,
	)
	require.NoError(t, err)

	testCases := []struct {
		name     string
		reporter sdk.AccAddress
		amount   *big.Int
	}{
		{"nilReporterRealAmount", nil, big.NewInt(1)},
		{"realReporterNilAmount", reporter, nil},
		{"nilReporterNilAmount", nil, nil},
		{"realReporterRealAmount", reporter, big.NewInt(1)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			start := time.Now()
			tr.HandleReward(tc.reporter, tc.amount)
			if time.Since(start) > 100*time.Millisecond {
				t.Fatalf("HandleReward blocked caller for case %s", tc.name)
			}
		})
	}

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(tr.errCount.WithLabelValues("nil_amount")) == 2
	}, time.Second, 10*time.Millisecond, "expected nil_amount counter to reach 2")
}

func TestTraderFlushesOnShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reporter := sdk.AccAddress([]byte("reporter-address-0001"))

	cfg := newTestConfig(t, reporter)
	tr, err := New(
		ctx,
		cryptolog.New(),
		prometheus.NewRegistry(),
		cfg,
	)
	require.NoError(t, err)

	initial := testutil.ToFloat64(tr.btcConverted.WithLabelValues(tr.reporter.String()))
	require.Equal(t, 0.0, initial, "metric should start at zero")

	smallAmount := big.NewInt(4e17) // 0.1 TRB
	tr.HandleReward(reporter, smallAmount)

	time.Sleep(100 * time.Millisecond)
	if got := testutil.ToFloat64(tr.btcConverted.WithLabelValues(tr.reporter.String())); got != 0 {
		t.Fatalf("metric changed unexpectedly before flush: got %v", got)
	}

	cancel()

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(tr.btcConverted.WithLabelValues(tr.reporter.String())) > 0
	}, 10*time.Second, 10*time.Millisecond, "btcConverted metric did not increase after shutdown flush")
}

func newTestConfig(t *testing.T, reporter sdk.AccAddress) Config {
	t.Helper()

	cfg := Config{
		Reporter:  reporter,
		ApiKey:    testApiKey,
		SecretKey: testSecretKey,
	}

	if isGitHubActions() {
		server := newMockBinanceServer(t)
		cfg.BinanceAPIURL = server.URL
		t.Log("GITHUB actions - started a mock binance server")
	} else {
		cfg.BinanceAPIURL = "https://testnet.binance.vision"
	}

	return cfg
}

func isGitHubActions() bool {
	return os.Getenv("GITHUB_ACTIONS") == "true"
}

func newMockBinanceServer(t *testing.T) *httptest.Server {
	t.Helper()

	handler := http.NewServeMux()

	handler.HandleFunc("/api/v3/account", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{
			"makerCommission":0,
			"takerCommission":0,
			"buyerCommission":0,
			"sellerCommission":0,
			"canTrade":true,
			"canWithdraw":true,
			"canDeposit":true,
			"updateTime":0,
			"accountType":"SPOT",
			"balances":[
				{"asset":"TRB","free":"100.0","locked":"0"},
				{"asset":"USDC","free":"1000.0","locked":"0"},
				{"asset":"BTC","free":"0.01","locked":"0"}
			],
			"permissions":["SPOT"]
		}`)
	})

	handler.HandleFunc("/api/v3/ticker/price", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `[{"symbol":"BTCUSDC","price":"50000"}]`)
	})

	handler.HandleFunc("/api/v3/order", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := r.ParseForm(); err != nil {
			t.Fatalf("failed to parse form: %v", err)
		}
		switch r.Form.Get("symbol") {
		case "TRBUSDC":
			fmt.Fprint(w, `{
				"symbol":"TRBUSDC",
				"orderId":1,
				"clientOrderId":"test-sell",
				"transactTime":1700000000000,
				"price":"0",
				"origQty":"0.50",
				"executedQty":"0.50",
				"cummulativeQuoteQty":"25",
				"status":"FILLED",
				"timeInForce":"GTC",
				"type":"MARKET",
				"side":"SELL",
				"fills":[]
			}`)
		case "BTCUSDC":
			fmt.Fprint(w, `{
				"symbol":"BTCUSDC",
				"orderId":2,
				"clientOrderId":"test-buy",
				"transactTime":1700000000100,
				"price":"0",
				"origQty":"0.0019",
				"executedQty":"0.0019",
				"cummulativeQuoteQty":"90",
				"status":"FILLED",
				"timeInForce":"GTC",
				"type":"MARKET",
				"side":"BUY",
				"fills":[]
			}`)
		default:
			http.Error(w, "unsupported symbol", http.StatusBadRequest)
		}
	})

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}
