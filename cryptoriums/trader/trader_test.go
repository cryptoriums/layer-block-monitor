package trader

import (
	"context"
	"math/big"
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

	tr, err := New(
		context.Background(),
		log.NewNopLogger(),
		prometheus.NewRegistry(),
		Config{
			Reporter:   reporter,
			ApiKey:     testApiKey,
			SecretKey:  testSecretKey,
			UseTestnet: true,
		},
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
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			start := time.Now()
			tr.HandleReward(tc.reporter, tc.amount)
			if time.Since(start) > 20*time.Millisecond {
				t.Fatalf("HandleReward blocked caller for case %s", tc.name)
			}
		})
	}

	require.Eventually(t, func() bool {
		return testutil.ToFloat64(tr.errCount.WithLabelValues("nil_amount")) == 2
	}, 200*time.Millisecond, 10*time.Millisecond, "expected nil_amount counter to reach 2")
}

func TestTraderFlushesOnShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reporter := sdk.AccAddress([]byte("reporter-address-0001"))

	tr, err := New(
		ctx,
		cryptolog.New(),
		prometheus.NewRegistry(),
		Config{
			Reporter:   reporter,
			ApiKey:     testApiKey,
			SecretKey:  testSecretKey,
			UseTestnet: true,
		},
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
