package trader

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"sync"
	"time"

	"cosmossdk.io/log"
	"github.com/adshao/go-binance/v2"
	sdk "github.com/cosmos/cosmos-sdk/types"
	bigpkg "github.com/cryptoriums/packages/big"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/tellor-io/layer/cryptoriums"
)

const (
	componentName       = "trader"
	minimumTradeBalance = 0.5
)

// Config controls reporter access and execution behavior.
type Config struct {
	Reporter   sdk.AccAddress `json:"reporter" yaml:"reporter"`
	ApiKey     string
	SecretKey  string
	UseTestnet bool
}

// Trader executes trades and tracks balances for the configured reporter.
type Trader struct {
	logger             log.Logger
	trader             *binance.Client
	errCount           *prometheus.CounterVec
	btcConverted       *prometheus.CounterVec
	tokenBalanceTrader *prometheus.GaugeVec

	reporter sdk.AccAddress

	mu              sync.Mutex
	accumulatedTRB  float64
	accumulatedUSDC float64
}

func New(
	ctx context.Context,
	logger log.Logger,
	reg prometheus.Registerer,
	cfg Config,
) (*Trader, error) {
	logger = logger.With("component", componentName)

	if cfg.Reporter.Empty() {
		return nil, fmt.Errorf("trader reporter must be set")
	}

	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if cfg.UseTestnet {
		binance.UseTestnet = true
	}

	bclient := binance.NewClient(cfg.ApiKey, cfg.SecretKey)
	_, err := bclient.NewGetAccountService().Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("binance client initialization:%w", err)
		// invalid key/secret or permission issue (look at err.Error())
	}

	trader := &Trader{
		logger:   logger,
		trader:   bclient,
		reporter: cfg.Reporter,
		errCount: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: cryptoriums.MetricsNamespace,
			Subsystem: componentName,
			Name:      "errors_total",
			Help:      "Errors when sending a trade request",
		}, []string{"reason"}),
		btcConverted: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: cryptoriums.MetricsNamespace,
			Subsystem: componentName,
			Name:      "btc_converted",
			Help:      "The total BTC forwarded to the reporter",
		}, []string{"reporter"}),
		tokenBalanceTrader: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Namespace: cryptoriums.MetricsNamespace,
			Subsystem: componentName,
			Name:      "balance_trader",
			Help:      "The current trader token balances",
		}, []string{"token"}),
	}

	go trader.awaitShutdown(ctx)

	return trader, nil
}

// HandleReward executes the trading flow when the reward belongs to the configured reporter.
func (m *Trader) HandleReward(reporter sdk.AccAddress, amount *big.Int) {
	if amount == nil {
		m.errCount.WithLabelValues("nil_amount").Inc()
		m.logger.Error("received nil reward amount")
		return
	}
	if !reporter.Equals(m.reporter) {
		return
	}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := m.performTrade(ctx, amount, false); err != nil {
			switch {
			case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
				m.errCount.WithLabelValues("timeout").Inc()
				m.logger.Error("trade timed out", "err", err)
			default:
				m.errCount.WithLabelValues("trade_execution").Inc()
				m.logger.Error("failed executing trade", "err", err)
			}
		}
	}()
}

// forceFlush drains accumulated balances regardless of thresholds.
func (m *Trader) forceFlush() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := m.performTrade(ctx, big.NewInt(0), true)
	if err != nil {
		m.errCount.WithLabelValues("force_flush").Inc()
	}
	return err
}

func (m *Trader) awaitShutdown(ctx context.Context) {
	<-ctx.Done()
	if err := m.forceFlush(); err != nil {
		m.logger.Error("force flush on shutdown", "err", err)
	}
}

func (m *Trader) performTrade(ctx context.Context, trbToConvertB *big.Int, force bool) (err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	defer func() {
		if balErr := m.refreshBalances(ctx); balErr != nil {
			m.logger.Error("updating trader balances", "err", balErr)
			m.errCount.WithLabelValues("balance_refresh").Inc()
		}
	}()

	trbToConvert := bigpkg.ToFloatDiv(trbToConvertB, 1e18)
	if trbToConvert > 300 {
		return fmt.Errorf("unexpected value for a trade TRB:%v", trbToConvert)
	}

	m.accumulatedTRB += trbToConvert
	if m.accumulatedTRB < minimumTradeBalance && !force {
		m.logger.Info("not enough accumulated balance", "balance", m.accumulatedTRB)
		return nil
	}

	q := fmt.Sprintf("%.2f", m.accumulatedTRB)
	order, err := m.trader.NewCreateOrderService().
		Symbol("TRBUSDC").
		Side(binance.SideTypeSell).
		Type(binance.OrderTypeMarket).
		Quantity(q).
		Do(ctx)
	if err != nil {
		return fmt.Errorf("executing TRBUSDC trade accumulated TRB:%v: %w", q, err)
	}

	m.accumulatedTRB = 0

	cumulativeQuoteQuantity, err := strconv.ParseFloat(order.CummulativeQuoteQuantity, 64)
	if err != nil {
		return fmt.Errorf("parse CummulativeQuoteQuantity: %w", err)
	}

	m.accumulatedUSDC += cumulativeQuoteQuantity // Use accumulatedUSDC so that if the next trade fails it is executed on the next run.

	m.logger.Info(
		"executed order",
		"symbol", order.Symbol,
		"type", order.Side,
		"TRB-in", q,
		"USDC-out", order.CummulativeQuoteQuantity,
	)

	select {
	case <-time.After(time.Second): // The binance api needs time to process the first order.
	case <-ctx.Done():
		return ctx.Err()
	}

	amountAfterFee := m.accumulatedUSDC - (m.accumulatedUSDC * 0.01)
	btcOut, err := m.buyBTC(ctx, amountAfterFee)
	if err != nil {
		return fmt.Errorf("executing BTCUSDC trade accumulatedUSDC:%v: %w", amountAfterFee, err)
	}

	m.logger.Info(
		"executed order",
		"USDC-in-accumulated", m.accumulatedUSDC,
		"afterTradeFee", amountAfterFee,
		"BTC-out", btcOut,
	)
	m.accumulatedUSDC = 0

	m.btcConverted.WithLabelValues(m.reporter.String()).Add(btcOut)

	return nil
}

func (m *Trader) buyBTC(ctx context.Context, amountUSDC float64) (float64, error) {
	priceStr, err := m.trader.NewListPricesService().Symbol("BTCUSDC").Do(ctx)
	if err != nil {
		return 0, fmt.Errorf("getting BTCUSDC price: %w", err)
	}
	price, err := strconv.ParseFloat(priceStr[0].Price, 64)
	if err != nil {
		return 0, fmt.Errorf("parse BTCUSDC price:%v: %w", priceStr[0].Price, err)
	}

	quantity := amountUSDC / price
	quantity = math.Round(quantity/0.0001) * 0.0001

	order, err := m.trader.NewCreateOrderService().
		Symbol("BTCUSDC").
		Side(binance.SideTypeBuy).
		Type(binance.OrderTypeMarket).
		Quantity(fmt.Sprintf("%f", quantity)).
		Do(ctx)
	if err != nil {
		return 0, fmt.Errorf("executing BTCUSDC trade: %w", err)
	}
	btcOut, err := strconv.ParseFloat(order.ExecutedQuantity, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing ExecutedQuantity to float: %w", err)
	}

	return btcOut, nil
}

func (m *Trader) refreshBalances(parent context.Context) error {
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()

	resp, err := m.trader.NewGetAccountService().Do(ctx)
	if err != nil {
		m.errCount.WithLabelValues("balance_fetch").Inc()
		return fmt.Errorf("NewGetAccountService: %w", err)
	}

	for _, balance := range resp.Balances {
		value, err := strconv.ParseFloat(balance.Free, 64)
		if err != nil {
			m.errCount.WithLabelValues("balance_parse").Inc()
			return fmt.Errorf("parsing trader balance to float: %w", err)
		}
		m.tokenBalanceTrader.WithLabelValues(balance.Asset).Set(value)
	}

	return nil
}
