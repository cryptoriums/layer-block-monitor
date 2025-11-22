package dispute

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"cosmossdk.io/log"
	ctypes "github.com/cometbft/cometbft/abci/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tellor-io/layer/x/dispute/types"
	"gopkg.in/yaml.v2"
)

const (
	Component           = "dispute_monitor"
	ReasonOpenDisputes  = "there are open disputes so not safe to continue reporting, if the open disputes are safe to ignore add their IDs in the config"
	ReasonTooManyErrors = "too many consecutive errors querying dispute endpoints"
	ErrorThreshold      = 20
)

type Config struct {
	Nodes          []string `yaml:"nodes"`
	IgnoreDisputes []uint64
}

type Monitor struct {
	cfg                 Config
	errorCounter        *prometheus.CounterVec
	openDisputesCount   prometheus.Gauge
	disputesEventsCount prometheus.Counter
	logger              log.Logger
	mtx                 sync.Mutex
}

func New(logger log.Logger, cfg Config) *Monitor {
	errorCounter := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dispute_query_errors_total",
			Help: "Total errors querying dispute endpoints per node",
		},
		[]string{"node"},
	)
	openDisputesCount := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "dispute_open_disputes_count",
			Help: "Number of open disputes reported by the last successful query",
		},
	)
	disputesEventsCount := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "dispute_events_total",
			Help: "Total dispute events processed by the monitor",
		},
	)

	return &Monitor{
		cfg:                 cfg,
		errorCounter:        errorCounter,
		openDisputesCount:   openDisputesCount,
		disputesEventsCount: disputesEventsCount,
		logger:              logger.With("component", Component),
	}
}

func (m *Monitor) ReloadCfg(cfg Config) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.cfg = cfg
}

func (m *Monitor) Start(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	consecutiveErrors := 0

	for {
		select {
		case <-ctx.Done():
			m.logger.Info("Monitor stopped")
			return
		case <-ticker.C:
			cfg := m.cfg
			var wg sync.WaitGroup
			type disputeResult struct {
				node    string
				openIDs []uint64
				err     error
			}
			resultsCh := make(chan disputeResult, len(cfg.Nodes))
			for _, node := range cfg.Nodes {
				wg.Add(1)
				go func(node string) {
					defer wg.Done()
					result, err := m.queryDisputes(node, interval)
					if err != nil {
						m.logger.Error("error querying node", "url", node, "error", err)
						resultsCh <- disputeResult{node: node, openIDs: nil, err: err}
						return
					}
					if result == nil || result.OpenDisputes == nil {
						resultsCh <- disputeResult{node: node, openIDs: nil, err: fmt.Errorf("no open disputes")}
						return
					}
					resultsCh <- disputeResult{node: node, openIDs: result.OpenDisputes.Ids, err: nil}
				}(node)
			}
			wg.Wait()
			close(resultsCh)

			errorCount := 0
			for res := range resultsCh {
				m.openDisputesCount.Set(float64(len(res.openIDs)))
				if res.err != nil {
					errorCount++
					continue
				}
				if len(res.openIDs) > 0 {
					m.logger.Warn("open dispute detected", "node", res.node, "ids", res.openIDs)
				}
				for _, disputeID := range res.openIDs {
					m.panicWhenNotIgnored(disputeID)
				}
			}

			if errorCount == len(cfg.Nodes) {
				consecutiveErrors++
				if consecutiveErrors > ErrorThreshold {
					m.logger.Error("Too many consecutive errors, panicking")
					panic(ReasonTooManyErrors)
				}
				continue
			}
			consecutiveErrors = 0
		}
	}
}

func (m *Monitor) HandleDisputeEvent(ev ctypes.Event) {
	m.disputesEventsCount.Inc()

	for _, attr := range ev.Attributes {
		attrVal := attr.Value
		if attr.Key == "dispute_id" {
			disputeID, err := ParseDisputeID(attrVal)
			if err != nil {
				panic("parsing dispute ID")
			}

			m.panicWhenNotIgnored(disputeID)

		}
	}
}

func (m *Monitor) panicWhenNotIgnored(disputeID uint64) {
	m.mtx.Lock()
	cfg := m.cfg
	m.mtx.Unlock()
	var noPanic bool
	for _, ignoreID := range cfg.IgnoreDisputes {
		if ignoreID == disputeID {
			noPanic = true
			break
		}
	}
	if !noPanic {
		panic(ReasonOpenDisputes)
	}
}

func (m *Monitor) queryDisputes(node string, interval time.Duration) (*types.QueryOpenDisputesResponse, error) {
	url := fmt.Sprintf("%s/tellor-io/layer/dispute/open-disputes", node)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		m.errorCounter.WithLabelValues(node).Inc()
		return nil, err
	}
	client := &http.Client{Timeout: 2 * interval}
	resp, err := client.Do(req)
	if err != nil {
		m.errorCounter.WithLabelValues(node).Inc()
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		m.errorCounter.WithLabelValues(node).Inc()
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		m.errorCounter.WithLabelValues(node).Inc()
		return nil, err
	}

	var result types.QueryOpenDisputesResponse
	if err := yaml.Unmarshal(body, &result); err != nil {
		m.errorCounter.WithLabelValues(node).Inc()
		return nil, err
	}
	return &result, nil
}

// ParseDisputeID converts decimal strings to uint64.
func ParseDisputeID(val string) (uint64, error) {
	return strconv.ParseUint(val, 10, 64)
}
