package dispute

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"cosmossdk.io/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tellor-io/layer/x/dispute/types"
	"gopkg.in/yaml.v2"
)

const (
	Component           = "dispute_monitor"
	ReasonOpenDisputes  = "there are open disputes so not safe to continue reporting, if the open disputes are safe to ignore add their IDs in the config"
	ReasonTooManyErrors = "too many consecutive errors querying dispute endpoints"
)

type Config struct {
	Nodes          []string `yaml:"nodes"`
	ErrorThreshold int      `yaml:"errorThreshold"`
}

type Monitor struct {
	cfg          Config
	errorCounter *prometheus.CounterVec
	logger       log.Logger
}

func New(logger log.Logger, cfg Config) *Monitor {
	errorCounter := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "dispute_query_errors_total",
			Help: "Total errors querying dispute endpoints per node",
		},
		[]string{"node"},
	)
	if cfg.ErrorThreshold == 0 {
		cfg.ErrorThreshold = 20
	}
	return &Monitor{
		cfg:          cfg,
		errorCounter: errorCounter,
		logger:       logger.With("component", Component),
	}
}

func (m *Monitor) ReloadCfg(cfg Config) {
	if cfg.ErrorThreshold == 0 {
		cfg.ErrorThreshold = 20
	}
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
					m.logger.Warn("there are open disputes", "node", node, "ids", result.OpenDisputes.Ids)
					resultsCh <- disputeResult{node: node, openIDs: result.OpenDisputes.Ids, err: nil}
				}(node)
			}
			wg.Wait()
			close(resultsCh)

			shouldPanic := false
			errorCount := 0
			for res := range resultsCh {
				if res.err != nil {
					errorCount++
				}
				if len(res.openIDs) > 0 {
					shouldPanic = true
					m.logger.Warn("open dispute detected", "node", res.node, "ids", res.openIDs)
				}
			}
			if shouldPanic {
				panic(ReasonOpenDisputes)
			}
			if errorCount == len(cfg.Nodes) {
				consecutiveErrors++
				if consecutiveErrors > cfg.ErrorThreshold {
					m.logger.Error("Too many consecutive errors, panicking")
					panic(ReasonTooManyErrors)
				}
				continue
			}
			consecutiveErrors = 0
		}
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
