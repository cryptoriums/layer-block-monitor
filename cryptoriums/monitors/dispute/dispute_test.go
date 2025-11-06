package dispute

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"

	"cosmossdk.io/log"
	abci "github.com/cometbft/cometbft/abci/types"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/rs/zerolog"
	"github.com/tellor-io/layer/x/dispute/types"
	"gopkg.in/yaml.v2"
)

var (
	queryInterval = 50 * time.Millisecond
)

func TestPanicsOnOpenDisputes_MultiServer(t *testing.T) {
	// Helper to create a server with given dispute IDs
	createServer := func(ids []uint64) *httptest.Server {
		respObj := &types.QueryOpenDisputesResponse{
			OpenDisputes: &types.OpenDisputes{Ids: ids},
		}
		mockResponseBytes, err := yaml.Marshal(respObj)
		if err != nil {
			t.Fatalf("Failed to marshal response: %v", err)
		}
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(200)
			w.Write(mockResponseBytes)
		}))
	}

	// Case 1: One server returns open disputes, one returns none
	serverOpen := createServer([]uint64{1, 2, 3})
	defer serverOpen.Close()
	serverNone := createServer([]uint64{})
	defer serverNone.Close()

	cfg := Config{Nodes: []string{serverOpen.URL, serverNone.URL}}
	logger := log.NewLogger(os.Stderr, log.LevelOption(zerolog.DebugLevel), log.ColorOption(false))
	monitor := New(logger, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	panicCh := make(chan interface{}, 1)
	doneCh := make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				panicCh <- r
			}
			close(doneCh)
		}()
		monitor.Start(ctx, queryInterval)
	}()
	select {
	case p := <-panicCh:
		if msg, ok := p.(string); ok {
			if msg != ReasonOpenDisputes {
				t.Fatalf("Unexpected panic message: %v", msg)
			}
			t.Logf("Monitor panicked as expected (one open, one none): %v", msg)
		} else {
			t.Fatalf("Panic was not a string: %v", p)
		}
	case <-doneCh:
		t.Fatal("Monitor exited without panicking on open disputes (one open, one none)")
	case <-time.After(5 * queryInterval):
		t.Fatal("Timeout: Monitor did not panic on open disputes (one open, one none)")
	}

	// Case 2: Both servers return open disputes
	serverOpen2 := createServer([]uint64{4, 5})
	defer serverOpen2.Close()
	cfg2 := Config{Nodes: []string{serverOpen.URL, serverOpen2.URL}}
	monitor = New(logger, cfg2)
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	panicCh = make(chan interface{}, 1)
	doneCh = make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				panicCh <- r
			}
			close(doneCh)
		}()
		monitor.Start(ctx, queryInterval)
	}()
	select {
	case p := <-panicCh:
		if msg, ok := p.(string); ok {
			if msg != ReasonOpenDisputes {
				t.Fatalf("Unexpected panic message: %v", msg)
			}
			t.Logf("Monitor panicked as expected (both open): %v", msg)
		} else {
			t.Fatalf("Panic was not a string: %v", p)
		}
	case <-doneCh:
		t.Fatal("Monitor exited without panicking on open disputes (both open)")
	case <-time.After(5 * queryInterval):
		t.Fatal("Timeout: Monitor did not panic on open disputes (both open)")
	}
}

func TestPanicsOnTooManyErrors(t *testing.T) {
	// Always return error from server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		w.Write([]byte("error"))
	}))
	defer server.Close()

	cfg := Config{Nodes: []string{server.URL}}
	logger := log.NewLogger(os.Stderr, log.LevelOption(zerolog.DebugLevel), log.ColorOption(false))
	monitor := New(logger, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	panicCh := make(chan interface{}, 1)
	doneCh := make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				panicCh <- r
			}
			close(doneCh)
		}()
		monitor.Start(ctx, queryInterval)
	}()

	select {
	case p := <-panicCh:
		if msg, ok := p.(string); ok {
			if msg != ReasonTooManyErrors {
				t.Fatalf("Unexpected panic message: %v", msg)
			}
			t.Logf("Monitor panicked as expected (too many errors): %v", msg)
		} else {
			t.Fatalf("Panic was not a string: %v", p)
		}
	case <-doneCh:
		t.Fatal("Monitor exited without panicking on too many errors")
	case <-time.After((ErrorThreshold + 5) * queryInterval):
		t.Fatal("Timeout: Monitor did not panic on too many errors")
	}
}

func TestDoesNotPanicOnTooManyErrorsIfOneServerSucceeds(t *testing.T) {
	// Server 1: always errors
	serverErr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		w.Write([]byte("error"))
	}))
	defer serverErr.Close()

	// Server 2: always succeeds (no open disputes)
	respObj := &types.QueryOpenDisputesResponse{
		OpenDisputes: &types.OpenDisputes{Ids: []uint64{}},
	}
	mockResponseBytes, _ := yaml.Marshal(respObj)
	serverOK := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write(mockResponseBytes)
	}))
	defer serverOK.Close()

	cfg := Config{Nodes: []string{serverErr.URL, serverOK.URL}}
	logger := log.NewLogger(os.Stderr, log.LevelOption(zerolog.DebugLevel), log.ColorOption(false))
	monitor := New(logger, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	panicCh := make(chan interface{}, 1)
	doneCh := make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				panicCh <- r
			}
			close(doneCh)
		}()
		monitor.Start(ctx, queryInterval)
	}()

	select {
	case p := <-panicCh:
		if msg, ok := p.(string); ok {
			if msg == ReasonTooManyErrors {
				t.Fatalf("Monitor panicked for too many errors, but at least one server succeeded: %v", msg)
			}
			// If it panics for open disputes, that's fine (not this test's concern)
		} else {
			t.Fatalf("Panic was not a string: %v", p)
		}
	case <-time.After((ErrorThreshold + 5) * queryInterval):
		// Success: did not panic for too many errors
	}
}

func TestDoesNoPanicWhenDisputeIsIgnored(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_ = yaml.NewEncoder(w).Encode(&types.QueryOpenDisputesResponse{
			OpenDisputes: &types.OpenDisputes{Ids: []uint64{42}},
		})
	}))
	defer server.Close()

	cfg := Config{
		Nodes:          []string{server.URL},
		IgnoreDisputes: []uint64{42},
	}
	logger := log.NewLogger(os.Stderr, log.LevelOption(zerolog.DebugLevel), log.ColorOption(false))
	monitor := New(logger, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	panicCh := make(chan interface{}, 1)
	doneCh := make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				panicCh <- r
			}
			close(doneCh)
		}()
		monitor.Start(ctx, queryInterval)
	}()

	select {
	case p := <-panicCh:
		t.Fatalf("Monitor panicked despite ignored dispute: %v", p)
	case <-doneCh:
		t.Fatal("Monitor exited unexpectedly while disputes were ignored")
	case <-time.After(5 * queryInterval):
		// Expected path: no panic within observation window.
	}

	cancel()
	<-doneCh

	if got := testutil.ToFloat64(monitor.openDisputesCount); got != 1 {
		t.Fatalf("unexpected open dispute gauge value: got %v, want 1", got)
	}
}

func TestEventHandlingNoPanicWhenDisputeIsIgnored(t *testing.T) {
	cfg := Config{
		IgnoreDisputes: []uint64{7},
	}
	logger := log.NewLogger(os.Stderr, log.LevelOption(zerolog.DebugLevel), log.ColorOption(false))
	monitor := New(logger, cfg)

	ev := abci.Event{
		Type: "new_dispute",
		Attributes: []abci.EventAttribute{
			{Key: "dispute_id", Value: strconv.FormatUint(7, 10)},
		},
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Unexpected panic when dispute is ignored: %v", r)
		}
	}()

	monitor.HandleDisputeEvent(ev)

	if got := testutil.ToFloat64(monitor.disputesEventsCount); got != 1 {
		t.Fatalf("unexpected dispute event counter value: got %v, want 1", got)
	}
}

func TestEventHandling(t *testing.T) {
	cfg := Config{}
	logger := log.NewLogger(os.Stderr, log.LevelOption(zerolog.DebugLevel), log.ColorOption(false))
	monitor := New(logger, cfg)

	ev := abci.Event{
		Type: "new_dispute",
		Attributes: []abci.EventAttribute{
			{Key: "dispute_id", Value: strconv.FormatUint(999, 10)},
		},
	}

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("Expected panic for unignored dispute event but got none")
		} else if msg, ok := r.(string); ok {
			if msg != ReasonOpenDisputes {
				t.Fatalf("Unexpected panic message: %v", msg)
			}
		} else {
			t.Fatalf("Panic was not a string: %v", r)
		}

		if got := testutil.ToFloat64(monitor.disputesEventsCount); got != 1 {
			t.Fatalf("unexpected dispute event counter value: got %v, want 1", got)
		}
	}()

	monitor.HandleDisputeEvent(ev)
}
