package dispute

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"cosmossdk.io/log"
	"github.com/rs/zerolog"
	"github.com/tellor-io/layer/x/dispute/types"
	"gopkg.in/yaml.v2"
)

var (
	queryInterval = 50 * time.Millisecond
)

func TestMonitorPanicsOnOpenDisputes_MultiServer(t *testing.T) {
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

	cfg := Config{Nodes: []string{serverOpen.URL, serverNone.URL}, ErrorThreshold: 2}
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
	cfg2 := Config{Nodes: []string{serverOpen.URL, serverOpen2.URL}, ErrorThreshold: 2}
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

func TestMonitorPanicsOnTooManyErrors(t *testing.T) {
	// Always return error from server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		w.Write([]byte("error"))
	}))
	defer server.Close()

	cfg := Config{Nodes: []string{server.URL}, ErrorThreshold: 2}
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
	case <-time.After(5 * queryInterval):
		t.Fatal("Timeout: Monitor did not panic on too many errors")
	}
}

func TestMonitorDoesNotPanicOnTooManyErrorsIfOneServerSucceeds(t *testing.T) {
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

	cfg := Config{Nodes: []string{serverErr.URL, serverOK.URL}, ErrorThreshold: 2}
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
	case <-time.After(5 * queryInterval):
		// Success: did not panic for too many errors
	}
}
