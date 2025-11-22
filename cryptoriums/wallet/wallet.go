package wallet

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"cosmossdk.io/log"
	"github.com/cosmos/cosmos-sdk/codec"
	"github.com/cosmos/cosmos-sdk/crypto/keyring"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/spf13/viper"
)

const (
	defaultUnlockAddr = ":8888"
	shutdownTimeout   = 5 * time.Second
	PassField         = "pass"
)

type unlockServer struct {
	logger   log.Logger
	server   *http.Server
	passCh   chan string
	validate func(string) error
}

func newUnlockServer(logger log.Logger, validate func(string) error) *unlockServer {
	mux := http.NewServeMux()
	us := &unlockServer{
		logger:   logger.With("component", "wallet"),
		passCh:   make(chan string, 1),
		validate: validate,
	}
	mux.HandleFunc("/", us.handleUnlock)
	us.server = &http.Server{
		Addr:    defaultUnlockAddr,
		Handler: mux,
	}
	return us
}

func (u *unlockServer) Start(ctx context.Context) {
	go func() {
		<-ctx.Done()
		u.shutdown(ctx)
	}()

	u.logger.Info("wallet unlock server starting", "addr", u.server.Addr)
	if err := u.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		u.logger.Error("wallet unlock server", "error", err)
	}
}

func (u *unlockServer) handleUnlock(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "only POST", http.StatusMethodNotAllowed)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	pass := strings.TrimSpace(r.FormValue(PassField))
	if pass == "" {
		http.Error(w, "passphrase required", http.StatusBadRequest)
		return
	}
	if err := u.validate(pass); err != nil {
		http.Error(w, "invalid passphrase", http.StatusUnauthorized)
		u.logger.Debug("unlock failed", "error", err)
		return
	}
	select {
	case u.passCh <- pass:
	default:
		select {
		case <-u.passCh:
		default:
		}
		u.passCh <- pass
	}
	if _, err := w.Write([]byte("passphrase received")); err != nil {
		u.logger.Error("write unlock response", "error", err)
	}
}

func (u *unlockServer) waitForPassphrase(ctx context.Context) (string, error) {
	select {
	case pass := <-u.passCh:
		return pass, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (u *unlockServer) shutdown(ctx context.Context) {
	shutdownCtx, cancel := context.WithTimeout(ctx, shutdownTimeout)
	defer cancel()
	if err := u.server.Shutdown(shutdownCtx); err != nil && err != http.ErrServerClosed {
		u.logger.Error("shutdown unlock server", "error", err)
	}
}

// Reader returns a reader that yields the validated passphrase. In web mode it blocks until
// the unlock endpoint supplies a correct passphrase; otherwise it returns stdin.
func Reader(ctx context.Context, l log.Logger, c codec.Codec) (io.Reader, error) {
	if !strings.EqualFold(os.Getenv("KEYRING_UNLOCK_MODE"), "web") {
		return os.Stdin, nil
	}
	srv := newUnlockServer(l, func(pass string) error { return validatePass(pass, c) })
	go srv.Start(ctx)

	pass, err := srv.waitForPassphrase(ctx)
	if err != nil {
		return nil, err
	}
	srv.shutdown(ctx)
	return strings.NewReader(pass + "\n"), nil
}

func validatePass(pass string, c codec.Codec) error {
	if c == nil {
		return fmt.Errorf("wallet codec not provided")
	}

	kr, err := newKeyring(pass, c)
	if err != nil {
		return err
	}

	keyName := viper.GetString("key-name")
	if keyName == "" {
		return fmt.Errorf("key-name not set")
	}

	_, _, err = kr.Sign(keyName, []byte("wallet unlock validation"), 1)
	return err
}

func newKeyring(pass string, c codec.Codec) (keyring.Keyring, error) {
	krBackend := viper.GetString("keyring-backend")
	if krBackend == "" {
		return nil, fmt.Errorf("keyring-backend not set")
	}
	krDir := viper.GetString("keyring-dir")
	if krDir == "" {
		krDir = viper.GetString("home")
	}
	if krDir == "" {
		return nil, fmt.Errorf("keyring directory not set")
	}

	return keyring.New(sdk.KeyringServiceName(), krBackend, krDir, strings.NewReader(pass), c)
}
