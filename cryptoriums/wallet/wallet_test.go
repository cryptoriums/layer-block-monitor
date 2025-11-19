package wallet

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/cosmos/cosmos-sdk/codec"
	"github.com/cosmos/cosmos-sdk/codec/types"
	"github.com/cosmos/cosmos-sdk/crypto/hd"
	"github.com/cosmos/cosmos-sdk/crypto/keyring"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	cryptolog "github.com/tellor-io/layer/cryptoriums/log"
)

func TestWalletUnlock(t *testing.T) {
	dir := t.TempDir()
	const keyName = "wallet-key"

	t.Setenv("KEYRING_UNLOCK_MODE", "web")
	viper.Set("keyring-backend", "file")
	viper.Set("keyring-dir", dir)
	viper.Set("key-name", keyName)

	cdc := codec.NewProtoCodec(types.NewInterfaceRegistry())
	passCorrect := "correct-password"

	reader := strings.NewReader(passCorrect + "\n" + passCorrect + "\n") // Twice as it needs confirmation.
	kr, err := keyring.New(sdk.KeyringServiceName(), keyring.BackendFile, dir, reader, cdc)
	require.NoError(t, err)
	_, _, err = kr.NewMnemonic(keyName, keyring.English, "", "", hd.Secp256k1)
	require.NoError(t, err)

	ctx := context.Background()

	readerCh := make(chan []byte, 1)
	go func() {
		r, err := Reader(ctx, cryptolog.New(), cdc)
		require.NoError(t, err)
		b, err := io.ReadAll(r)
		require.NoError(t, err)
		readerCh <- b
	}()

	postPass(t, "bad-password", http.StatusUnauthorized)

	select {
	case <-readerCh:
		t.Fatal("reader returned even though passphrases were invalid")
	case <-time.After(200 * time.Millisecond):
	}

	postPass(t, passCorrect, http.StatusOK)

	select {
	case b := <-readerCh:
		require.Equal(t, passCorrect+"\n", string(b))
	case <-time.After(time.Second):
		t.Fatal("reader did not unblock after valid pass")
	}
}

func postPass(t *testing.T, pass string, expectStatus int) {
	t.Helper()
	require.Eventually(t, func() bool {
		resp, err := http.PostForm("http://127.0.0.1"+defaultUnlockAddr+"/", url.Values{
			PassField: {pass},
		})
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		if resp.StatusCode != expectStatus {
			t.Log("act", resp.StatusCode, "exp", expectStatus)
		}
		return resp.StatusCode == expectStatus
	}, time.Second, 50*time.Millisecond)
}
