package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	cmtjson "github.com/cometbft/cometbft/libs/json"
	abci "github.com/cometbft/cometbft/abci/types"
	coretypes "github.com/cometbft/cometbft/rpc/core/types"
	ctypes "github.com/cometbft/cometbft/types"
	"github.com/stretchr/testify/require"
)

var targetHeights = []int64{10037791, 10037792, 10037793, 10037794}

func TestCaptureBlocks(t *testing.T) {
	ctx := context.Background()
	baseURL := "https://mainnet.tellorlayer.com/rpc"

	var fixtures []ctypes.EventDataNewBlock

	for _, height := range targetHeights {
		blockResp := fetchJSON(ctx, t, fmt.Sprintf("%s/block?height=%d", baseURL, height))
		resultsResp := fetchJSON(ctx, t, fmt.Sprintf("%s/block_results?height=%d", baseURL, height))

		var blockData struct {
			Result coretypes.ResultBlock `json:"result"`
		}
		require.NoError(t, cmtjson.Unmarshal(blockResp, &blockData))

		var resultsData struct {
			Result coretypes.ResultBlockResults `json:"result"`
		}
		require.NoError(t, cmtjson.Unmarshal(resultsResp, &resultsData))

		finalize := abci.ResponseFinalizeBlock{
			Events:                resultsData.Result.FinalizeBlockEvents,
			ValidatorUpdates:      resultsData.Result.ValidatorUpdates,
			ConsensusParamUpdates: resultsData.Result.ConsensusParamUpdates,
			AppHash:               resultsData.Result.AppHash,
		}
		if len(resultsData.Result.TxsResults) > 0 {
			finalize.TxResults = make([]*abci.ExecTxResult, len(resultsData.Result.TxsResults))
			for i, tx := range resultsData.Result.TxsResults {
				if tx != nil {
					copyTx := *tx
					finalize.TxResults[i] = &copyTx
				}
			}
		}

		fixtures = append(fixtures, ctypes.EventDataNewBlock{
			Block:               blockData.Result.Block,
			BlockID:             blockData.Result.BlockID,
			ResultFinalizeBlock: finalize,
		})

		t.Logf("captured block %d", height)
	}

	outPath := filepath.Join(".", "blocks_fixtures.json")
	require.NoError(t, os.MkdirAll(filepath.Dir(outPath), 0o755))
	outFile, err := os.Create(outPath)
	require.NoError(t, err)
	defer outFile.Close()

	enc := json.NewEncoder(outFile)
	enc.SetIndent("", "  ")
	require.NoError(t, enc.Encode(fixtures))
}

func fetchJSON(ctx context.Context, t *testing.T, url string) []byte {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return raw
}
