package flashblocks

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	opclient "github.com/ethereum-optimism/optimism/op-service/client"
	"github.com/ethereum/go-ethereum/log"
)

type Flashblock struct {
	PayloadID string `json:"payload_id"`
	Index     int    `json:"index"`
	Diff      struct {
		StateRoot    string `json:"state_root"`
		ReceiptsRoot string `json:"receipts_root"`
		LogsBloom    string `json:"logs_bloom"`
		GasUsed      string `json:"gas_used"`
		BlockHash    string `json:"block_hash"`
		Transactions []any  `json:"transactions"`
		Withdrawals  []any  `json:"withdrawals"`
	} `json:"diff"`
	Metadata struct {
		BlockNumber        int                    `json:"block_number"`
		NewAccountBalances map[string]string      `json:"new_account_balances"`
		Receipts           map[string]interface{} `json:"receipts"`
	} `json:"metadata"`
}

type FlashblocksStreamMode string

const (
	FlashblocksStreamMode_Leader   FlashblocksStreamMode = "leader"
	FlashblocksStreamMode_Follower FlashblocksStreamMode = "follower"
)

// UnmarshalJSON implements custom unmarshaling for Flashblock to lower case the keys of .metadata.new_account_balances.
func (f *Flashblock) UnmarshalJSON(data []byte) error {
	type TempFlashblock Flashblock // need a type alias to avoid infinite recursion
	temp := (*TempFlashblock)(f)

	if err := json.Unmarshal(data, temp); err != nil {
		return err
	}
	if f.Metadata.NewAccountBalances == nil {
		return nil
	}

	loweredBalances := make(map[string]string)
	for key, value := range f.Metadata.NewAccountBalances {
		loweredBalances[strings.ToLower(key)] = value
	}
	f.Metadata.NewAccountBalances = loweredBalances

	return nil
}

// listenForFlashblocks reads flashblocks from the given websocket client for the
// specified duration. This mirrors the devstack DSL helper but lives inside the
// test package so acceptance tests can reuse the logic.
func listenForFlashblocks(logger log.Logger, wsClient *opclient.WSClient, duration time.Duration, output chan<- []byte, done chan<- struct{}) error {
	defer close(done)

	logger.Info("Listening on WebSocket client", "duration", duration)

	timeout := time.After(duration)
	messageCount := 0
	for {
		select {
		case <-timeout:
			logger.Info("WebSocket read timeout reached", "total_messages", messageCount)
			return nil
		default:
			readCtx, cancel := context.WithTimeout(context.Background(), duration)
			_, message, err := wsClient.Read(readCtx)
			cancel()
			if err != nil {
				if strings.Contains(err.Error(), "deadline exceeded") || strings.Contains(err.Error(), "timeout") {
					continue
				}
				logger.Error("Error reading WebSocket message", "error", err, "message_count", messageCount)
				return err
			}
			messageCount++
			logger.Debug("Received WebSocket message", "message_count", messageCount, "message_length", len(message))
			select {
			case output <- message:
				logger.Debug("Message sent to output channel", "message_count", messageCount)
			case <-timeout:
				logger.Info("Timeout while sending message to output channel", "total_messages", messageCount)
				return nil
			}
		}
	}
}
