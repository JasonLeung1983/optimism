package dsl

import (
	"context"
	"strings"
	"time"

	opclient "github.com/ethereum-optimism/optimism/op-service/client"

	"github.com/ethereum-optimism/optimism/op-devstack/stack"
	"github.com/ethereum/go-ethereum/log"
)

type OPRBuilderNodeSet []*OPRBuilderNode

func NewOPRBuilderNodeSet(inner []stack.OPRBuilderNode, control stack.ControlPlane) OPRBuilderNodeSet {
	oprbuilders := make([]*OPRBuilderNode, len(inner))
	for i, c := range inner {
		oprbuilders[i] = NewOPRBuilderNode(c, control)
	}
	return oprbuilders
}

type OPRBuilderNode struct {
	commonImpl
	inner    stack.OPRBuilderNode
	wsClient *opclient.WSClient
	control  stack.ControlPlane
}

func NewOPRBuilderNode(inner stack.OPRBuilderNode, control stack.ControlPlane) *OPRBuilderNode {
	return &OPRBuilderNode{
		commonImpl: commonFromT(inner.T()),
		inner:      inner,
		wsClient:   inner.FlashblocksClient(),
		control:    control,
	}
}

func (c *OPRBuilderNode) String() string {
	return c.inner.ID().String()
}

func (c *OPRBuilderNode) Escape() stack.OPRBuilderNode {
	return c.inner
}

func (c *OPRBuilderNode) ListenFor(logger log.Logger, duration time.Duration, output chan<- []byte, done chan<- struct{}) error {
	return listenForWS(logger, c.wsClient, duration, output, done)
}

func (el *OPRBuilderNode) Stop() {
	el.log.Info("Stopping", "id", el.inner.ID())
	el.control.OPRBuilderNodeState(el.inner.ID(), stack.Stop)
}

func (el *OPRBuilderNode) Start() {
	el.control.OPRBuilderNodeState(el.inner.ID(), stack.Start)
}

// listenForWS reads from the given websocket client for the specified duration,
// and forwards any received messages to the output channel.
func listenForWS(logger log.Logger, wsClient *opclient.WSClient, duration time.Duration, output chan<- []byte, done chan<- struct{}) error {
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
				// Per-read timeout is expected; continue until overall duration elapses.
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
