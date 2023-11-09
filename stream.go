package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	log0 "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"runtime"
)

type BlockRequest struct {
	Ack    bool
	NextId int64 // blockId > 0 or -1 for the next one
}

func decodeRequests(ctx context.Context, rw *bufio.ReadWriter, id peer.ID, logger log0.EventLogger) error {
	db := ctx.Value("db").(*sql.DB)
	quit := make(chan struct{})
	nextId := int64(0)

	var blockRequest BlockRequest

	go func() {
		logger.Info("Processing requests")
		acked := true
		for {
			str, err := rw.ReadString('\n')
			if err != nil {
				logger.Warn("read err: ", err)
				quit <- struct{}{}
				return
			} else {
				// count += n
				err = json.Unmarshal([]byte(str), &blockRequest)
				if !blockRequest.Ack && acked {
					logger.Infof("ASKD: %d", blockRequest.NextId)
					if err != nil {
						logger.Warn("Error converting data message: ", err)
					} else {
						if blockRequest.NextId == -1 {
							nextId += 1
						} else {
							nextId = blockRequest.NextId
						}
						block, err := getBlock(db, uint(nextId))
						if err != nil {
							logger.Warn("Error getting block: ", err)
							continue
						}
						bytes, err := json.Marshal(block)
						n, err := rw.WriteString(fmt.Sprintf("%s\n", string(bytes)))
						_ = rw.Flush()
						if err != nil {
							logger.Warn("Error sending block: ", err)
						} else {
							logger.Infof("%d (%d bytes) > %s", nextId, n, id)
							acked = false
						}
					}
				} else if blockRequest.Ack && !acked {
					acked = true
				} else {
					logger.Warn("Illegal state")
				}
			}
			runtime.Gosched()
		}
	}()

	select {
	case <-quit:
		return errors.New("stopped")
	}

}

func streamBlocks(ctx context.Context) {
	h := ctx.Value("host").(host.Host)
	logger := ctx.Value("logger").(log0.EventLogger)

	h.SetStreamHandler("/xen/blocks/sync/0.1.0", func(s network.Stream) {
		logger.Infof("Received new stream from: %s", s.Conn().RemotePeer())
		rw := bufio.NewReadWriter(bufio.NewReader(s), bufio.NewWriter(s))
		logger.Debug("Reading stream")
		// err := decode(rw, logger)
		err := decodeRequests(ctx, rw, s.Conn().RemotePeer(), logger)
		logger.Info("Stream: ", err)
	})
	logger.Info("Listening to incoming requests")

	<-ctx.Done()
}
