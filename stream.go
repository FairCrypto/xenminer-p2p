package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	log0 "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
)

type BlockRequest struct {
	NextId int64 // blockId > 0 or -1 for the next one
}

func decodeRequests(ctx context.Context, rw *bufio.ReadWriter, logger log0.EventLogger) error {
	db := ctx.Value("db").(*sql.DB)
	quit := make(chan struct{})
	nextId := int64(0)

	go func() {
		for {
			str, err := rw.ReadString('\n')
			if err != nil {
				logger.Warn("read err: ", err)
				quit <- struct{}{}
				return
			} else {
				// count += n
				var blockRequest BlockRequest
				err = json.Unmarshal([]byte(str), &blockRequest)
				logger.Debug("REQ: ", blockRequest.NextId)
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
					n, err := rw.WriteString(string(bytes))
					if err != nil {
						logger.Warn("Error sending block: ", err)
					} else {
						logger.Infof("SND %d, %d bytes", nextId, n)
					}
				}
			}
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
		logger.Info("listener received new stream", s.Stat())
		rw := bufio.NewReadWriter(bufio.NewReader(s), bufio.NewWriter(s))
		logger.Debug("Reading stream")
		// err := decode(rw, logger)
		err := decodeRequests(ctx, rw, logger)
		logger.Info("Stream err: ", err)
	})
	logger.Info("Listening")

	<-ctx.Done()
}
