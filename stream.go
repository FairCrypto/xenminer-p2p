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

type XSyncRequest struct {
	Ack       bool
	BatchSize uint32
	FromId    uint64 // blockId > 0 or -1 for the next one
	ToId      uint64
}

type BlockRequest struct {
	Ack    bool
	SeqNo  uint32
	NextId int64 // blockId > 0 or -1 for the next one
}

func decodeRequests(ctx context.Context, rw *bufio.ReadWriter, id peer.ID, logger log0.EventLogger) error {
	db := ctx.Value("db").(*sql.DB)
	quit := make(chan struct{})
	nextId := int64(0)

	var xSyncRequest XSyncRequest
	var blockRequest BlockRequest

	go func() {
		logger.Info("Processing requests")
		localHeight := getCurrentHeight(db)

		str, err := rw.ReadString('\n')
		if err != nil {
			logger.Warn("read err: ", err)
			quit <- struct{}{}
			return
		}
		// count += n
		err = json.Unmarshal([]byte(str), &xSyncRequest)
		if err != nil {
			logger.Warn("unmarshal err: ", err)
			quit <- struct{}{}
			return
		}
		logger.Infof("RCVD %s", xSyncRequest)
		if xSyncRequest.Ack {
			logger.Warn("illegal state: ack=true")
			quit <- struct{}{}
			return
		}
		if xSyncRequest.FromId > uint64(localHeight) {
			logger.Warn("illegal ask: > max_height")
			quit <- struct{}{}
			return
		}
		if xSyncRequest.ToId > uint64(localHeight) {
			xSyncRequest.ToId = uint64(localHeight)
		}
		if xSyncRequest.BatchSize > blockBatchSize {
			xSyncRequest.BatchSize = uint32(blockBatchSize)
		}
		xSyncRequest.Ack = true
		xSyncBytes, err := json.Marshal(&xSyncRequest)
		if err != nil {
			logger.Warn("Err in marshal ", err)
			quit <- struct{}{}
			return
		}
		_, err = rw.WriteString(fmt.Sprintf("%s\n", string(xSyncBytes)))
		if err != nil {
			logger.Warn("Err in write ", err)
			quit <- struct{}{}
			return
		}
		_ = rw.Flush()
		logger.Infof("ACKD %s", xSyncRequest)
		confirmedReqStr, err := rw.ReadString('\n')
		if err != nil {
			logger.Warn("read err: ", err)
			quit <- struct{}{}
			return
		}
		err = json.Unmarshal([]byte(confirmedReqStr), &xSyncRequest)
		if err != nil {
			logger.Warn("Err in unmarshal: ", err)
			quit <- struct{}{}
			return
		}
		logger.Infof("NEGD %s", xSyncRequest)

		acked := true
		for {
			str, err := rw.ReadString('\n')
			if err != nil {
				logger.Warn("read err: ", err)
				quit <- struct{}{}
				return
			}
			// count += n
			err = json.Unmarshal([]byte(str), &blockRequest)
			if err != nil {
				logger.Warn("Error converting data message: ", err, str)
				quit <- struct{}{}
				return
			}
			if !blockRequest.Ack && acked {
				logger.Infof("ASKD: %d", blockRequest.NextId)
				if blockRequest.NextId == -1 {
					nextId += 1
				} else {
					nextId = blockRequest.NextId
				}
				blocks := make([]Block, xSyncRequest.BatchSize)
				firstId := nextId
				for i := 0; i < int(xSyncRequest.BatchSize); i++ {
					block, err := getBlock(db, uint(nextId))
					if err != nil {
						logger.Warn("Error getting block: ", err)
						quit <- struct{}{}
						return
					}
					blocks = append(blocks, *block)
					nextId += 1
				}
				bytes, err := json.Marshal(&blocks)
				n, err := rw.WriteString(fmt.Sprintf("%s\n", string(bytes)))
				_ = rw.Flush()
				if err != nil {
					logger.Warn("Error sending blocks: ", err)
					quit <- struct{}{}
					return
				} else {
					logger.Infof("%d...%d (%d bytes) > %s", firstId, nextId, n, id)
					acked = false
				}
			} else if blockRequest.Ack && !acked {
				acked = true
			} else {
				logger.Warn("Illegal state")
				quit <- struct{}{}
				return
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

	streamHandler := func(s network.Stream) {
		logger.Infof("Received new stream from: %s", s.Conn().RemotePeer())
		rw := bufio.NewReadWriter(bufio.NewReader(s), bufio.NewWriter(s))
		logger.Debug("Reading stream")
		// err := decode(rw, logger)
		err := decodeRequests(ctx, rw, s.Conn().RemotePeer(), logger)
		logger.Info("Stream: ", err)
	}

	h.SetStreamHandler(blockSyncProto, streamHandler)
	logger.Info("Listening to incoming requests")

	<-ctx.Done()
}
