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
)

type XSyncMessageType int

const (
	setupReq XSyncMessageType = iota
	setupAck
	setupCnf
	blocksReq
	blocksResp
)

type XSyncMessage struct {
	SeqNo  uint32
	Type   XSyncMessageType
	Count  uint32
	FromId uint64
	ToId   uint64
	Blocks []Block
}

func decodeRequests(ctx context.Context, rw *bufio.ReadWriter, id peer.ID, logger log0.EventLogger) error {
	db := ctx.Value("db").(*sql.DB)
	quit := make(chan string)
	nextId := int64(0)
	localHeight := getCurrentHeight(db)
	xSyncChan := make(chan XSyncMessage)

	processReadError := func(err error) {
		logger.Warn("read err: ", err)
		quit <- "read err"
	}
	processWriteError := func(err error) {
		logger.Warn("write err: ", err)
		quit <- "write err"
	}
	processMarshalError := func(err error) {
		logger.Warn("marshal err: ", err)
		quit <- "marshal err"
	}
	processUnmarshalError := func(err error) {
		logger.Warn("unmarshal err: ", err)
		quit <- "unmarshal err"
	}
	processProtocolError := func(aux string) {
		logger.Warnf("protocol err: %s", aux)
		quit <- "unmarshal err"
	}

	go func() {
		var xSyncRequest XSyncMessage
		logger.Info("Processing requests")
		for {
			str, err := rw.ReadString('\n')
			if err != nil {
				processReadError(err)
				return
			}
			if len(str) == 1 {
				continue
			}
			err = json.Unmarshal([]byte(str), &xSyncRequest)
			if err != nil {
				processUnmarshalError(err)
				return
			}
			xSyncChan <- xSyncRequest
			// runtime.Gosched()
		}
	}()

	for {
		select {
		case msg := <-xSyncChan:
			switch msg.Type {
			case setupReq:
				logger.Infof("RCVD %s", msg)
				if msg.FromId > uint64(localHeight) {
					processProtocolError("illegal ask: > max_height")
				}
				if msg.ToId > uint64(localHeight) {
					msg.ToId = uint64(localHeight)
				}
				if msg.Count > blockBatchSize {
					msg.Count = uint32(blockBatchSize)
				}
				msg.Type = setupAck
				bytes, err := json.Marshal(&msg)
				if err != nil {
					processMarshalError(err)
				}
				_, err = rw.WriteString(fmt.Sprintf("%s\n", string(bytes)))
				if err != nil {
					processWriteError(err)
				}
				_ = rw.Flush()
				logger.Infof("ACKD %s", msg)

			case setupCnf:
				nextId = int64(msg.FromId) - 1
				logger.Infof("NEGD %s", msg)

			case blocksReq:
				if msg.FromId == 0 {
					nextId += 1
				} else {
					nextId = int64(msg.FromId)
				}
				logger.Infof("ASKD: %d -> %d, count=%d", msg.FromId, nextId, msg.Count)
				msg.Type = blocksResp
				firstId := nextId
				for i := 0; i < int(msg.Count); i++ {
					block, err := getBlock(db, uint(nextId))
					if err != nil {
						logger.Warn("Error getting block: ", err)
						quit <- "error getting block"
					}
					logger.Infof("Packed block %d %d", nextId, block.Id)
					msg.Blocks = append(msg.Blocks, *block)
					nextId += 1
				}
				bytes, err := json.Marshal(&msg)
				n, err := rw.WriteString(fmt.Sprintf("%s\n", string(bytes)))
				_ = rw.Flush()
				if err != nil {
					processWriteError(err)
				} else {
					logger.Infof("%d...%d (%d bytes) > %s", firstId, nextId, n, id)
				}
			}

		case <-quit:
			return errors.New("stopped")
		}

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
