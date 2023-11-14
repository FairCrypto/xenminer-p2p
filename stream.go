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

type XSyncMessageType int

const (
	setupReq XSyncMessageType = iota
	setupAck
	setupCnf
	blocksReq
	blocksResp
)

type XSyncMessage struct {
	SeqNo  int32
	Type   XSyncMessageType
	Count  uint32
	FromId uint64
	ToId   uint64
	Blocks []Block
}

func decodeRequests(ctx context.Context, rw *bufio.ReadWriter, id peer.ID, logger log0.EventLogger) error {
	db := ctx.Value("db").(*sql.DB)

	quitReading := make(chan struct{}, 1)
	quit := make(chan string, 1)
	quitWithError := make(chan error, 1)
	xSyncChan := make(chan XSyncMessage, 1)

	nextId := int64(0)
	localHeight := getCurrentHeight(db)

	processReadError := func(err error) {
		logger.Warn("read err: ", err)
		quitWithError <- err
	}
	processWriteError := func(err error) {
		logger.Warn("write err: ", err)
		quitWithError <- err
	}
	processMarshalError := func(err error) {
		logger.Warn("marshal err: ", err)
		quitWithError <- err
	}
	processUnmarshalError := func(err error) {
		logger.Warn("unmarshal err: ", err)
		quitWithError <- err
	}
	processProtocolError := func(aux string) {
		logger.Warnf("protocol err: %s", aux)
		quitWithError <- errors.New(fmt.Sprintf("proto err: %s", aux))
	}
	defer func() {
		close(quitReading)
		close(quit)
		close(quitWithError)
	}()

	go func() {
		var xSyncRequest XSyncMessage
		logger.Info("Processing requests")
		for {
			select {
			case <-quitReading:
				logger.Info("Done processing requests")
				return

			default:
				str, err := rw.ReadString('\n')
				if err != nil {
					processReadError(err)
					quitReading <- struct{}{}
					quit <- "read error"
				}
				if len(str) == 1 {
					continue
				}
				err = json.Unmarshal([]byte(str), &xSyncRequest)
				if err != nil {
					processUnmarshalError(err)
					quit <- "read error"
				}
				xSyncChan <- xSyncRequest
			}
			runtime.Gosched()
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
				nextId = int64(msg.FromId)
				logger.Infof("NEGD %s", msg)

			case blocksReq:
				if msg.FromId == 0 {
					// nextId += 1
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
					logger.Debugf("Packed block %d %d", nextId, block.Id)
					msg.Blocks = append(msg.Blocks, *block)
					nextId += 1
				}
				logger.Infof("MSG %d %d", msg.Type, len(msg.Blocks))
				bytes, err := json.Marshal(&msg)
				n, err := rw.WriteString(fmt.Sprintf("%s\n", string(bytes)))
				if err != nil {
					processWriteError(err)
				}
				_ = rw.Flush()
				logger.Infof("%d...%d (%d bytes) > %s (seq=%d)", firstId, nextId, n, id, msg.SeqNo)
			}

		case aux := <-quit:
			quitReading <- struct{}{}
			logger.Info("quit")
			return errors.New("stopped: " + aux)

		case err := <-quitWithError:
			quitReading <- struct{}{}
			logger.Info("quit with error")
			return err
		}
		runtime.Gosched()
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
