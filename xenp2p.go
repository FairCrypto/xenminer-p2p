package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	log0 "github.com/ipfs/go-log/v2"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/discovery"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	drouting "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	_ "github.com/mattn/go-sqlite3"
	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/samber/lo"
	"go.uber.org/zap/zapcore"
	"log"
	"math"
	"math/rand"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

type PeerId struct {
	Id      string `json:"id"`
	PrivKey string `json:"privKey"`
	PubKey  string `json:"pubKey"`
}

type Block struct {
	Id          uint   `json:"id"`
	Timestamp   string `json:"timestamp"`
	PrevHash    string `json:"prev_hash"`
	MerkleRoot  string `json:"merkle_root"`
	RecordsJson string `json:"records_json"`
	BlockHash   string `json:"block_hash"`
}

type HashRecord struct {
	Id           uint   `json:"id"`
	CreatedAt    string `json:"created_at"`
	Key          string `json:"key"`
	HashToVerify string `json:"hash_to_verify"`
	Account      string `json:"account"`
}

type RangeRecord struct {
	Id          uint   `json:"id"`
	Node        string `json:"node"`
	BlocksRange string `json:"blocks_range"`
	Hash        string `json:"hash"`
	Difficulty  uint   `json:"difficulty"`
	Ts          int64  `json:"ts"`
}

func (r RangeRecord) String() string {
	node := r.Node
	if len(r.Node) >= 8 {
		node = r.Node[len(r.Node)-8:]
	}
	return fmt.Sprintf("%s|%s|%s|%d", r.BlocksRange, node, r.Hash, r.Difficulty)
}

type Record struct {
	Id           uint    `json:"id"`
	Account      string  `json:"account"`
	BlockId      *uint64 `json:"block_id"`
	XuniId       *uint64 `json:"xuni_id"`
	Date         string  `json:"date"`
	HashToVerify string  `json:"hash_to_verify"`
	Key          string  `json:"key"`
}

type RawDataReq struct {
	IsXuni bool   `json:"is_xuni"`
	Ids    []uint `json:"ids"`
}

type Height struct {
	Max sql.NullInt32 `json:"max_height"`
}

type Blocks []Block

type Topics struct {
	shift       *pubsub.Topic
	newHash     *pubsub.Topic
	newXuni     *pubsub.Topic
	blockHeight *pubsub.Topic
	data        *pubsub.Topic
	get         *pubsub.Topic
	getRaw      *pubsub.Topic
	control     *pubsub.Topic
}

type Subs struct {
	shift       *pubsub.Subscription
	newHash     *pubsub.Subscription
	newXuni     *pubsub.Subscription
	blockHeight *pubsub.Subscription
	data        *pubsub.Subscription
	get         *pubsub.Subscription
	getRaw      *pubsub.Subscription
	control     *pubsub.Subscription
}

type NetworkState struct {
	ShiftNumber uint64  `json:"shiftNumber"`
	Difficulty  float32 `json:"difficulty"`
	BlockHeight uint64  `json:"blockHeight"`
	LastHashId  uint64  `json:"lastHashId"`
	LastXuniId  uint64  `json:"lastXuniId"`
}

const masterPeerId = "12D3KooWLGpxvuNUmMLrQNKTqvxXbXkR1GceyRSpQXd8ZGmprvjH"
const rendezvousString = "/xenblocks/0.1.0"
const blockSyncProto = "/xen/blocks/sync/0.1.2"
const maxDeltaLen = 20
const blockBatchSize = 10

// const yieldTime = 100 * time.Millisecond

var maxBlockHeight uint = 0

// var wantedBlockIds = map[uint]bool{}
var wantedBlockIds = cmap.New[bool]()

func processBlockHeight(ctx context.Context) {
	subs := ctx.Value("subs").(Subs)
	// topics := ctx.Value("topics").(Topics)
	h := ctx.Value("host").(host.Host)
	db := ctx.Value("db").(*sql.DB)
	peerId := ctx.Value("peerId").(string)
	logger := ctx.Value("logger").(log0.EventLogger)
	state := ctx.Value("state").(*NetworkState)

	var quitReceiving chan struct{}
	var quit chan struct{}
	var xSyncChan chan XSyncMessage

	var xSyncRequest XSyncMessage
	var negotiatedBatchCount = uint32(blockBatchSize)
	var receiving = false

	for {
		msg, err := subs.blockHeight.Next(ctx)
		if msg.ReceivedFrom.String() == peerId {
			continue
		}
		logger.Debugf("From %s via %s", msg.GetFrom().String(), msg.ReceivedFrom.String())

		if err != nil {
			logger.Warn("Error getting message: ", err)
		}
		var blockchainHeight uint
		err = json.Unmarshal(msg.Data, &blockchainHeight)
		if err != nil {
			logger.Warn("Error decoding message: ", err)
		}

		localHeight := getCurrentHeight(db)
		if maxBlockHeight == 0 && blockchainHeight >= maxBlockHeight && blockchainHeight >= localHeight {
			maxBlockHeight = blockchainHeight
			logger.Info("MAX HEIGHT: ", maxBlockHeight)
		}
		if maxBlockHeight > localHeight && peerId != masterPeerId && !receiving {
			logger.Info("DIFF: ", localHeight, "<", maxBlockHeight)
			delta := uint(math.Min(float64(maxBlockHeight-localHeight), maxDeltaLen))
			want := make([]uint, delta)
			for i := uint(0); i < delta; i++ {
				want[i] = localHeight + i + 1
				wantedBlockIds.Set(fmt.Sprintf("%d", localHeight+i+1), true)
			}
			logger.Infof("WANT: %d", delta)

			quitReceiving = make(chan struct{})
			quit = make(chan struct{})
			xSyncChan = make(chan XSyncMessage, 1)

			conn, err := h.NewStream(ctx, msg.GetFrom(), blockSyncProto)
			if err != nil {
				logger.Warn("Err in conn ", err)
				continue
			}
			rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
			logger.Infof("Connection to %s", msg.GetFrom().String(), conn.Stat())

			xSyncRequest = XSyncMessage{
				Type:   setupReq,
				SeqNo:  0,
				Count:  blockBatchSize,
				FromId: uint64(localHeight) + 1,
				ToId:   uint64(maxBlockHeight),
				Blocks: make([]Block, 0),
			}

			xSyncBytes, err := json.Marshal(&xSyncRequest)
			if err != nil {
				logger.Warn("Err in marshal ", err)
				continue
			}
			_, err = rw.WriteString(fmt.Sprintf("%s\n", string(xSyncBytes)))
			if err != nil {
				logger.Warn("Err in write ", err)
				continue
			}
			_ = rw.Flush()
			logger.Infof("REQD %s", xSyncRequest)

			receiving = true
			closing := false
			doReceive := func(quitReceiving chan struct{}) {
				defer func() {
					logger.Info("Stopping the receiver")
					err = conn.Close()
					logger.Info("Receiver stopped: ", err)
					if !closing {
						closing = true
						close(quitReceiving)
					}
					receiving = false
				}()
				for {
					select {
					case <-quitReceiving:
						return

					default:
						msgStr, err := rw.ReadString('\n')
						if err != nil && !closing {
							logger.Warn("read err: ", err)
							quitReceiving <- struct{}{}
						}
						if len(msgStr) != 1 && !closing {
							err = json.Unmarshal([]byte(msgStr), &xSyncRequest)
							if err != nil {
								logger.Warn("Err in unmarshall: ", err)
								quitReceiving <- struct{}{}
								break
							}
							if xSyncRequest.Count < 1 {
								logger.Warnf("XSync params don't fit: count=%d (%s)", xSyncRequest.Count)
								// break
							}
							if xSyncRequest.FromId != uint64(localHeight)+1 {
								logger.Warnf("XSync params don't fit: from=%d <> local=%d (%s)", xSyncRequest.FromId, localHeight+1)
								// break
							}
							if xSyncRequest.ToId <= uint64(localHeight) {
								logger.Warnf("XSync params don't fit: to=%d > local=%d (%s)", xSyncRequest.ToId, localHeight)
								// break
							}
							xSyncChan <- xSyncRequest
						}
					}
					runtime.Gosched()
				}
			}

			doSend := func(quit chan struct{}) {
				defer func() {
					err = conn.Close()
					logger.Warn("Quitting the proto: ", err)
				}()
				for {
					select {
					case <-quit:
						close(quitReceiving)
						closing = true
						return

					case xMsg := <-xSyncChan:
						logger.Infof("inc msg %s", xMsg.Type)
						switch xMsg.Type {
						case setupAck:
							logger.Infof("ACKD %s", xMsg)
							negotiatedBatchCount = xMsg.Count
							xMsg.Type = setupCnf
							bytes, err := json.Marshal(&xMsg)
							_, err = rw.WriteString(fmt.Sprintf("%s\n", string(bytes)))
							if err != nil {
								logger.Warn("Err in write ", err)
								quit <- struct{}{}
								break
							}
							_ = rw.Flush()

							delta = uint(xMsg.ToId - xMsg.FromId)
							// totalBatches := uint32(math.Ceil(float64(delta / uint(negotiatedBatchCount))))
							totalBatches := int32(2)
							logger.Infof("Negotiated count=%d batches=%d", negotiatedBatchCount, totalBatches)

							for batchNo := int32(0); batchNo < totalBatches; batchNo++ {
								xSyncRequest = XSyncMessage{
									Count:  negotiatedBatchCount,
									FromId: uint64(0),
									ToId:   uint64(0),
									SeqNo:  batchNo,
									Type:   blocksReq,
									Blocks: make([]Block, 0),
								}
								if batchNo+1 == totalBatches {
									xSyncRequest.SeqNo = -1
								}
								bytes, err := json.Marshal(&xSyncRequest)
								if err != nil {
									logger.Warn("Err in marshall ", err)
									quit <- struct{}{}
									break
								}
								_, err = rw.WriteString(fmt.Sprintf("%s\n", string(bytes)))
								if err != nil {
									logger.Warn("Err in write ", err)
									quit <- struct{}{}
									break
								}
								_ = rw.Flush()
								logger.Infof("BREQ %d", xSyncRequest)
							}

						case blocksResp:
							logger.Infof("Received %d blocks (seq=%d)", len(xMsg.Blocks), xMsg.SeqNo)
							for _, block := range xMsg.Blocks {
								prevBlock, err := getPrevBlock(db, &block)
								if err != nil {
									logger.Warnf("Error getting prev block $d: ", block.Id, err)
									quit <- struct{}{}
									break
								}
								if prevBlock.BlockHash != block.PrevHash {
									logger.Error("Error block hash mismatch on ids: ", prevBlock.BlockHash, block.PrevHash)
									quit <- struct{}{}
									break
								}
								blockIsValid, err := validateBlock(block, logger)
								if peerId != masterPeerId && blockIsValid {
									err = insertBlock(db, &block)
									if err != nil {
										logger.Warnf("Error adding block %d to DB: %s", block.Id, err)
										quit <- struct{}{}
										break
									} else {
										wantedBlockIds.Remove(fmt.Sprintf("%d", block.Id))
										logger.Infof("%d < %s", block.Id, msg.GetFrom())
									}
								}
							}
							if xMsg.SeqNo == -1 {
								// closing = true
								logger.Info("Complete")
								close(quit)
								// _ = conn.Close()
								// close(quitReceiving)
								// time.Sleep(time.Second)
								// close(quit)
							}
						}
					}
					runtime.Gosched()
				}
			}

			go doReceive(quitReceiving)
			go doSend(quit)
		}

		if maxBlockHeight == localHeight {
			logger.Debug("IN SYNC: ", localHeight, "=", maxBlockHeight)
		}
		state.BlockHeight = uint64(maxBlockHeight)
		runtime.Gosched()
	}
}

func processGet(ctx context.Context) {
	subs := ctx.Value("subs").(Subs)
	// topics := ctx.Value("topics").(Topics)
	db := ctx.Value("db").(*sql.DB)
	h := ctx.Value("host").(host.Host)
	peerId := ctx.Value("peerId").(string)
	logger := ctx.Value("logger").(log0.EventLogger)

	for {
		msg, err := subs.get.Next(ctx)
		if msg.ReceivedFrom.String() == peerId {
			continue
		}
		if err != nil {
			logger.Warn("Error getting want message: ", err)
		}
		var blockIds []uint
		err = json.Unmarshal(msg.Data, &blockIds)
		if err != nil {
			logger.Warn("Error converting want message: ", err)
		}
		logger.Debug("WANT block_id(s):", len(blockIds))
		var blocks Blocks

		conn, err := h.NewStream(ctx, msg.GetFrom(), "/xen/blocks/sync/0.1.0")
		if err != nil {
			logger.Warn("Err in conn ", err)
			continue
		}

		rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
		logger.Infof("Connection to %s", msg.GetFrom().String(), conn.Stat())

		for _, blockId := range blockIds {
			block, err := getBlock(db, blockId)
			// NB: ignoring the error which might result from missing blocks
			if err == nil {
				blocks = append(blocks, *block)
			}
		}
		logger.Info("SEND block(s):", len(blocks))
		t := time.Now().UnixMilli()
		bytes, err := json.Marshal(&blocks)
		// n, err := rw.WriteString(fmt.Sprintf("%s\n", string(bytes)))
		n, err := rw.Write(append(bytes, '\n'))
		if err != nil {
			logger.Warn("Error sending stream: ", err)
		} else {
			logger.Infof("Wrote: %d b in %d ms", n, time.Now().UnixMilli()-t)
			time.Sleep(2 * time.Second)
			err = rw.Flush()
			err = conn.Close()
			if err != nil {
				logger.Warn("Error closing stream: ", err)
			}
		}

		/*
			for _, blockId := range blockIds {
				block, err := getBlock(db, blockId)
				// NB: ignoring the error which might result from missing blocks
				if err == nil {
					blocks = append(blocks, *block)
				}
			}
			logger.Debug("SEND block(s):", len(blocks))
			bytes, err := json.Marshal(&blocks)
			err = topics.data.Publish(ctx, bytes)
			if err != nil {
				logger.Warn("Error publishing data message: ", err)
			}

		*/
		runtime.Gosched()
	}
}

func processGetRaw(ctx context.Context) {
	subs := ctx.Value("subs").(Subs)
	topics := ctx.Value("topics").(Topics)
	db := ctx.Value("db").(*sql.DB)
	peerId := ctx.Value("peerId").(string)
	logger := ctx.Value("logger").(log0.EventLogger)

	for {
		msg, err := subs.getRaw.Next(ctx)
		if msg.ReceivedFrom.String() == peerId {
			continue
		}
		if err != nil {
			logger.Warn("Error getting get raw message: ", err)
		}
		var req RawDataReq
		err = json.Unmarshal(msg.Data, &req)
		if err != nil {
			logger.Warn("Error converting get raw message: ", err)
		}
		logger.Debug("WANT raw ids (s): ", req.Ids, " (xuni=", req.IsXuni, ")")
		for _, recordId := range req.Ids {
			var getRecord = getHash
			if req.IsXuni {
				getRecord = getXuni
			}
			record, err := getRecord(db, recordId)
			// NB: ignoring the error which might result from missing blocks
			if err == nil {
				bytes, err := json.Marshal(*record)
				if err != nil {
					logger.Warn("Error converting hash record to data: ", err)
				}
				if req.IsXuni {
					err = topics.newXuni.Publish(ctx, bytes)
				} else {
					err = topics.newHash.Publish(ctx, bytes)
				}
				if err != nil {
					logger.Warn("Error publishing raw data record message: ", err)
				}
				logger.Debug("SENT REC", recordId, " (xuni=", req.IsXuni, ")")
			} else {
				// logger.Debug("!RECORD", recordId)
				err = nil
			}
		}
		runtime.Gosched()
	}
}

func validateBlock(block Block, logger log0.EventLogger) (bool, error) {
	recordsJson := block.RecordsJson
	var records []Record
	err := json.Unmarshal([]byte(recordsJson), &records)
	if err != nil {
		logger.Warn("Error converting records JSON: ", err)
	}

	toHash := func(record Record, index int) string {
		var id int64
		if record.XuniId != nil {
			id = int64(*record.XuniId)
		} else {
			id = int64(*record.BlockId)
		}
		// hash_value(str(block_id) + hash_to_verify + key + account))
		stringToHash := strconv.FormatInt(id, 10) + record.HashToVerify + record.Key + record.Account
		h := sha256.New()
		defer h.Reset()
		h.Write([]byte(stringToHash))
		bs := h.Sum(make([]byte, 0, h.Size()))
		return hex.EncodeToString(bs)
	}
	hashes := lo.Map(records, toHash)
	merkleRoot, _ := buildMerkleTree(hashes, map[string]MerkleNode{})
	return merkleRoot == block.MerkleRoot, err
}

func shouldRespond() bool {
	dice := rand.Intn(100)
	if dice < 5 {
		return true
	}
	return false
}

func processData(ctx context.Context) {
	subs := ctx.Value("subs").(Subs)
	db := ctx.Value("db").(*sql.DB)
	peerId := ctx.Value("peerId").(string)
	logger := ctx.Value("logger").(log0.EventLogger)

	for {
		msg, err := subs.data.Next(ctx)

		if msg.ReceivedFrom.String() == peerId || !shouldRespond() {
			continue
		}
		if err != nil {
			logger.Warn("Error getting data message: ", err)
		}
		var blocks Blocks
		err = json.Unmarshal(msg.Data, &blocks)
		logger.Debug("RECV: ", len(blocks))
		if err != nil {
			logger.Warn("Error converting data message: ", err)
		}
		for _, block := range blocks {
			if msg.ReceivedFrom.String() == peerId {
				logger.Debug("DATA block_id:", block.Id, "merkle_root:", block.MerkleRoot[0:6])
			}
			if !wantedBlockIds.Has(fmt.Sprintf("%d", block.Id)) {
				continue
			}
			if block.Id > 1 {
				prevBlock, err := getPrevBlock(db, &block)
				if err != nil {
					// logger.Warn("Error when processing row: ", err)
					continue
				}
				if prevBlock.BlockHash != block.PrevHash {
					logger.Error("Error block hash mismatch on ids: ", prevBlock.BlockHash, block.PrevHash)
					continue
				}
			}
			blockIsValid, err := validateBlock(block, logger)
			if peerId != masterPeerId && blockIsValid {
				err = insertBlock(db, &block)
				if err != nil {
					logger.Warnf("Error adding block %d to DB: %s", block.Id, err)
				} else {
					wantedBlockIds.Remove(fmt.Sprintf("%d", block.Id))
				}
			}
		}
		runtime.Gosched()
	}
}

func processRange(ctx context.Context) {
	subs := ctx.Value("subs").(Subs)
	topics := ctx.Value("topics").(Topics)
	peerId := ctx.Value("peerId").(string)
	logger := ctx.Value("logger").(log0.EventLogger)
	controlDb := ctx.Value("controlDb").(*sql.DB)

	for {
		msg, err := subs.control.Next(ctx)
		if err != nil {
			logger.Warn("Error getting control message: ", err)
			continue
		}
		if msg.ReceivedFrom.String() == peerId {
			continue
		}
		var rangeRecord RangeRecord
		err = json.Unmarshal(msg.Data, &rangeRecord)
		if err != nil {
			logger.Warn("Error converting data message: ", err)
		}
		if msg.ReceivedFrom.String() != peerId {
			from := msg.ReceivedFrom.String()[len(msg.ReceivedFrom.String())-8:]
			if rangeRecord.Node != msg.ReceivedFrom.String() && rangeRecord.Node != "myself" {
				logger.Debugf("!! Tampered NodeId: expected %s, received %s", msg.ReceivedFrom.String(), rangeRecord.Node)
			}
			rangeRecord.Node = msg.ReceivedFrom.String()
			err = insertRangeRecord(controlDb, rangeRecord)
			if err != nil {
				logger.Debug("Error inserting range: ", err)
			} else {
				logger.Infof("RANGE: %s < %s", rangeRecord.String(), from)
				rangeRecord.Node = peerId
				bytes, err := json.Marshal(rangeRecord)
				if err != nil {
					logger.Warn("Error converting range: ", err)
				}
				err = topics.control.Publish(ctx, bytes)
				if err != nil {
					logger.Warn("Error publishing range: ", err)
				}
				logger.Infof("RANGE: %s >", rangeRecord.String())
			}
		}
		runtime.Gosched()
	}
}

func hasPeer(peers peer.IDSlice, p string) bool {
	for i := 0; i < peers.Len(); i++ {
		if strings.HasSuffix(p, peers[i].String()) {
			return true
		}
	}
	return false
}

func hasDestination(destinations []string, p string) bool {
	for i := 0; i < len(destinations); i++ {
		if destinations[i] == p {
			return true
		}
	}
	return false
}

func checkConnections(ctx context.Context, destinations []string) {
	h := ctx.Value("host").(host.Host)
	logger := ctx.Value("logger").(log0.EventLogger)

	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	quit := make(chan struct{})

	for {
		select {
		case <-t.C:
			// check if peer is not connected and try to reconnect
			peers := h.Peerstore().Peers()
			for _, addr := range destinations {
				if !hasPeer(peers, addr) {
					logger.Infof("Reconnecting to %s", addr)
					connectToPeer(ctx, addr)
				}
			}

		case <-quit:
			t.Stop()
			return
		}
	}
}

func discoverPeers(ctx context.Context, disc *drouting.RoutingDiscovery, destinations []string) {
	h := ctx.Value("host").(host.Host)
	dhTable := ctx.Value("dht").(*dht.IpfsDHT)
	logger := ctx.Value("logger").(log0.EventLogger)

	t := time.NewTicker(20 * time.Second)
	defer t.Stop()
	quit := make(chan struct{})
	// Now, look for others who have announced
	// This is like your friend telling you the location to meet you.

	for {
		select {
		case <-t.C:
			logger.Debug("RT: ", dhTable.RoutingTable().GetPeerInfos())
			var options []discovery.Option
			options = append(options, discovery.TTL(peerstore.PermanentAddrTTL))
			t, err := disc.Advertise(ctx, rendezvousString, options...)
			peerChan, err := disc.FindPeers(ctx, rendezvousString)
			logger.Debug("Searching for other peers ", t.String())
			if err != nil {
				logger.Warn(err)
			}

			for p := range peerChan {
				logger.Debug("Maybe peer: ", p)
				if p.ID == h.ID() ||
					hasDestination(destinations, p.ID.String()) ||
					hasPeer(h.Peerstore().Peers(), p.ID.String()) {
					continue
				}
				logger.Info("Found peer: ", p)
				h.Peerstore().AddAddrs(p.ID, p.Addrs, peerstore.PermanentAddrTTL)
				err = h.Connect(ctx, p)
				if err != nil {
					logger.Warn("Error connecting to peer: ", err)
				} else {
					logger.Info("Connected to: ", p)
				}
			}

		case <-quit:
			t.Stop()
			return
		}
	}

}

func broadcastBlockHeight(ctx context.Context) {
	topics := ctx.Value("topics").(Topics)
	db := ctx.Value("db").(*sql.DB)

	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	quit := make(chan struct{})

	for {
		select {
		case <-t.C:
			maxHeight := getCurrentHeight(db)
			bytes, err := json.Marshal(maxHeight)
			if err != nil {
				log.Fatal("Error converting block_height", err)
			}
			err = topics.blockHeight.Publish(ctx, bytes)
			if err != nil {
				log.Fatal("Error publishing message", err)
			}
		case <-quit:
			t.Stop()
			return
		}
	}
}

/*
NB: THis is running only on Supernode !!!
*/
func broadcastLastHash(ctx context.Context) {
	topics := ctx.Value("topics").(Topics)
	dbh := ctx.Value("dbh").(*sql.DB)
	logger := ctx.Value("logger").(log0.EventLogger)
	state := ctx.Value("state").(*NetworkState)

	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	quit := make(chan struct{})

	for {
		select {
		case <-t.C:
			lastHash := getLatestHash(dbh)
			// lastXuni := getLatestXuni(dbh)
			// logger.Info("Last ", lastHash.Id, lastXuni.Id)
			var hashOrXuni *HashRecord
			if uint64(lastHash.Id) > state.LastHashId {
				hashOrXuni = lastHash
				state.LastHashId = uint64(lastHash.Id)
				logger.Info("New Hash Id ", state.LastHashId)
			}
			// if lastXuni.Id > *lastXuniId {
			//	hashOrXuni = lastXuni
			//	*lastXuniId = lastXuni.Id
			//	logger.Info("New Xuni Id ", *lastXuniId)
			//}

			if hashOrXuni != nil {
				bytes, err := json.Marshal(*hashOrXuni)
				if err != nil {
					log.Fatal("Error converting hash/xuni", err)
				}
				err = topics.newHash.Publish(ctx, bytes)
				if err != nil {
					log.Fatal("Error publishing message", err)
				}
			}
		case <-quit:
			t.Stop()
			return
		}
	}
}

func broadcastLastRange(ctx context.Context) {
	topics := ctx.Value("topics").(Topics)
	controlDb := ctx.Value("controlDb").(*sql.DB)
	logger := ctx.Value("logger").(log0.EventLogger)

	// t := time.NewTicker(100 * time.Millisecond)
	t := time.NewTicker(3 * time.Second)
	defer t.Stop()
	quit := make(chan struct{})
	var lastRangeId uint = 0

	for {
		select {
		case <-t.C:
			lastRange := getLatestRange(controlDb)

			if lastRange != nil {
				bytes, err := json.Marshal(*lastRange)
				if err != nil {
					logger.Fatal("Error converting range", err)
				}
				if lastRange.Id > lastRangeId {
					err = topics.control.Publish(ctx, bytes)
					if err != nil {
						logger.Fatal("Error publishing message", err)
					} else {
						logger.Infof("RANGE: %s >", lastRange)
						lastRangeId = lastRange.Id
					}
				}
			}
		case <-quit:
			t.Stop()
			return
		}
	}
}

func processNewHash(ctx context.Context) {
	subs := ctx.Value("subs").(Subs)
	topics := ctx.Value("topics").(Topics)
	dbh := ctx.Value("dbh").(*sql.DB)
	peerId := ctx.Value("peerId").(string)
	logger := ctx.Value("logger").(log0.EventLogger)
	state := ctx.Value("state").(*NetworkState)

	const interval = 60
	// const longInterval = 600

	type HashMap map[uint]uint
	hashMap := HashMap{}
	// queue := make([]HashMap, 0)

	cHash := make(chan HashRecord, 1)
	cXuni := make(chan HashRecord, 1)
	cState := make(chan NetworkState, 1)

	go func() {
		for {
			msg, err := subs.newHash.Next(ctx)
			if msg.ReceivedFrom.String() == peerId {
				continue
			}
			var hash HashRecord
			err = json.Unmarshal(msg.Data, &hash)
			if err != nil {
				logger.Warn("Error decoding message: ", err)
			}
			cHash <- hash
			runtime.Gosched()
		}
	}()

	go func() {
		for {
			msg, err := subs.newXuni.Next(ctx)
			if msg.ReceivedFrom.String() == peerId {
				continue
			}
			var hash HashRecord
			err = json.Unmarshal(msg.Data, &hash)
			if err != nil {
				logger.Warn("Error decoding message: ", err)
			}
			cXuni <- hash
			runtime.Gosched()
		}
	}()

	go func() {
		for {
			msg, err := subs.shift.Next(ctx)
			if msg.ReceivedFrom.String() == peerId {
				continue
			}
			var state NetworkState
			err = json.Unmarshal(msg.Data, &state)
			if err != nil {
				logger.Warn("Error decoding message: ", err)
			}
			cState <- state
			runtime.Gosched()
		}
	}()

	var lastTs uint = 0
	for {
		select {
		case xuni := <-cXuni:
			// logger.Info("Discovered New Hash Id ", hash.Id)
			// validate hash and save it to blocks.db / xuni.db
			if peerId != masterPeerId {
				some, _ := getXuni(dbh, xuni.Id)
				if some == nil {
					err := insertXuniRecord(dbh, xuni)
					if err != nil {
						logger.Warn("Error inserting xuni to DBH: ", err)
					}
				}
			}

		case hash := <-cHash:
			// logger.Info("Discovered New Hash Id ", hash.Id)
			lastHash := getLatestHashId(dbh)
			// validate hash and save it to blocks.db / xuni.db
			if peerId != masterPeerId {
				logger.Debug("New RAW data record ID: ", hash.Id)
				some, _ := getHash(dbh, hash.Id)
				if some == nil {
					err := insertHashRecord(dbh, hash)
					if err != nil {
						logger.Warn("Error inserting hash to DBH: ", err)
					}
				}
			}

			if hash.Id > lastHash {
				state.LastHashId = uint64(hash.Id)
				countPre := len(hashMap)
				hashMap[hash.Id] = uint(time.Now().Unix())
				if len(hashMap) > countPre {
					if lastTs == 0 {
						lastTs = uint(time.Now().Unix())
					}
					if len(hashMap)%interval == 0 {
						difficulty := interval / float32(uint(time.Now().Unix())-lastTs)
						state.Difficulty = difficulty
						state.ShiftNumber++
						logger.Infof("Difficulty %f, shift %d ", difficulty, state.ShiftNumber)

						data, err := json.Marshal(*state)
						if err != nil {
							logger.Warn("Error encoding data message: ", err)
						}
						err = topics.shift.Publish(ctx, data)
						if err != nil {
							logger.Warn("Error publishing data message: ", err)
						}
						lastTs = 0
						hashMap = map[uint]uint{}
					}
				}
			}

		case gotState := <-cState:
			/*
				logger.Infof(
					"received shift %d (%d), diff %f (%f)",
					gotState.ShiftNumber,
					state.ShiftNumber,
					gotState.Difficulty,
					state.Difficulty,
				)
			*/
			if gotState.ShiftNumber > state.ShiftNumber {
				if state.ShiftNumber == 0 || gotState.ShiftNumber-state.ShiftNumber > 5 {
					lastTs = 0
					hashMap = map[uint]uint{}
					state.ShiftNumber = gotState.ShiftNumber
				}
			}
			if gotState.ShiftNumber == state.ShiftNumber &&
				math.Abs(float64(state.Difficulty-gotState.Difficulty)) > 0.001 {
				state.Difficulty = (state.Difficulty + gotState.Difficulty) / 2
				data, err := json.Marshal(*state)
				if err != nil {
					logger.Warn("Error encoding data message: ", err)
				}
				err = topics.shift.Publish(ctx, data)
			}
		}
	}
}

func requestMissingHashesAndXunis(ctx context.Context) {
	topics := ctx.Value("topics").(Topics)
	dbh := ctx.Value("dbh").(*sql.DB)
	logger := ctx.Value("logger").(log0.EventLogger)

	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	quit := make(chan struct{})

	for {
		select {
		case <-t.C:
			hashIds := getMissingHashIds(dbh)
			if len(hashIds) > 0 {
				bytes, err := json.Marshal(RawDataReq{
					IsXuni: false,
					Ids:    hashIds,
				})
				if err != nil {
					logger.Warn("Error converting hashIds: ", err)
				}
				err = topics.getRaw.Publish(ctx, bytes)
				if err != nil {
					logger.Warn("Error publishing message: ", err)
				}
				logger.Debug("WANT RAW: ", hashIds, " (xuni=false)")
			}
			xuniIds := getMissingXuniIds(dbh)
			if len(xuniIds) > 0 {
				bytes, err := json.Marshal(RawDataReq{
					IsXuni: true,
					Ids:    xuniIds,
				})
				if err != nil {
					logger.Warn("Error converting xuniIds: ", err)
				}
				err = topics.getRaw.Publish(ctx, bytes)
				if err != nil {
					logger.Warn("Error publishing message: ", err)
				}
				logger.Debug("WANT RAW: ", hashIds, " (xuni=true)")
			}
		case <-quit:
			t.Stop()
			return
		}
	}
}

/*
Starts a XenBlocks P2P Node
A Node can have one or many of the following roles:
- supernode (temporary, reserved)
- relay (temporary, reserved)
- miner
- validator
- rpc
*/
func main() {

	logger := log0.Logger("xen-blocks")

	roleSet := flag.String("roles", "", "defines node roles (coma-separated")
	init := flag.Bool("init", false, "init node and exit")
	reset := flag.Bool("reset", false, "reset all node's DBs")
	resetBlockchain := flag.Bool("reset-blockchain", false, "reset node's blockchain DB")
	resetHashes := flag.Bool("reset-hashes", false, "reset node's raw hashes DB")
	syncBlocksToHashes := flag.Bool("hashes", false, "sync raw hashes and exit")
	configPath := flag.String("config", ".node", "path to config file")
	readOnlyDB := flag.Bool("readonly", false, "open DB as read-only")
	client := flag.Bool("client", false, "start in client-only mode")
	logLevel := flag.String("log", "warn", "set log level")

	source := flag.String("test-source", "", "send data to sink")
	sink := flag.Bool("test-sink", false, "receive data from source")
	flag.Parse()

	isSupportedRole := func(item string, index int) bool {
		return slices.Contains(supportedRoles, item)
	}
	roles := lo.Filter[string](strings.Split(*roleSet, ","), isSupportedRole)
	node := Node{roles: roles}

	if *logLevel != "" {
		level, _ := zapcore.ParseLevel(*logLevel)
		log0.SetAllLoggers(log0.LogLevel(level))
		_ = log0.SetLogLevel("pubsub", "error")
		_ = log0.SetLogLevel("dht", "error")
	} else {
		log0.SetAllLoggers(log0.LevelWarn)
		_ = log0.SetLogLevel("pubsub", "error")
		_ = log0.SetLogLevel("dht", "error")
	}

	if *init {
		initNode(*configPath, logger)
		os.Exit(0)
	}
	if *syncBlocksToHashes {
		syncHashes(*configPath, logger)
		os.Exit(0)
	}
	if *resetBlockchain || *reset {
		resetBlockchainDb(*configPath, logger)
	}
	if *resetHashes || *reset {
		resetHashesDb(*configPath, logger)
	}

	// initialize Node dir, peerId, DBs and ENV
	initNode(*configPath, logger)

	logger.Info("Loading config from: ", *configPath)
	logger.Info("Starting Node: ", node)

	state := &NetworkState{
		ShiftNumber: 0,
		BlockHeight: 0,
		LastHashId:  0,
		LastXuniId:  0,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, "logger", logger)
	ctx = context.WithValue(ctx, "state", state)

	// setup DB and check / init table(s)
	db := setupDB(*configPath, *readOnlyDB, logger)
	ctx = context.WithValue(ctx, "db", db)
	defer func(db *sql.DB) {
		_ = db.Close()
	}(db)

	// setup hash/xuni DB and check / init table(s)
	hdb := setupHashesDB(*configPath, *readOnlyDB, logger, state)
	logger.Infof("Latest state: %d", state)
	ctx = context.WithValue(ctx, "dbh", hdb)
	defer func(hdb *sql.DB) {
		_ = hdb.Close()
	}(hdb)

	// setup control DB
	controlDb := setupControlDB(*configPath, logger)
	ctx = context.WithValue(ctx, "controlDb", controlDb)
	defer func(controlDb *sql.DB) {
		_ = controlDb.Close()
	}(controlDb)

	// setup redis client
	setupRedis(ctx)

	// load peer params from config file
	addr, privKey, peerId := loadPeerParams(*configPath, logger)
	ctx = context.WithValue(ctx, "peerId", peerId)
	if peerId == masterPeerId {
		logger.Info("Master Node")
	} else {
		if *client {
			logger.Info("Client Node")
		} else {
			logger.Info("Peer Node")
		}
	}

	// construct a libp2p Host.
	h := setupHost(privKey, addr)
	for _, addr := range h.Addrs() {
		fullAddr := fmt.Sprintf("%s/p2p/%s", addr, h.ID())
		logger.Infof("Server address: %s", fullAddr)
	}
	ctx = context.WithValue(ctx, "host", h)
	defer func(h host.Host) {
		_ = h.Close()
	}(h)

	// setup connections to bootstrap peers
	destinations := prepareBootstrapAddresses(*configPath, logger)
	peers := lo.Map(destinations, toAddrInfo)
	peerPtrs := lo.Map(destinations, toAddrInfoPtr)

	var disc *drouting.RoutingDiscovery
	if *client || *source != "" || *sink {
		setupConnections(ctx, destinations)
	}

	if *source != "" {
		id, err := peer.Decode(*source)
		if err != nil {
			log.Fatal("Error ", err)
		}
		doSend(ctx, id)
		select {}

	} else if *sink {
		doReceive(ctx, protocol.TestingID)
		// select {}

	} else {

		var dhtOptions []dht.Option
		if len(destinations) > 0 {
			dhtOptions = append(dhtOptions, dht.BootstrapPeers(peers...))
		}
		if *client {
			// dhtOptions = append(dhtOptions, dht.Mode(dht.ModeServer))
			dhtOptions = append(dhtOptions, dht.Mode(dht.ModeClient))
		} else {
			dhtOptions = append(dhtOptions, dht.Mode(dht.ModeServer))
		}

		kademliaDHT, err := dht.New(ctx, h, dhtOptions...)
		if err != nil {
			panic(err)
		}
		ctx = context.WithValue(ctx, "dht", kademliaDHT)
		defer func(kademliaDHT *dht.IpfsDHT) {
			_ = kademliaDHT.Close()
		}(kademliaDHT)

		disc = setupDiscovery(ctx, destinations)
		// Bootstrap the DHT. In the default configuration, this spawns a Background
		// thread that will refresh the peer table every five minutes.
		logger.Info("Bootstrapping the DHT")
		if err = kademliaDHT.Bootstrap(ctx); err != nil {
			panic(err)
		}

		// setup pubsub protocol (either floodsub or gossip)
		var pubsubOptions []pubsub.Option
		pubsubOptions = append(pubsubOptions, pubsub.WithDirectPeers(peers))
		if !*client {
			pubsubOptions = append(pubsubOptions, pubsub.WithDiscovery(disc))
		}
		// ps, err := pubsub.NewFloodSub(ctx, h, pubsubOptions...)
		ps, err := pubsub.NewGossipSub(ctx, h, pubsubOptions...)
		ctx = context.WithValue(ctx, "pubsub", ps)
		if err != nil {
			logger.Error("Error starting pubsub protocol", err)
			panic(err)
		}

		logger.Info("Started Node: ", peerId)

		// subscribe to essential topics
		topics, subs := subscribeToTopics(ps, logger)
		ctx = context.WithValue(ctx, "topics", topics)
		ctx = context.WithValue(ctx, "subs", subs)

		if node.isRpc() {
			go rpcServer(ctx, peerPtrs)
		}

		// create a group of async processes
		var wg sync.WaitGroup

		// spawn message processing by topics
		wg.Add(1)
		go processBlockHeight(ctx)

		// wg.Add(1)
		// go processData(ctx)

		wg.Add(1)
		go processRange(ctx)

		// wg.Add(1)
		// go processGet(ctx)

		wg.Add(1)
		go processGetRaw(ctx)

		wg.Add(1)
		go processNewHash(ctx)

		wg.Add(1)
		go broadcastBlockHeight(ctx)

		wg.Add(1)
		go broadcastLastRange(ctx)

		if peerId == masterPeerId {
			wg.Add(1)
			go broadcastLastHash(ctx)
		} else {
			// wg.Add(1)
			// go requestMissingHashesAndXunis(ctx)
		}

		if *client {
			// check / renew connections periodically
			wg.Add(1)
			go checkConnections(ctx, destinations)
		}

		// doReceive(ctx, "/xen/blocks/sync/0.1.0")
		// wg.Add(1)
		streamBlocks(ctx)

		// if len(destinations) > 0 {
		//	wg.Add(1)
		//	go discoverPeers(ctx, disc, destinations)
		// }

		// wait until interrupted
		wg.Wait()
	}
}
