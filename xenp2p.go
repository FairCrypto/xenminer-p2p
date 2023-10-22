package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"flag"
	log0 "github.com/ipfs/go-log/v2"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	drouting "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	_ "github.com/mattn/go-sqlite3"
	"github.com/samber/lo"
	"go.uber.org/zap/zapcore"
	"log"
	"math"
	"os"
	"runtime"
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

/*
	EXAMPLE:
	{
		"account": "0x449e81babda663f233cd197b1a0174e6779f7f8e",
		"block_id": 2740231, // OR "xuni_id": 2740231,
		"date": "2023-10-11 16:16:12",
		"hash_to_verify": "$argon2id$v=19$m=65400,t=1,p=1$WEVOMTAwODIwMjJYRU4$7GIkohf/jGOqPJt08s0FjWk9VqYpjBXEN11HJsBKCbsnUCWUy4vRDXSoZ9CkkDEToYNLJwj4XjHvXYX3VNPnyQ",
		"id": 5006101,
		"key": "7ad9bf0adbfd6e0ff40463eefaaef9544a15639d79e55ee48fd6c6260979ca9b"
	}
*/

type HashRecord struct {
	Id           uint   `json:"id"`
	CreatedAt    string `json:"created_at"`
	Key          string `json:"key"`
	HashToVerify string `json:"hash_to_verify"`
	Account      string `json:"account"`
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
}

type Subs struct {
	shift       *pubsub.Subscription
	newHash     *pubsub.Subscription
	newXuni     *pubsub.Subscription
	blockHeight *pubsub.Subscription
	data        *pubsub.Subscription
	get         *pubsub.Subscription
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
const yieldTime = 100 * time.Millisecond

// const shortYieldTime = 10 * time.Millisecond

func max(a, b uint) uint {
	if a <= b {
		return a
	}
	return b
}

func processBlockHeight(ctx context.Context) {
	subs := ctx.Value("subs").(Subs)
	topics := ctx.Value("topics").(Topics)
	db := ctx.Value("db").(*sql.DB)
	peerId := ctx.Value("peerId").(string)
	logger := ctx.Value("logger").(log0.EventLogger)
	state := ctx.Value("state").(*NetworkState)

	for {
		msg, err := subs.blockHeight.Next(ctx)
		if msg.ReceivedFrom.String() == peerId {
			continue
		} else {
			// logger.Info("received msg from ", msg.ReceivedFrom.String())
		}
		if err != nil {
			logger.Warn("Error getting message: ", err)
		}
		var blockchainHeight uint
		err = json.Unmarshal(msg.Data, &blockchainHeight)
		if err != nil {
			logger.Warn("Error decoding message: ", err)
		}

		localHeight := getCurrentHeight(db)
		if blockchainHeight > localHeight && peerId != masterPeerId {
			logger.Info("DIFF", localHeight, "<", blockchainHeight)
			delta := uint(math.Min(float64(blockchainHeight-localHeight), 5))
			want := make([]uint, delta)
			for i := uint(0); i < delta; i++ {
				want[i] = localHeight + i + 1
			}
			msgBytes, err := json.Marshal(want)
			if err != nil {
				logger.Warn("Error encoding message: ", err)
			}
			err = topics.get.Publish(ctx, msgBytes)
			if err != nil {
				logger.Warn("Error publishing message: ", err)
			}
		}
		if blockchainHeight == localHeight {
			logger.Info("IN SYNC", localHeight, "=", blockchainHeight)
		}
		state.BlockHeight = uint64(max(blockchainHeight, localHeight))
		runtime.Gosched()
	}
}

func processGet(ctx context.Context) {
	subs := ctx.Value("subs").(Subs)
	topics := ctx.Value("topics").(Topics)
	db := ctx.Value("db").(*sql.DB)
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
		logger.Debug("WANT block_id(s):", blockIds)
		for _, blockId := range blockIds {
			block, err := getBlock(db, blockId)
			// NB: ignoring the error which might result from missing blocks
			if err == nil {
				blocks := []Block{*block}
				bytes, err := json.Marshal(blocks)
				if err != nil {
					logger.Warn("Error converting block to data: ", err)
				}
				err = topics.data.Publish(ctx, bytes)
				if err != nil {
					logger.Warn("Error publishing data message: ", err)
				}
				// logger.Info("SENT", blockId)
			} else {
				logger.Debug("!BLOCK", blockId)
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

func processData(ctx context.Context) {
	subs := ctx.Value("subs").(Subs)
	db := ctx.Value("db").(*sql.DB)
	peerId := ctx.Value("peerId").(string)
	logger := ctx.Value("logger").(log0.EventLogger)

	for {
		msg, err := subs.data.Next(ctx)
		if msg.ReceivedFrom.String() == peerId {
			continue
		}
		if err != nil {
			logger.Warn("Error getting data message: ", err)
		}
		var blocks Blocks
		err = json.Unmarshal(msg.Data, &blocks)
		if err != nil {
			logger.Warn("Error converting data message: ", err)
		}
		for _, block := range blocks {
			if msg.ReceivedFrom.String() == peerId {
				logger.Debug("DATA block_id:", block.Id, "merkle_root:", block.MerkleRoot[0:6])
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
					logger.Warn("Error adding block to DB: ", err)
				}
			}
		}
		time.Sleep(yieldTime)
		// runtime.Gosched()
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

/*
func discoverPeers(ctx context.Context, disc *drouting.RoutingDiscovery, destinations []string) {
	h := ctx.Value("host").(host.Host)
	logger := ctx.Value("logger").(log0.EventLogger)

	t := time.NewTicker(20 * time.Second)
	defer t.Stop()
	quit := make(chan struct{})
	// Now, look for others who have announced
	// This is like your friend telling you the location to meet you.

	for {
		select {
		case <-t.C:
			var options []discovery.Option
			options = append(options, discovery.TTL(10*time.Minute))
			t, err := disc.Advertise(ctx, rendezvousString, options...)
			peerChan, err := disc.FindPeers(ctx, rendezvousString)
			logger.Info("Searching for other peers ", t.String())
			if err != nil {
				logger.Warn(err)
			}

			for p := range peerChan {
				logger.Info("Maybe peer:", p)
				if p.ID == h.ID() ||
					hasDestination(destinations, p.ID.String()) ||
					hasPeer(h.Peerstore().Peers(), p.ID.String()) {
					continue
				}
				logger.Info("Found peer:", p)
				err = h.Connect(ctx, p)
				if err != nil {
					logger.Warn("Error connecting to peer: ", err)
				}
				logger.Info("Connected to:", p)
			}

		case <-quit:
			t.Stop()
			return
		}
	}

}
*/

func broadcastBlockHeight(ctx context.Context) {
	topics := ctx.Value("topics").(Topics)
	db := ctx.Value("db").(*sql.DB)

	t := time.NewTicker(20 * time.Second)
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

func processNewHash(ctx context.Context) {
	subs := ctx.Value("subs").(Subs)
	topics := ctx.Value("topics").(Topics)
	// db := ctx.Value("db").(*sql.DB)
	peerId := ctx.Value("peerId").(string)
	logger := ctx.Value("logger").(log0.EventLogger)
	state := ctx.Value("state").(*NetworkState)

	const interval = 60
	// const longInterval = 600

	type HashMap map[uint]uint
	hashMap := HashMap{}
	// queue := make([]HashMap, 0)

	cHash := make(chan HashRecord, 1)
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
		case hash := <-cHash:
			// logger.Info("Discovered New Hash Id ", hash.Id)
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

		case gotState := <-cState:
			logger.Infof(
				"received shift %d (%d), diff %f (%f)",
				gotState.ShiftNumber,
				state.ShiftNumber,
				gotState.Difficulty,
				state.Difficulty,
			)
			if gotState.ShiftNumber > state.ShiftNumber {
				if state.ShiftNumber == 0 {
					lastTs = 0
					hashMap = map[uint]uint{}
				}
				state.ShiftNumber = gotState.ShiftNumber
			}
		}
	}
}

func runShifts(ctx context.Context) {
	subs := ctx.Value("subs").(Subs)
	topics := ctx.Value("topics").(Topics)
	peerId := ctx.Value("peerId").(string)
	logger := ctx.Value("logger").(log0.EventLogger)
	state := ctx.Value("state").(*NetworkState)

	// TODO: refactor to config !!!
	const shiftDuration = 60 * time.Second

	c := make(chan uint64, 1)

	go func() {
		for {
			msg, err := subs.shift.Next(ctx)
			if msg.ReceivedFrom.String() == peerId {
				continue
			}
			var currentShift uint64
			err = json.Unmarshal(msg.Data, &currentShift)
			if err != nil {
				logger.Warn("Error decoding message: ", err)
			}
			c <- currentShift
			runtime.Gosched()
		}
	}()

	for {
		select {
		case shift := <-c:
			logger.Infof(
				"received shift %d (%d)",
				shift,
				state.ShiftNumber,
			)
			if shift > state.ShiftNumber {
				state.ShiftNumber = shift
			}

		case <-time.After(shiftDuration):
			logger.Info("timeout waiting for shift")
			state.ShiftNumber++
			data, err := json.Marshal(state.ShiftNumber)
			if err != nil {
				logger.Warn("Error encoding message: ", err)
			}
			err = topics.shift.Publish(ctx, data)
			if err != nil {
				logger.Warn("Error publishing message: ", err)
			}
			logger.Infof("published shift %d", state.ShiftNumber)
		}
	}

}

func main() {

	logger := log0.Logger("xen-blocks")

	init := flag.Bool("init", false, "init node")
	reset := flag.Bool("reset", false, "reset node")
	syncBlocksToHashes := flag.Bool("hashes", false, "sync hashes")
	configPath := flag.String("config", ".node", "path to config file")
	readOnlyDB := flag.Bool("readonly", false, "open DB as read-only")
	client := flag.Bool("client", false, "start in client-only mode")
	logLevel := flag.String("log", "warn", "log level")
	flag.Parse()

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
	if *reset {
		resetNode(*configPath, logger)
	}

	logger.Info("Loading config from", *configPath)
	logger.Info("Starting Node")

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
	ctx = context.WithValue(ctx, "host", h)
	defer func(h host.Host) {
		_ = h.Close()
	}(h)

	// setup connections to bootstrap peers
	destinations := prepareBootstrapAddresses(*configPath, logger)
	peers := lo.Map(destinations, toAddrInfo)

	var disc *drouting.RoutingDiscovery
	if *client {
		setupConnections(ctx, destinations)
	}
	//} else {
	var dhtOptions []dht.Option
	if len(destinations) > 0 {
		dhtOptions = append(dhtOptions, dht.BootstrapPeers(peers...))
	}
	if *client {
		// dhtOptions = append(dhtOptions, dht.Mode(dht.ModeServer))
		dhtOptions = append(dhtOptions, dht.Mode(dht.ModeClient))
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
	//}

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

	logger.Info("Started: ", peerId)

	// subscribe to essential topics
	topics, subs := subscribeToTopics(ps, logger)
	ctx = context.WithValue(ctx, "topics", topics)
	ctx = context.WithValue(ctx, "subs", subs)

	// create a group of async processes
	var wg sync.WaitGroup

	// spawn message processing by topics
	wg.Add(1)
	go processBlockHeight(ctx)

	wg.Add(1)
	go processData(ctx)

	wg.Add(1)
	go processGet(ctx)

	wg.Add(1)
	go processNewHash(ctx)

	wg.Add(1)
	go broadcastBlockHeight(ctx)

	if peerId == masterPeerId {
		wg.Add(1)
		go broadcastLastHash(ctx)
	} else {
		// wg.Add(1)
		// go runShifts(ctx)
	}

	if *client {
		// check / renew connections periodically
		wg.Add(1)
		go checkConnections(ctx, destinations)
	}

	/*
		if len(destinations) > 0 {
			wg.Add(1)
			go discoverPeers(ctx, disc, destinations)
		}
	*/

	// wait until interrupted
	wg.Wait()

}
