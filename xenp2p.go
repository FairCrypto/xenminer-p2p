package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	log0 "github.com/ipfs/go-log/v2"
	"github.com/joho/godotenv"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	drouting "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	dutil "github.com/libp2p/go-libp2p/p2p/discovery/util"
	_ "github.com/mattn/go-sqlite3"
	"github.com/multiformats/go-multiaddr"
	"github.com/samber/lo"
	"go.uber.org/zap/zapcore"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
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
	blockHeight *pubsub.Topic
	data        *pubsub.Topic
	get         *pubsub.Topic
}

type Subs struct {
	blockHeight *pubsub.Subscription
	data        *pubsub.Subscription
	get         *pubsub.Subscription
}

const masterPeerId = "12D3KooWLGpxvuNUmMLrQNKTqvxXbXkR1GceyRSpQXd8ZGmprvjH"
const rendezvousString = "/xenblocks/0.1.0"
const yieldTime = 100 * time.Millisecond

func processBlockHeight(ctx context.Context) {
	subs := ctx.Value("subs").(Subs)
	topics := ctx.Value("topics").(Topics)
	db := ctx.Value("db").(*sql.DB)
	peerId := ctx.Value("peerId").(string)
	logger := ctx.Value("logger").(log0.EventLogger)

	for {
		msg, err := subs.blockHeight.Next(ctx)
		if msg.ReceivedFrom.String() == peerId {
			continue
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
		time.Sleep(yieldTime)
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
		time.Sleep(yieldTime)
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
	}
}

func loadPeerParams(path string, logger log0.EventLogger) (multiaddr.Multiaddr, crypto.PrivKey, string) {
	content, err := os.ReadFile(path + "/peer.json")
	if err != nil {
		logger.Error("Error when opening file: ", err)
	}
	err = godotenv.Load(path + "/.env")
	if err != nil {
		logger.Error("Error loading ENV: ", err)
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "10330"
	}

	// Now let's unmarshall the data into `peerId`
	var peerId PeerId
	err = json.Unmarshal(content, &peerId)
	if err != nil {
		logger.Error("Error reading Peer config file: ", err)
	}
	logger.Info("PeerId: ", peerId.Id)

	addr, err := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/0.0.0.0/tcp/%s", port))
	if err != nil {
		logger.Error("Error making address: ", err)
	}

	sk, err := base64.StdEncoding.DecodeString(peerId.PrivKey)
	if err != nil {
		logger.Error("Error base64-decoding pk: ", err)
	}

	privKey, err := crypto.UnmarshalPrivateKey(sk)
	if err != nil {
		logger.Error("Error converting pk: ", err)
	}
	if err != nil {
		panic(err)
	}

	return addr, privKey, peerId.Id
}

func prepareBootstrapAddresses(path string, logger log0.EventLogger) []string {
	err := godotenv.Load(path + "/.env")
	if err != nil {
		logger.Error("Error loading ENV: ", err)
	}
	notEmpty := func(item string, index int) bool {
		return item != ""
	}
	bootstrapHosts := lo.Filter[string](strings.Split(os.Getenv("BOOTSTRAP_HOSTS"), ","), notEmpty)
	bootstrapPorts := lo.Filter[string](strings.Split(os.Getenv("BOOTSTRAP_PORTS"), ","), notEmpty)
	bootstrapPeers := lo.Filter[string](strings.Split(os.Getenv("BOOTSTRAP_PEERS"), ","), notEmpty)

	var destinations []string
	for i, peerId := range bootstrapPeers {
		destinations = append(
			destinations,
			fmt.Sprintf(
				"/ip4/%s/tcp/%s/p2p/%s",
				bootstrapHosts[i],
				bootstrapPorts[i],
				peerId,
			))
	}
	return destinations
}

func subscribeToTopics(ps *pubsub.PubSub, logger log0.EventLogger) (topics Topics, subs Subs) {
	var err error
	topics.blockHeight, err = ps.Join("block_height")
	if err != nil {
		logger.Error("Error joining topic", err)
	}
	subs.blockHeight, err = topics.blockHeight.Subscribe()
	if err != nil {
		logger.Error("Error subscribing to topic", err)
	}

	topics.get, err = ps.Join("get")
	if err != nil {
		logger.Error("Error joining topic", err)
	}
	subs.get, err = topics.get.Subscribe()
	if err != nil {
		logger.Error("Error subscribing to topic", err)
	}

	topics.data, err = ps.Join("data")
	if err != nil {
		logger.Error("Error joining topic", err)
	}
	subs.data, err = topics.data.Subscribe()
	if err != nil {
		logger.Error("Error subscribing to topic", err)
	}

	if err != nil {
		panic(err)
	}
	return
}

func setupDB(path string, ro bool, logger log0.EventLogger) *sql.DB {
	err := godotenv.Load(path + "/.env")
	var dbPath = ""
	if err != nil {
		err = nil
	}
	dbPath = os.Getenv("DB_LOCATION")
	if dbPath == "" {
		dbPath = "file:" + path + "/blockchain.db?cache=shared&"
	} else {
		dbPath = "file:" + dbPath + "?cache=shared&"
	}
	if ro {
		// add read-only flag
		dbPath += "mode=ro"
	} else {
		dbPath += "_journal_mode=WAL&mode=rwc"
	}
	logger.Info("DB path: ", dbPath)
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		log.Fatal("Error when opening DB file: ", err)
	}

	if !ro {
		_, err = db.Exec(createBlockchainTableSql)
		if err != nil {
			log.Fatal("Error when checking/creating table: ", err)
		}
	}

	maxHeight := getCurrentHeight(db)
	logger.Info("HGHT ", maxHeight)

	return db
}

func setupHashesDB(path string, ro bool, logger log0.EventLogger) (*sql.DB, uint, uint) {
	err := godotenv.Load(path + "/.env")
	var dbPath = ""
	if err != nil {
		err = nil
	}
	dbPath = os.Getenv("DBH_LOCATION")
	if dbPath == "" {
		dbPath = "file:" + path + "/blocks.db?cache=shared&"
	} else {
		dbPath = "file:" + dbPath + "?cache=shared&"
	}
	if ro {
		// add read-only flag
		dbPath += "mode=ro"
	} else {
		dbPath += "_journal_mode=WAL&mode=rwc"
	}

	logger.Info("DBH path: ", dbPath)
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		log.Fatal("Error when opening hashes DB file: ", err)
	}

	if !ro {
		_, err = db.Exec(createHashesTableSql)
		if err != nil {
			log.Fatal("Error creating hashes table: ", err)
		}

		_, err = db.Exec(createXunisTableSql)
		if err != nil {
			log.Fatal("Error creating xunis table: ", err)
		}
	}

	lastHashId := getLatestHashId(db)
	lastXuniId := getLatestXuniId(db)
	logger.Info("LAST ", lastHashId, lastXuniId)

	return db, lastHashId, lastXuniId
}

func setupHost(privKey crypto.PrivKey, addr multiaddr.Multiaddr) host.Host {
	h, err := libp2p.New(
		libp2p.ListenAddrs(addr),
		libp2p.Identity(privKey),
		// libp2p.Transport(tcp.NewTCPTransport),
		// libp2p.Security(noise.ID, noise.New), // redundant
	)
	if err != nil {
		log.Fatal("Error starting Peer: ", err)
	}
	return h
}

var toAddrInfo = func(destination string, _ int) peer.AddrInfo {
	address, err := multiaddr.NewMultiaddr(destination)
	if err != nil {
		log.Println(err)
	}
	info, err := peer.AddrInfoFromP2pAddr(address)
	if err != nil {
		log.Println(err)
	}
	return *info
}

func connectToPeer(ctx context.Context, destination string) {
	h := ctx.Value("host").(host.Host)
	logger := ctx.Value("logger").(log0.EventLogger)

	// Turn the destination into a multiaddr.
	info := toAddrInfo(destination, 0)
	// Add the destination's peer multiaddress in the peerstore.
	// This will be used during connection and stream creation by Libp2p.
	h.Peerstore().AddAddrs(info.ID, info.Addrs, peerstore.PermanentAddrTTL)
	err := h.Connect(ctx, info)
	if err != nil {
		logger.Warn("Error connecting: ", err)
	}
}

func setupConnections(ctx context.Context, destinations []string) {
	logger := ctx.Value("logger").(log0.EventLogger)

	logger.Info("destinations", destinations)

	for _, dest := range destinations {
		connectToPeer(ctx, dest)
		logger.Info("Connect to: ", dest)
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

func setupDiscovery(ctx context.Context, destinations []string) *drouting.RoutingDiscovery {
	h := ctx.Value("host").(host.Host)
	dhTable := ctx.Value("dht").(*dht.IpfsDHT)
	logger := ctx.Value("logger").(log0.EventLogger)

	// Let's connect to the bootstrap nodes first. They will tell us about the
	// other nodes in the network.

	var wg sync.WaitGroup
	for i, peerAddr := range destinations {
		peerInfo := toAddrInfo(peerAddr, i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.Peerstore().AddAddrs(peerInfo.ID, peerInfo.Addrs, peerstore.PermanentAddrTTL)
			if err := h.Connect(ctx, peerInfo); err != nil {
				logger.Warn(err)
			} else {
				logger.Warn("Connection established with bootstrap node:", peerInfo)
			}
		}()
	}
	wg.Wait()

	// We use a rendezvous point "meet me here" to announce our location.
	// This is like telling your friends to meet you at the Eiffel Tower.
	routingDiscovery := drouting.NewRoutingDiscovery(dhTable)
	dutil.Advertise(ctx, routingDiscovery, rendezvousString)
	logger.Info("Started announcing")

	logger.Info("Searching for other peers")
	peerChan, err := routingDiscovery.FindPeers(ctx, rendezvousString)
	if err != nil {
		logger.Warn(err)
	}

	for p := range peerChan {
		logger.Info("Peer candidate: ", p)
		if p.ID == h.ID() || hasDestination(destinations, p.ID.String()) {
			continue
		}
		logger.Info("Found peer:", p)
		err = h.Connect(ctx, p)
		if err != nil {
			logger.Warn("Error connecting to peer: ", err)
		}
		logger.Info("Connected to:", p)
	}
	return routingDiscovery

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
	} else {
		log0.SetAllLoggers(log0.LevelWarn)
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, "logger", logger)

	// setup DB and check / init table(s)
	db := setupDB(*configPath, *readOnlyDB, logger)

	_, lastHashId, lastXuniId := setupHashesDB(*configPath, *readOnlyDB, logger)
	logger.Infof("Latest: hash %d, xuni %d", lastHashId, lastXuniId)

	ctx = context.WithValue(ctx, "db", db)
	defer func(db *sql.DB) {
		_ = db.Close()
	}(db)

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
	} else {
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
	}

	// Bootstrap the DHT. In the default configuration, this spawns a Background
	// thread that will refresh the peer table every five minutes.
	// logger.Info("Bootstrapping the DHT")
	// if err = kademliaDHT.Bootstrap(ctx); err != nil {
	// 	panic(err)
	// }

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
	go broadcastBlockHeight(ctx)

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
