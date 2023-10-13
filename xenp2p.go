package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/joho/godotenv"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/discovery"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	drouting "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	dutil "github.com/libp2p/go-libp2p/p2p/discovery/util"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	_ "github.com/mattn/go-sqlite3"
	"github.com/multiformats/go-multiaddr"
	"github.com/samber/lo"
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

const masterPeerId = "12D3KooWLGpxvuNUmMLrQNKTqvxXbXkR1GceyRSpQXd8ZGmprvjH"

func processBlockHeight(
	peerId string,
	ctx context.Context,
	blockHeightSub *pubsub.Subscription,
	getTopic *pubsub.Topic,
	db *sql.DB,
) {
	for {
		msg, err := blockHeightSub.Next(ctx)
		if msg.ReceivedFrom.String() == peerId {
			continue
		}
		if err != nil {
			log.Fatal("Error getting message", err)
		}
		var blockchainHeight uint
		err = json.Unmarshal(msg.Data, &blockchainHeight)
		if err != nil {
			log.Fatal("Error decoding message", err)
		}

		localHeight := getCurrentHeight(db)
		if blockchainHeight > localHeight && peerId != masterPeerId {
			log.Println("DIFF", localHeight, "<", blockchainHeight)
			delta := uint(math.Min(float64(blockchainHeight-localHeight), 5))
			want := make([]uint, delta)
			for i := uint(0); i < delta; i++ {
				want[i] = localHeight + i + 1
			}
			msgBytes, err := json.Marshal(want)
			if err != nil {
				log.Fatal("Error encoding message", err)
			}
			err = getTopic.Publish(ctx, msgBytes)
			if err != nil {
				log.Fatal("Error publishing message", err)
			}
		}
		if blockchainHeight == localHeight {
			log.Println("IN SYNC", localHeight, "=", blockchainHeight)
		}
	}
}

func processGet(
	peerId string,
	ctx context.Context,
	getSub *pubsub.Subscription,
	dataTopic *pubsub.Topic,
	db *sql.DB,
) {
	for {
		msg, err := getSub.Next(ctx)
		if msg.ReceivedFrom.String() == peerId {
			continue
		}
		if err != nil {
			log.Fatal("Error getting want message: ", err)
		}
		var blockIds []uint
		err = json.Unmarshal(msg.Data, &blockIds)
		if err != nil {
			log.Fatal("Error converting want message: ", err)
		}
		log.Println("WANT block_id(s):", blockIds)
		for _, blockId := range blockIds {
			block, err := getBlock(db, blockId)
			// NB: ignoring the error which might result from missing blocks
			if err == nil {
				blocks := []Block{*block}
				bytes, err := json.Marshal(blocks)
				if err != nil {
					log.Fatal("Error converting block to data: ", err)
				}
				err = dataTopic.Publish(ctx, bytes)
				if err != nil {
					log.Fatal("Error publishing data message: ", err)
				}
				// log.Println("SENT", blockId)
			} else {
				log.Println("!BLOCK", blockId)
				err = nil
			}
		}
	}
}

func validateBlock(block Block) (bool, error) {
	recordsJson := block.RecordsJson
	var records []Record
	err := json.Unmarshal([]byte(recordsJson), &records)
	if err != nil {
		log.Println("Error converting records JSON: ", err)
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

func processData(
	peerId string,
	ctx context.Context,
	dataSub *pubsub.Subscription,
	db *sql.DB,
) {
	for {
		msg, err := dataSub.Next(ctx)
		if msg.ReceivedFrom.String() == peerId {
			continue
		}
		if err != nil {
			log.Fatal("Error getting data message: ", err)
		}
		var blocks Blocks
		err = json.Unmarshal(msg.Data, &blocks)
		if err != nil {
			log.Fatal("Error converting data message: ", err)
		}
		for _, block := range blocks {
			if msg.ReceivedFrom.String() == peerId {
				log.Println("DATA block_id:", block.Id, "merkle_root:", block.MerkleRoot[0:6])
			}
			if block.Id > 1 {
				prevBlock, err := getPrevBlock(db, &block)
				if err != nil {
					// log.Println("Error when processing row: ", err)
					continue
				}
				if prevBlock.BlockHash != block.PrevHash {
					log.Println("Error block hash mismatch on ids: ", prevBlock.BlockHash, block.PrevHash)
					continue
				}
			}
			blockIsValid, err := validateBlock(block)
			if peerId != masterPeerId && blockIsValid {
				err = insertBlock(db, &block)
				if err != nil {
					log.Println("Error adding block to DB: ", err)
				}
			}
		}
	}
}

func loadPeerParams(path string) (multiaddr.Multiaddr, crypto.PrivKey, string) {
	content, err := os.ReadFile(path + "/peer.json")
	if err != nil {
		log.Fatal("Error when opening file: ", err)
	}
	err = godotenv.Load(path + "/.env")
	if err != nil {
		log.Fatal("Error loading ENV: ", err)
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "10330"
	}

	// Now let's unmarshall the data into `peerId`
	var peerId PeerId
	err = json.Unmarshal(content, &peerId)
	if err != nil {
		log.Fatal("Error reading Peer config file: ", err)
	}
	log.Println("PeerId: ", peerId.Id)

	addr, err := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/0.0.0.0/tcp/%s", port))
	if err != nil {
		log.Fatal("Error making address: ", err)
	}

	sk, err := base64.StdEncoding.DecodeString(peerId.PrivKey)
	if err != nil {
		log.Fatal("Error base64-decoding pk: ", err)
	}

	privKey, err := crypto.UnmarshalPrivateKey(sk)
	if err != nil {
		log.Fatal("Error converting pk: ", err)
	}

	return addr, privKey, peerId.Id
}

func prepareBootstrapAddresses(path string) []string {
	err := godotenv.Load(path + "/.env")
	if err != nil {
		log.Fatal("Error loading ENV: ", err)
	}
	notEmpty := func(item string, index int) bool {
		return item != ""
	}
	bootstrapHosts := lo.Filter[string](strings.Split(os.Getenv("BOOTSTRAP_HOSTS"), ","), notEmpty)
	bootstrapPorts := lo.Filter[string](strings.Split(os.Getenv("BOOTSTRAP_PORTS"), ","), notEmpty)
	bootstrapPeers := lo.Filter[string](strings.Split(os.Getenv("BOOTSTRAP_PEERS"), ","), notEmpty)

	destinations := make([]string, len(bootstrapPeers))
	for i, peerId := range bootstrapPeers {
		destinations[i] = fmt.Sprintf(
			"/ip4/%s/tcp/%s/p2p/%s",
			bootstrapHosts[i],
			bootstrapPorts[i],
			peerId,
		)
	}
	return destinations
}

func subscribeToTopics(ps *pubsub.PubSub) (
	*pubsub.Subscription,
	*pubsub.Subscription,
	*pubsub.Subscription,
	*pubsub.Topic,
	*pubsub.Topic,
	*pubsub.Topic,
) {
	blockHeightTopic, err := ps.Join("block_height")
	if err != nil {
		log.Fatal("Error joining topic", err)
	}
	blockHeightSub, err := blockHeightTopic.Subscribe()
	if err != nil {
		log.Fatal("Error subscribing to topic", err)
	}

	getTopic, err := ps.Join("get")
	if err != nil {
		log.Fatal("Error joining topic", err)
	}
	getSub, err := getTopic.Subscribe()
	if err != nil {
		log.Fatal("Error subscribing to topic", err)
	}

	dataTopic, err := ps.Join("data")
	if err != nil {
		log.Fatal("Error joining topic", err)
	}
	dataSub, err := dataTopic.Subscribe()
	if err != nil {
		log.Fatal("Error subscribing to topic", err)
	}
	log.Println(blockHeightTopic.ListPeers(), dataTopic.ListPeers(), getTopic.ListPeers())
	return blockHeightSub, dataSub, getSub, blockHeightTopic, dataTopic, getTopic
}

func setupDB(path string) *sql.DB {
	err := godotenv.Load(path + "/.env")
	var dbPath = ""
	if err != nil {
		err = nil
	}
	dbPath = os.Getenv("DB_LOCATION")
	if dbPath == "" {
		dbPath = path + "/blockchain.db"
	}

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		log.Fatal("Error when opening DB file: ", err)
	}

	_, err = db.Exec(createBlockchainTableSql)
	if err != nil {
		log.Fatal("Error when checking/creating table: ", err)
	}

	maxHeight := getCurrentHeight(db)
	log.Println("HGHT", maxHeight)

	return db
}

func setupHost(privKey crypto.PrivKey, addr multiaddr.Multiaddr) host.Host {
	h, err := libp2p.New(
		libp2p.ListenAddrs(addr),
		libp2p.Identity(privKey),
		libp2p.Transport(tcp.NewTCPTransport),
		// libp2p.Security(noise.ID, noise.New), // redundant
	)
	if err != nil {
		log.Fatal("Error starting Peer: ", err)
	}
	return h
}

func connectToPeer(ctx context.Context, h host.Host, destination string) {
	// Turn the destination into a multiaddr.
	address, err := multiaddr.NewMultiaddr(destination)
	if err != nil {
		log.Println(err)
	}
	info, err := peer.AddrInfoFromP2pAddr(address)
	if err != nil {
		log.Println(err)
	}
	// Add the destination's peer multiaddress in the peerstore.
	// This will be used during connection and stream creation by Libp2p.
	h.Peerstore().AddAddrs(info.ID, info.Addrs, peerstore.PermanentAddrTTL)
	err = h.Connect(ctx, *info)
	if err != nil {
		log.Println("Error connecting: ", err)
	}
}

func setupConnections(ctx context.Context, h host.Host, destinations []string) {
	log.Println(destinations)

	for _, dest := range destinations {
		connectToPeer(ctx, h, dest)
		log.Println("Connect to: ", dest)
	}
}

func hasPeer(peers peer.IDSlice, p string) bool {
	log.Println("?", peers, p)
	for i := 0; i < peers.Len(); i++ {
		if peers[i].String() == p {
			return true
		}
	}
	return false
}

func hasDestination(destinations []string, p string) bool {
	log.Println("??", destinations, p)
	for i := 0; i < len(destinations); i++ {
		if destinations[i] == p {
			return true
		}
	}
	return false
}

func checkConnections(ctx context.Context, h host.Host, destinations []string, t time.Ticker, quit <-chan struct{}) {
	for {
		select {
		case <-t.C:
			// check if peer is not connected and try to reconnect
			peers := h.Peerstore().Peers()
			log.Println(peers)
			for _, addr := range destinations {
				if !hasPeer(peers, addr) {
					connectToPeer(ctx, h, addr)
				}
			}

		case <-quit:
			t.Stop()
			return
		}
	}
}

func discoverPeers(
	ctx context.Context,
	h host.Host,
	disc *drouting.RoutingDiscovery,
	destinations []string,
	t time.Ticker,
	quit <-chan struct{},
) {
	// Now, look for others who have announced
	// This is like your friend telling you the location to meet you.

	for {
		select {
		case <-t.C:
			var options []discovery.Option
			options = append(options, discovery.TTL(10*time.Minute))
			// _ = options.Apply()
			t, err := disc.Advertise(ctx, "/peers", options...)
			peerChan, err := disc.FindPeers(ctx, "/peers")
			log.Println("Searching for other peers for ", t.String())
			if err != nil {
				log.Println(err)
			}

			for p := range peerChan {
				log.Println(
					"Candidate ",
					p.ID,
					h.ID(),
					p.ID == h.ID(),
					hasDestination(destinations, p.ID.String()),
					hasPeer(h.Peerstore().Peers(), p.ID.String()),
				)
				if p.ID == h.ID() ||
					hasDestination(destinations, p.ID.String()) ||
					hasPeer(h.Peerstore().Peers(), p.ID.String()) {
					continue
				}
				log.Println("Found peer:", p)
				err = h.Connect(ctx, p)
				if err != nil {
					log.Println("Error connecting to peer: ", err)
				}
				log.Println("Connected to:", p)
			}

		case <-quit:
			t.Stop()
			return
		}
	}

}

func checkPubsubPeers(ps *pubsub.PubSub, t time.Ticker, quit <-chan struct{}) {
	for {
		select {
		case <-t.C:
			// check if peer is not connected and try to reconnect

			peers := ps.ListPeers("block_height")
			log.Println("PEERS", peers)

		case <-quit:
			t.Stop()
			return
		}
	}
}

func broadcastBlockHeight(ctx context.Context, topic *pubsub.Topic, db *sql.DB, t time.Ticker, quit <-chan struct{}) {
	for {
		select {
		case <-t.C:
			maxHeight := getCurrentHeight(db)
			bytes, err := json.Marshal(maxHeight)
			if err != nil {
				log.Fatal("Error converting block_height", err)
			}
			err = topic.Publish(ctx, bytes)
			if err != nil {
				log.Fatal("Error publishing message", err)
			}
		case <-quit:
			t.Stop()
			return
		}
	}
}

func doHousekeeping(ctx context.Context, topic *pubsub.Topic, db *sql.DB, t time.Ticker, quit <-chan struct{}) {
	for {
		select {
		case <-t.C:
			blocks := getMissingBlocks(db)
			if len(blocks) > 0 {
				bytes, err := json.Marshal(blocks)
				if err != nil {
					log.Fatal("Error converting block_id: ", err)
				}
				err = topic.Publish(ctx, bytes)
				if err != nil {
					log.Fatal("Error publishing message: ", err)
				}
			}
		case <-quit:
			t.Stop()
			return
		}
	}
}

func initNode(path0 string) {
	log.Println("Initializing node cfg and DB")
	var path string
	if path0 == "" {
		path = ".node"
	} else {
		path = path0
	}
	// check if dir doesn't exist; if no, create it
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		err := os.Mkdir(path, os.ModePerm)
		if err != nil {
			log.Println(err)
		}
	}
	log.Println("Created dir")
	pathToDb := path + "/blockchain.db"
	if _, err := os.Stat(pathToDb); errors.Is(err, os.ErrNotExist) {
		db, err := sql.Open("sqlite3", pathToDb)
		if err != nil {
			log.Fatal("Error when opening DB file: ", err)
		}
		_, err = db.Exec(initDbSql)
		if err != nil {
			log.Fatal("Error when init DB file: ", err)
		}
		err = db.Close()
		if err != nil {
			log.Fatal("Error closing DB: ", err)
		}
		log.Println("Created DB")
	}

	pathToPeerId := path + "/peer.json"
	if _, err := os.Stat(pathToPeerId); errors.Is(err, os.ErrNotExist) {
		priv, pub, err := crypto.GenerateEd25519Key(rand.Reader)
		if err != nil {
			log.Fatal("Error when generating keypair: ", err)
		}
		privBytes, err := crypto.MarshalPrivateKey(priv)
		pubBytes, err := crypto.MarshalPublicKey(pub)
		id, err := peer.IDFromPublicKey(pub)
		if err != nil {
			log.Fatal("Error when converting keypair: ", err)
		}
		privKey := base64.StdEncoding.EncodeToString(privBytes)
		pubKey := base64.StdEncoding.EncodeToString(pubBytes)
		peerId := PeerId{Id: id.String(), PrivKey: privKey, PubKey: pubKey}
		bytes, err := json.Marshal(&peerId)
		if err != nil {
			log.Fatal("Error when converting peerId: ", err)
		}
		err = os.WriteFile(pathToPeerId, bytes, os.ModePerm)
		if err != nil {
			log.Fatal("Error writing peerId file: ", err)
		}
		log.Println("Created peerId file")
	}
}

func setupDiscovery(ctx context.Context, h host.Host, dht *dht.IpfsDHT, destinations []string) *drouting.RoutingDiscovery {

	// Let's connect to the bootstrap nodes first. They will tell us about the
	// other nodes in the network.
	var wg sync.WaitGroup
	for _, peerAddr := range destinations {
		address, err := multiaddr.NewMultiaddr(peerAddr)
		if err != nil {
			log.Println(err)
		}
		peerInfo, _ := peer.AddrInfoFromP2pAddr(address)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := h.Connect(ctx, *peerInfo); err != nil {
				log.Println(err)
			} else {
				log.Println("Connection established with bootstrap node:", *peerInfo)
			}
		}()
	}
	wg.Wait()

	// We use a rendezvous point "meet me here" to announce our location.
	// This is like telling your friends to meet you at the Eiffel Tower.
	routingDiscovery := drouting.NewRoutingDiscovery(dht)
	dutil.Advertise(ctx, routingDiscovery, "/peers")
	log.Println("Started announcing")

	log.Println("Searching for other peers")
	peerChan, err := routingDiscovery.FindPeers(ctx, "/peers")
	if err != nil {
		log.Println(err)
	}

	for p := range peerChan {
		if p.ID == h.ID() || hasDestination(destinations, p.ID.String()) {
			continue
		}
		log.Println("Found peer:", p)
		err = h.Connect(ctx, p)
		if err != nil {
			log.Println("Error connecting to peer: ", err)
		}
		log.Println("Connected to:", p)
	}
	return routingDiscovery

}

func main() {

	init := flag.Bool("init", false, "init node")
	configPath := flag.String("config", ".node", "path to config file")
	flag.Parse()

	if *init {
		initNode(*configPath)
		os.Exit(0)
	}

	log.Println("Loading config from", *configPath)

	// setup DB and check / init table(s)
	db := setupDB(*configPath)

	log.Println("Starting Node")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// load peer params from config file
	addr, privKey, peerId := loadPeerParams(*configPath)
	if peerId == masterPeerId {
		log.Println("Master Node")
	} else {
		log.Println("Peer Node")
	}

	// construct a libp2p Host.
	h := setupHost(privKey, addr)

	// setup connections to bootstrap peers
	destinations := prepareBootstrapAddresses(*configPath)

	// setup DHT discovery
	var options []dht.Option
	if len(destinations) == 0 {
		options = append(options, dht.Mode(dht.ModeServer))
	}
	kademliaDHT, err := dht.New(ctx, h, options...)
	if err != nil {
		panic(err)
	}

	// Bootstrap the DHT. In the default configuration, this spawns a Background
	// thread that will refresh the peer table every five minutes.
	log.Println("Bootstrapping the DHT")
	if err = kademliaDHT.Bootstrap(ctx); err != nil {
		panic(err)
	}
	time.Sleep(2 * time.Second)
	setupDiscovery(ctx, h, kademliaDHT, destinations)
	disc := setupDiscovery(ctx, h, kademliaDHT, destinations)
	// setupConnections(ctx, h, destinations)

	// setup pubsub protocol (either floodsub or gossip)
	ps, err := pubsub.NewFloodSub(ctx, h)
	// ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		log.Fatal("Error starting pubsub protocol", err)
	}

	log.Println("Started: ", peerId)

	// subscribe to essential topics
	blockHeightSub, dataSub, getSub, blockHeightTopic, dataTopic, getTopic := subscribeToTopics(ps)

	// spawn message processing by topics
	go processBlockHeight(peerId, ctx, blockHeightSub, getTopic, db)
	go processData(peerId, ctx, dataSub, db)
	go processGet(peerId, ctx, getSub, dataTopic, db)

	// check / renew connections periodically
	every15Seconds := time.NewTicker(15 * time.Second)
	defer every15Seconds.Stop()
	go discoverPeers(ctx, h, disc, destinations, *every15Seconds, make(chan struct{}))

	every5Seconds := time.NewTicker(5 * time.Second)
	defer every5Seconds.Stop()
	go checkConnections(ctx, h, destinations, *every5Seconds, make(chan struct{}))
	go checkPubsubPeers(ps, *every5Seconds, make(chan struct{}))
	go doHousekeeping(ctx, getTopic, db, *every5Seconds, make(chan struct{}))

	everySecond := time.NewTicker(2 * time.Second)
	defer everySecond.Stop()
	go broadcastBlockHeight(ctx, blockHeightTopic, db, *everySecond, make(chan struct{}))

	// wait until interrupted
	select {}
}
