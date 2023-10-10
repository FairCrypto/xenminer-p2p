package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/joho/godotenv"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	_ "github.com/mattn/go-sqlite3"
	"github.com/multiformats/go-multiaddr"
	"github.com/samber/lo"
	"log"
	"math"
	"os"
	"strings"
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

type DbBlock struct {
	Id          sql.NullInt32  `json:"id"`
	Timestamp   sql.NullString `json:"timestamp"`
	PrevHash    sql.NullString `json:"prev_hash"`
	MerkleRoot  sql.NullString `json:"merkle_root"`
	RecordsJson sql.NullString `json:"records_json"`
	BlockHash   sql.NullString `json:"block_hash"`
}

type Height struct {
	Max sql.NullInt32 `json:"max_height"`
}

type Blocks []Block

func processBlockHeight(
	ctx context.Context,
	blockHeightSub *pubsub.Subscription,
	getTopic *pubsub.Topic,
	db *sql.DB,
) {
	for {
		msg, err := blockHeightSub.Next(ctx)
		if err != nil {
			log.Fatal("Error getting message", err)
		}
		var blockchainHeight uint
		err = json.Unmarshal(msg.Data, &blockchainHeight)
		if err != nil {
			log.Fatal("Error decoding message", err)
		}

		localHeight := getCurrentHeight(db)
		if blockchainHeight > localHeight {
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
	}
}

func processGet(ctx context.Context, getSub *pubsub.Subscription, dataTopic *pubsub.Topic, db *sql.DB) {
	for {
		msg, err := getSub.Next(ctx)
		// TODO: check and process only messages coming from other nodes, not our own
		if err != nil {
			log.Fatal("Error getting message", err)
		}
		var blockIds []uint
		err = json.Unmarshal(msg.Data, &blockIds)
		if err != nil {
			log.Fatal("Error converting message", err)
		}
		log.Println("WANT block_id(s):", blockIds)
		for _, blockId := range blockIds {
			row := db.QueryRow(getRowBlockchainSql, fmt.Sprintf("%d", blockId))
			if err != nil {
				log.Fatal("Error when opening DB: ", err)
			}
			var block DbBlock
			// var id string
			// var timeStamp string
			// var prevHash string
			// var merkleRoot string
			// var recordsJson string
			// var blockHash string
			err = row.Scan(&block.Id, &block.Timestamp, &block.PrevHash, &block.MerkleRoot, &block.RecordsJson, &block.BlockHash)
			// err = row.Scan(&block.Id, &block.Timestamp, &block.PrevHash, &block.MerkleRoot, &block.RecordsJson, &block.BlockHash)
			if err != nil {
				log.Fatal("Error retrieving data from DB: ", err)
			}
			bytes, err := json.Marshal(block)
			if err != nil {
				log.Fatal("Error converting block", err)
			}
			err = dataTopic.Publish(ctx, bytes)
			if err != nil {
				log.Fatal("Error publishing message", err)
			}
			log.Println("SENT", blockId)
		}
	}
}

func processData(ctx context.Context, dataSub *pubsub.Subscription, db *sql.DB) {
	for {
		msg, err := dataSub.Next(ctx)
		if err != nil {
			log.Fatal("Error getting message", err)
		}
		var blocks Blocks
		err = json.Unmarshal(msg.Data, &blocks)
		if err != nil {
			log.Fatal("Error converting message", err)
		}
		for _, block := range blocks {
			log.Println("DATA block_id:", block.Id, "merkle_root:", block.MerkleRoot[0:6])
			_, err = db.Exec(
				insertBlockchainSql,
				block.Id,
				block.Timestamp,
				block.PrevHash,
				block.MerkleRoot,
				block.RecordsJson,
				block.BlockHash,
			)
			if err != nil {
				log.Fatal("Error adding block to DB", err)
			}
		}
	}
}

func getCurrentHeight(db *sql.DB) uint {
	rows, err := db.Query(getMaxHeightBlockchainSql)
	if err != nil {
		log.Fatal("Error when opening DB: ", err)
	}
	var height Height
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			log.Fatal("Error when closing DB: ", err)
		}
	}(rows)
	rows.Next()
	err = rows.Scan(&height.Max)
	if err != nil {
		log.Fatal("Error retrieving data from DB: ", err)
	}
	if height.Max.Valid {
		return uint(height.Max.Int32)
	} else {
		return 0
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

// /ip4/127.0.0.1/tcp/10330/p2p/Qma3GsJmB47xYuyahPZPSadh1avvxfyYQwk8R3UnFrQ6aP
// /ip4/35.87.16.125/tcp/10330/p2p/12D3KooWEmj8Qy3G68gKTHroiMEn59HiziqEN7QdiHMkviBEDr69

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
	return blockHeightSub, dataSub, getSub, blockHeightTopic, dataTopic, getTopic
}

func setupDB(path string) *sql.DB {
	db, err := sql.Open("sqlite3", path+"/blockchain.db")
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
	// This will be used during connection and stream creation by libp2p.
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
	for i := 0; i < peers.Len(); i++ {
		if peers[i].String() == p {
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

func initNode() {
	log.Println("Initializing node cfg and DB")
	path := ".node"
	// check if dir doesn't exist; if no, create it
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		err := os.Mkdir(path, os.ModePerm)
		if err != nil {
			log.Println(err)
		}
	}
	log.Println("Created dir")
	path = ".node/blockchain.db"
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		db, err := sql.Open("sqlite3", path)
		if err != nil {
			log.Fatal("Error when opening DB file: ", err)
		}
		_, err = db.Exec("VACUUM;")
		if err != nil {
			log.Fatal("Error when init DB file: ", err)
		}
		err = db.Close()
		if err != nil {
			log.Fatal("Error closing DB: ", err)
		}
		log.Println("Created DB")
	}
}

func main() {

	init := flag.Bool("init", false, "init node")
	configPath := flag.String("config", ".node", "path to config file")
	flag.Parse()

	if *init {
		initNode()
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

	// construct a libp2p Host.
	h := setupHost(privKey, addr)

	// setup connections to bootstrap peers
	destinations := prepareBootstrapAddresses(*configPath)
	setupConnections(ctx, h, destinations)

	// setup pubsub protocol (either floodsub or gossip)
	ps, err := pubsub.NewFloodSub(ctx, h)
	// ps, err := pubsub.NewGossipSub(ctx, host)
	if err != nil {
		log.Fatal("Error starting pubsub protocol", err)
	}

	// TODO: setup local mDNS discovery
	// if err := setupDiscovery(host); err != nil {
	//	panic(err)
	// }

	log.Println("Started: ", peerId)

	// subscribe to essential topics
	blockHeightSub, dataSub, getSub, blockHeightTopic, dataTopic, getTopic := subscribeToTopics(ps)

	// spawn message processing by topics
	go processBlockHeight(ctx, blockHeightSub, getTopic, db)
	go processData(ctx, dataSub, db)
	go processGet(ctx, getSub, dataTopic, db)

	// check / renew connections periodically
	every5Seconds := time.NewTicker(5 * time.Second)
	everySecond := time.NewTicker(1 * time.Second)
	go checkConnections(ctx, h, destinations, *every5Seconds, make(chan struct{}))
	go checkPubsubPeers(ps, *every5Seconds, make(chan struct{}))
	go broadcastBlockHeight(ctx, blockHeightTopic, db, *everySecond, make(chan struct{}))

	// wait until interrupted
	select {}
}
