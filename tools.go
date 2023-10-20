package main

import (
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	log0 "github.com/ipfs/go-log/v2"
	"github.com/joho/godotenv"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"log"
	"os"
)

func initNode(path0 string, logger log0.EventLogger) {
	logger.Info("Initializing node cfg and DB")
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
			logger.Warn(err)
		}
	}
	logger.Info("Created dir")
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
		logger.Info("Created DB")
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
		logger.Info("Created peerId file")
	}
}

func resetNode(path0 string, logger log0.EventLogger) {
	err := godotenv.Load(path0 + "/.env")
	var dbPath = ""
	if err != nil {
		err = nil
	}
	dbPath = os.Getenv("DB_LOCATION")
	if dbPath == "" {
		dbPath = path0 + "/blockchain.db"
	}

	logger.Info("Resetting node to block height 0: ", dbPath)
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		log.Fatal("Error when opening DB file: ", err)
	}
	_, err = db.Exec(resetDbSql)
	if err != nil {
		log.Fatal("Error when resetting DB: ", err)
	}
	err = db.Close()
	if err != nil {
		log.Fatal("Error closing DB: ", err)
	}
	logger.Info("Reset DB")
}

func syncHashes(path0 string, logger log0.EventLogger) {
	err := godotenv.Load(path0 + "/.env")
	var dbPath = ""
	if err != nil {
		err = nil
	}
	dbPath = os.Getenv("DB_LOCATION")
	if dbPath == "" {
		dbPath = path0 + "/blockchain.db?cache=shared&mode=rwc&_journal_mode=WAL"
	}

	logger.Info("Opening DB: ", dbPath)
	db, err := sql.Open("sqlite3", "file:"+dbPath)
	if err != nil {
		log.Fatal("Error when opening DB file: ", err)
	}

	_, err = db.Exec(createHashesTableSql)
	if err != nil {
		log.Fatal("Error creating hashes table: ", err)
	}

	_, err = db.Exec(createXunisTableSql)
	if err != nil {
		log.Fatal("Error creating xunis table: ", err)
	}

	logger.Info("Copying hashes...")
	c := getAllBlocks(db)
	count := 0
	xen11 := 0
	xuni := 0
	for row := range c {
		var records []Record
		err := json.Unmarshal([]byte(row.RecordsJson), &records)
		for _, rec := range records {
			// datetime, err := time.Parse(time.RFC3339, strings.Replace(rec.Date, " ", "T", 1)+"Z")
			// if err != nil {
			// 	log.Println("Error parsing time: ", err)
			// }
			var hashRec HashRecord
			if rec.XuniId != nil {
				hashRec = HashRecord{
					Id: uint(int64(*rec.XuniId)),
					// CreatedAt:    uint(datetime.Unix()),
					CreatedAt:    rec.Date,
					Account:      rec.Account,
					HashToVerify: rec.HashToVerify,
					Key:          rec.Key,
				}
				err = insertXuniRecord(db, hashRec)
				xuni++
			} else {
				hashRec = HashRecord{
					Id: uint(int64(*rec.BlockId)),
					// CreatedAt:    uint(datetime.Unix()),
					CreatedAt:    rec.Date,
					Account:      rec.Account,
					HashToVerify: rec.HashToVerify,
					Key:          rec.Key,
				}
				err = insertHashRecord(db, hashRec)
				xen11++
			}
			if err != nil {
				log.Println("\nError inserting: ", err)
			} else {
				count++
				fmt.Printf("\rProcessing rec (%d/%d): %d", xen11, xuni, count)
			}
		}
		if err != nil {
			log.Println("\nError converting: ", err)
		}
	}
	fmt.Println()

	err = db.Close()
	if err != nil {
		log.Fatal("Error closing DB: ", err)
	}
	logger.Info("Done with DB")
}