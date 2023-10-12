package main

import (
	"database/sql"
	"fmt"
	"log"
)

func getBlock(db *sql.DB, blockId uint) (*Block, error) {
	row := db.QueryRow(getRowBlockchainSql, fmt.Sprintf("%d", blockId))
	var block Block
	err := row.Scan(&block.Id, &block.Timestamp, &block.PrevHash, &block.MerkleRoot, &block.RecordsJson, &block.BlockHash)
	if err != nil {
		return nil, err
	}
	return &block, nil
}

func getPrevBlock(db *sql.DB, block *Block) (*Block, error) {
	prevRow := db.QueryRow(getRowBlockchainSql, fmt.Sprintf("%d", block.Id-1))
	var prevBlock Block
	err := prevRow.Scan(
		&prevBlock.Id,
		&prevBlock.Timestamp,
		&prevBlock.PrevHash,
		&prevBlock.MerkleRoot,
		&prevBlock.RecordsJson,
		&prevBlock.BlockHash,
	)
	if err != nil {
		return nil, err
	}
	return &prevBlock, nil
}

func insertBlock(db *sql.DB, block *Block) error {
	_, err := db.Exec(
		insertBlockchainSql,
		block.Id,
		block.Timestamp,
		block.PrevHash,
		block.MerkleRoot,
		block.RecordsJson,
		block.BlockHash,
	)
	return err
}

func getMissingBlocks(db *sql.DB) []uint {
	currentHeight := getCurrentHeight(db)
	rows, err := db.Query(getMissingRowIdsBlockchainSql)
	if err != nil {
		log.Println("Error when querying DB: ", err)
		return make([]uint, 0)
	}
	var blockId uint
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			log.Println("Error when closing rows: ", err)
		}
	}(rows)
	var blocks []uint
	for rows.Next() {
		err = rows.Scan(&blockId)
		if blockId < currentHeight {
			// avoid repeatedly asking for next block if the DB is synced
			blocks = append(blocks, blockId)
		}
	}
	return blocks
}

func getCurrentHeight(db *sql.DB) uint {
	rows, err := db.Query(getMaxHeightBlockchainSql)
	if err != nil {
		log.Println("Error when querying DB: ", err)
		return 0
	}
	var height Height
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			log.Println("Error when closing rows: ", err)
		}
	}(rows)
	rows.Next()
	err = rows.Scan(&height.Max)
	if err != nil {
		log.Println("Error retrieving data from DB: ", err)
	}
	if height.Max.Valid {
		return uint(height.Max.Int32)
	} else {
		return 0
	}
}
