package main

const (
	createBlockchainTableSql string = `
		CREATE TABLE IF NOT EXISTS blockchain (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
		prev_hash TEXT,
		merkle_root TEXT,
		records_json TEXT,
		block_hash TEXT)
	`

	getMaxHeightBlockchainSql string = `
		SELECT MAX(id) as max_height FROM blockchain;
	`

	insertBlockchainSql = `
    	INSERT INTO blockchain (id, timestamp, prev_hash, merkle_root, records_json, block_hash)
     	VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING;
	`

	getRowBlockchainSql string = `
		SELECT * FROM blockchain WHERE id = ?;
	`

	getMissingRowIdsBlockchainSql = `
		select id+1 from blockchain bo 
		where not exists (
			select null from blockchain bi where bi.id = bo.id + 1
		) group by id limit 10	
	`

	initDbSql = `VACUUM;`
)
