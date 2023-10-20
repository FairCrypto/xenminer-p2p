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

	getAllRowsBlockchainSql string = `
		SELECT * FROM blockchain;
	`

	getMissingRowIdsBlockchainSql = `
		select id+1 from blockchain bo 
		where not exists (
			select null from blockchain bi where bi.id = bo.id + 1
		) group by id limit 10	
	`

	initDbSql = `VACUUM;`

	resetDbSql = `delete from blockchain;`

	createHashesTableSql = `
		CREATE TABLE IF NOT EXISTS blocks (
    		block_id INTEGER PRIMARY KEY,
    		hash_to_verify TEXT,
    		key TEXT UNIQUE,
    		account TEXT,
    		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);
	`

	createXunisTableSql = `
		CREATE TABLE IF NOT EXISTS xuni (
    		id INTEGER PRIMARY KEY,
    		hash_to_verify TEXT,
    		key TEXT UNIQUE,
    		account TEXT,
    		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		);
	`

	insertHashSql = `
		INSERT INTO blocks (block_id, created_at, key, hash_to_verify, account)
		VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING;
	`

	insertXuniSql = `
		INSERT INTO xuni (id, created_at, key, hash_to_verify, account)
		VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING;
	`
	getLatestHashIdSql string = `
		SELECT MAX(id) as latest_hash_id FROM blocks;
	`

	getLatestXuniIdSql string = `
		SELECT MAX(id) as latest_xuni_id FROM xuni;
	`
)
