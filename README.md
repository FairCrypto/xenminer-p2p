# XENMiner Go (P2P layer)

## Prerequisites

1. Install Go

Instructions: https://go.dev/doc/install

2. Install sqlite

Instructions: https://www.sqlite.org/download.html

### Installation

1. Clone repo:

```azure
git clone https://github.com/FairCrypto/xenminer-p2p.git
cd xenminer-p2p
```

2. Open screen or tmux window
```azure
screen
```

3. Run XENMiner
```azure
go run *.go --log info --roles miner,rpc
```

4. Close screen/tmux (XENMiner will keep running)
```azure
Ctrl-A and D
```

### Check DHT Routing Table
```azure
curl -i http://localhost:3333/dht
```