package main

import (
	"context"
	"encoding/json"
	"github.com/gofiber/fiber/v2"
	log0 "github.com/ipfs/go-log/v2"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	crawler "github.com/libp2p/go-libp2p-kad-dht/crawler"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"time"
)

func rpcServer(ctx context.Context, peers []*peer.AddrInfo) {
	h := ctx.Value("host").(host.Host)
	ps := ctx.Value("pubsub").(*pubsub.PubSub)
	dhTable := ctx.Value("dht").(*dht.IpfsDHT)
	logger := ctx.Value("logger").(log0.EventLogger)

	app := fiber.New()

	app.Get("/status", func(c *fiber.Ctx) error {
		nodes := h.Peerstore().Peers()
		res, _ := json.Marshal(nodes)
		return c.SendString(string(res[:]))
	})

	app.Get("/pubsub/:topic", func(c *fiber.Ctx) error {
		nodes := ps.ListPeers(c.Params("topic"))
		res, _ := json.Marshal(nodes)
		return c.SendString(string(res[:]))
	})

	app.Get("/dht/crawl", func(c *fiber.Ctx) error {
		cr, _ := crawler.NewDefaultCrawler(h)
		cr.Run(ctx, peers,
			func(p peer.ID, rtPeers []*peer.AddrInfo) {
				logger.Info(p, rtPeers)
				res, _ := json.Marshal(rtPeers)
				c.SendString(string(res[:]))
			},
			func(p peer.ID, err error) {
				logger.Warn(err)
				c.SendString("error")
			})
		time.Sleep(10 * time.Second)
		return c.SendString("OK")
	})

	app.Get("/dht", func(c *fiber.Ctx) error {
		logger.Info("RT", dhTable.RoutingTable().GetPeerInfos())
		nodes := dhTable.RoutingTable().ListPeers()
		res, _ := json.Marshal(nodes)
		return c.SendString(string(res[:]))
	})

	err := app.Listen(":3333")
	if err != nil {
		logger.Warn("Error in RPC: ", err)
	} else {
		logger.Info("RPC is active")
	}
}
