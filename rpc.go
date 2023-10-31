package main

import (
	"context"
	"encoding/json"
	"github.com/gofiber/fiber/v2"
	log0 "github.com/ipfs/go-log/v2"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
)

func rpcServer(ctx context.Context) {
	h := ctx.Value("host").(host.Host)
	ps := ctx.Value("pubsub").(*pubsub.PubSub)
	dht := ctx.Value("dht").(*dht.IpfsDHT)
	logger := ctx.Value("logger").(log0.EventLogger)

	app := fiber.New()

	app.Get("/status", func(c *fiber.Ctx) error {
		nodes := h.Peerstore().Peers()
		res, _ := json.Marshal(nodes)
		return c.SendString(string(res[:]))
	})

	app.Get("/pubsub", func(c *fiber.Ctx) error {
		nodes := ps.ListPeers("data")
		res, _ := json.Marshal(nodes)
		return c.SendString(string(res[:]))
	})

	app.Get("/dht", func(c *fiber.Ctx) error {
		nodes, _ := dht.NetworkSize()
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
