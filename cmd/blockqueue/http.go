package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/lesismal/nbio/nbhttp"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	blockqueue "github.com/yudhasubki/blockqueue"
	"github.com/yudhasubki/blockqueue/pkg/etcd"
	"github.com/yudhasubki/blockqueue/pkg/sqlite"
	"github.com/yudhasubki/blockqueue/pkg/turso"
)

type Http struct{}

func (h *Http) Run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("blockqueue-http", flag.ContinueOnError)
	path := register(fs)
	fs.Usage = h.Usage

	err := fs.Parse(args)
	if err != nil {
		return err
	}

	if *path == "" {
		return errorEmptyPath
	}

	cfg, err := ReadConfigFile(*path)
	if err != nil {
		return err
	}

	var driver blockqueue.Driver
	switch cfg.Http.Driver {
	case "turso":
		turso, err := turso.New(cfg.Turso.URL)
		if err != nil {
			return err
		}
		driver = turso
	case "sqlite", "":
		sqlite, err := sqlite.New(cfg.SQLite.DatabaseName, sqlite.Config{
			BusyTimeout: cfg.SQLite.BusyTimeout,
		})
		if err != nil {
			slog.Error("failed to open database", "error", err)
			return err
		}

		driver = sqlite
	}

	etcd, err := etcd.New(
		cfg.Etcd.Path,
		etcd.WithSync(cfg.Etcd.Sync),
	)
	if err != nil {
		slog.Error("failed to open etcd database", "error", err)
		return err
	}

	ctx, cancel := context.WithCancel(ctx)

	stream := blockqueue.New(driver, etcd)

	err = stream.Run(ctx)
	if err != nil {
		cancel()
		return err
	}

	mux := chi.NewRouter()
	mux.Mount("/", (&blockqueue.Http{
		Stream: stream,
	}).Router())

	if cfg.Metric.Enable {
		mux.Mount("/prometheus/metrics", promhttp.Handler())
	}

	engine := nbhttp.NewEngine(nbhttp.Config{
		Network: "tcp",
		Addrs:   []string{":" + cfg.Http.Port},
		Handler: mux,
		IOMod:   nbhttp.IOModNonBlocking,
	})

	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	err = engine.Start()
	if err != nil {
		cancel()
		return err
	}
	<-shutdown

	cancel()

	engine.Stop()
	stream.Close()
	driver.Close()
	etcd.Close()

	// handling graceful shutdown
	time.Sleep(cfg.Http.Shutdown)

	return nil
}

func (h *Http) Usage() {
	fmt.Printf(`
The HTTP command lists all protocol needed in the configuration file.

Usage:
	blockqueue http [arguments]

Arguments:
	-config PATH
	    Specifies the configuration file.
`[1:],
	)
}
