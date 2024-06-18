package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	blockqueue "github.com/yudhasubki/blockqueue"
	"github.com/yudhasubki/blockqueue/pkg/sqlite"
	"github.com/yudhasubki/blockqueue/pkg/turso"
)

type Migrate struct{}

func (m *Migrate) Run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("blockqueue-http", flag.ContinueOnError)
	path := register(fs)
	fs.Usage = m.Usage

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

	_ = filepath.Walk("migration/", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()

		var buf bytes.Buffer

		_, err = io.Copy(&buf, file)
		if err != nil {
			return err
		}

		_, err = driver.Conn().Exec(buf.String())
		if err != nil {
			slog.Error("failed migrate", "filename", path, "error", err)
			return err
		}
		slog.Info("successfully migrate", "filename", path)

		return nil
	})

	return nil
}

func (m *Migrate) Usage() {
	fmt.Printf(`
The migrate command to migrate to the database.

Usage:
	blockqueue migrate [arguments]

Arguments:
	-config PATH
	    Specifies the configuration file.
`[1:],
	)
}
