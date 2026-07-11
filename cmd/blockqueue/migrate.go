package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"

	blockqueue "github.com/yudhasubki/blockqueue"
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

	driver, err := openConfiguredDriver(cfg)
	if err != nil {
		return err
	}

	if migrationErr := blockqueue.Migrate(ctx, driver); migrationErr != nil {
		return errors.Join(migrationErr, driver.Close())
	}
	slog.Info("successfully applied embedded migrations")
	return driver.Close()
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
