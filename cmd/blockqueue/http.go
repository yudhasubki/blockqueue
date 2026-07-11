package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	blockqueue "github.com/yudhasubki/blockqueue"
	"github.com/yudhasubki/blockqueue/httpapi"
)

type HTTP struct{}

func (h *HTTP) Run(ctx context.Context, args []string) error {
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
	checkpointInterval, err := configuredCheckpointInterval(cfg)
	if err != nil {
		return err
	}
	processedRetention, err := optionalConfigDuration("maintenance.processed_retention", cfg.Maintenance.ProcessedRetention)
	if err != nil {
		return err
	}
	deadLetterRetention, err := optionalConfigDuration("maintenance.dead_letter_retention", cfg.Maintenance.DeadLetterRetention)
	if err != nil {
		return err
	}
	scheduleRunRetention, err := optionalConfigDuration("maintenance.schedule_run_retention", cfg.Maintenance.ScheduleRunRetention)
	if err != nil {
		return err
	}
	driver, err := openConfiguredDriver(cfg)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Parse writer config.
	writerConfig := blockqueue.WriterOptions{}
	if cfg.Writer.BatchSize > 0 {
		writerConfig.BatchSize = cfg.Writer.BatchSize
	}
	writerConfig.MaxPendingMessages = cfg.Writer.MaxPendingMessages
	writerConfig.MaxPendingBytes = cfg.Writer.MaxPendingBytes
	if cfg.Writer.FlushInterval != "" {
		duration, err := time.ParseDuration(cfg.Writer.FlushInterval)
		if err != nil {
			_ = driver.Close()
			return fmt.Errorf("parse writer flush_interval: %w", err)
		}
		writerConfig.FlushInterval = duration
	}
	queue := blockqueue.New(driver, blockqueue.Options{
		Writer:               writerConfig,
		CheckpointInterval:   checkpointInterval,
		RetentionPeriod:      processedRetention,
		DeadLetterRetention:  deadLetterRetention,
		ScheduleRunRetention: scheduleRunRetention,
		DisableMetrics:       !cfg.Metric.Enable,
	})

	err = queue.Run(ctx)
	if err != nil {
		_ = driver.Close()
		return err
	}

	mux := chi.NewRouter()
	mux.Mount("/", httpapi.Router(queue, httpapi.Options{}))

	if cfg.Metric.Enable {
		mux.Mount("/prometheus/metrics", promhttp.Handler())
	}

	server := &http.Server{
		Addr:              net.JoinHostPort(cfg.Http.Host, cfg.Http.Port),
		Handler:           mux,
		ReadHeaderTimeout: cfg.Http.ReadHeaderTimeout,
		IdleTimeout:       cfg.Http.IdleTimeout,
		WriteTimeout:      cfg.Http.WriteTimeout,
		BaseContext: func(net.Listener) context.Context {
			return ctx
		},
	}

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	defer signal.Stop(shutdown)

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.ListenAndServe()
	}()
	var runtimeErr error
	select {
	case <-shutdown:
	case <-ctx.Done():
		runtimeErr = ctx.Err()
	case err := <-serveErr:
		if !errors.Is(err, http.ErrServerClosed) {
			runtimeErr = err
		}
	}

	// Stop accepting HTTP requests and drain in-flight handlers before writer
	// admission is closed and the database is checkpointed. Each phase gets
	// its own budget so a slow client cannot consume the writer drain deadline.
	httpCtx, httpCancel := context.WithTimeout(context.Background(), cfg.Http.Shutdown)
	httpErr := server.Shutdown(httpCtx)
	httpCancel()
	queueCtx, queueCancel := context.WithTimeout(context.Background(), cfg.Http.Shutdown)
	queueErr := queue.Shutdown(queueCtx)
	queueCancel()
	return errors.Join(runtimeErr, httpErr, queueErr)
}

func optionalConfigDuration(name, value string) (time.Duration, error) {
	if value == "" {
		return 0, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", name)
	}
	return duration, nil
}

func (h *HTTP) Usage() {
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
