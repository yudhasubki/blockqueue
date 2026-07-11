package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

var (
	errorEmptyPath = errors.New("configuration path is empty")
)

func main() {
	m := &Main{}

	err := m.Run(context.Background(), os.Args[1:])
	if err != nil {
		slog.Error("failed to run", "error", err)
		os.Exit(1)
	}
}

type Main struct{}

func (m *Main) Run(ctx context.Context, args []string) error {
	var cmd string
	if len(args) > 0 {
		cmd, args = args[0], args[1:]
	}

	switch cmd {
	case "http":
		return (&HTTP{}).Run(ctx, args)
	case "migrate":
		return (&Migrate{}).Run(ctx, args)
	default:
		if cmd == "" || cmd == "help" {
			m.Usage()
			return flag.ErrHelp
		}

		return fmt.Errorf("unknown command : %v", cmd)
	}
}

func (m *Main) Usage() {
	fmt.Println(`
blockqueue is an embeddable durable message queue server

Usage:

	blockqueue <command> [arguments]

The commands are:

	http    	run the HTTP API and dashboard
	migrate 	apply embedded database migrations
`[1:])
}

type Config struct {
	Http        HTTPConfig        `yaml:"http"`
	Logging     LoggingConfig     `yaml:"logging"`
	SQLite      SQLiteConfig      `yaml:"sqlite"`
	Turso       TursoConfig       `yaml:"turso"`
	PgSQL       PostgreConfig     `yaml:"pgsql"`
	Metric      MetricConfig      `yaml:"metric"`
	Writer      WriterConfig      `yaml:"writer"`
	Maintenance MaintenanceConfig `yaml:"maintenance"`
}

func ReadConfigFile(filename string) (_ Config, err error) {
	var config Config
	b, err := os.ReadFile(filename)
	if err != nil {
		return config, err
	}

	decoder := yaml.NewDecoder(strings.NewReader(os.ExpandEnv(string(b))))
	decoder.KnownFields(true)
	err = decoder.Decode(&config)
	if err != nil {
		return config, err
	}

	if config.Http.Shutdown.Seconds() == 0 {
		config.Http.Shutdown = 30 * time.Second
	}
	if config.Http.Host == "" {
		config.Http.Host = "127.0.0.1"
	}
	if config.Http.Port == "" {
		config.Http.Port = "8080"
	}
	if config.Http.ReadHeaderTimeout <= 0 {
		config.Http.ReadHeaderTimeout = 5 * time.Second
	}
	if config.Http.IdleTimeout <= 0 {
		config.Http.IdleTimeout = 2 * time.Minute
	}
	if config.Http.WriteTimeout <= 0 {
		config.Http.WriteTimeout = 65 * time.Second
	}

	logOutput := os.Stdout
	if config.Logging.Stderr {
		logOutput = os.Stderr
	}

	logOpts := slog.HandlerOptions{
		Level: slog.LevelInfo,
	}

	if config.Http.Driver == "" {
		config.Http.Driver = "sqlite"
	}

	switch strings.ToUpper(config.Logging.Level) {
	case "DEBUG":
		logOpts.Level = slog.LevelDebug
	case "WARN", "WARNING":
		logOpts.Level = slog.LevelWarn
	case "ERROR":
		logOpts.Level = slog.LevelError
	}

	var logHandler slog.Handler
	switch config.Logging.Type {
	case "json":
		logHandler = slog.NewJSONHandler(logOutput, &logOpts)
	case "text", "":
		logHandler = slog.NewTextHandler(logOutput, &logOpts)
	default:
		return config, fmt.Errorf("unsupported logging type %q", config.Logging.Type)
	}

	slog.SetDefault(slog.New(logHandler))

	return config, nil
}

type HTTPConfig struct {
	Host              string        `yaml:"host"`
	Port              string        `yaml:"port"`
	Shutdown          time.Duration `yaml:"shutdown"`
	ReadHeaderTimeout time.Duration `yaml:"read_header_timeout"`
	IdleTimeout       time.Duration `yaml:"idle_timeout"`
	WriteTimeout      time.Duration `yaml:"write_timeout"`
	Driver            string        `yaml:"driver"`
}

func register(fs *flag.FlagSet) *string {
	return fs.String("config", "", "config path")
}

type LoggingConfig struct {
	Level  string `yaml:"level"`
	Type   string `yaml:"type"`
	Stderr bool   `yaml:"stderr"`
}

type SQLiteConfig struct {
	DatabaseName       string `yaml:"db_name"`
	BusyTimeout        int    `yaml:"busy_timeout"`
	MaxOpenConns       int    `yaml:"max_open_conns"`
	MaxIdleConns       int    `yaml:"max_idle_conns"`
	CacheSize          int    `yaml:"cache_size"`          // KB (negative = KB, positive = pages)
	MmapSize           int64  `yaml:"mmap_size"`           // Memory-mapped I/O size in bytes
	CheckpointInterval string `yaml:"checkpoint_interval"` // Default: 30s
	Durability         string `yaml:"durability"`          // strict (default) or balanced
}

type TursoConfig struct {
	URL string `yaml:"url"`
}

type PostgreConfig struct {
	Host         string `yaml:"host"`
	Username     string `yaml:"username"`
	Password     string `yaml:"password"`
	Name         string `yaml:"name"`
	Port         int    `yaml:"port"`
	Timezone     string `yaml:"timezone"`
	MaxOpenConns int    `yaml:"max_open_conns"`
	MaxIdleConns int    `yaml:"max_idle_conns"`
	SSLMode      string `yaml:"ssl_mode"`
	Durability   string `yaml:"durability"` // strict (default) or balanced
}

type WriterConfig struct {
	BatchSize          int    `yaml:"batch_size"`
	FlushInterval      string `yaml:"flush_interval"`
	MaxPendingMessages int64  `yaml:"max_pending_messages"`
	MaxPendingBytes    int64  `yaml:"max_pending_bytes"`
}

type MetricConfig struct {
	Enable bool `yaml:"enable"`
}

type MaintenanceConfig struct {
	ProcessedRetention   string `yaml:"processed_retention"`
	DeadLetterRetention  string `yaml:"dead_letter_retention"`
	ScheduleRunRetention string `yaml:"schedule_run_retention"`
}
