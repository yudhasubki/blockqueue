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
	shutdown       = make(chan os.Signal, 1)
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
		return (&Http{}).Run(ctx, args)
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
blockqueue is a tool for send a jobs with consumer groups

Usage:

	blockqueue <command> [arguments]

The commands are:

	http    	running blockqueue with http-based
	migrate 	running migration blockqueue
`[1:])
}

type Config struct {
	Etcd    EtcdConfig    `yaml:"etcd"`
	Http    HttpConfig    `yaml:"http"`
	Logging LoggingConfig `yaml:"logging"`
	SQLite  SQLiteConfig  `yaml:"sqlite"`
	Turso   TursoConfig   `yaml:"turso"`
	PgSQL   PostgreConfig `yaml:"pgsql"`
	Job     JobConfig     `yaml:"job"`
	Metric  MetricConfig  `yaml:"metric"`
}

func ReadConfigFile(filename string) (_ Config, err error) {
	var config Config
	b, err := os.ReadFile(filename)
	if err != nil {
		return config, err
	}

	err = yaml.Unmarshal(b, &config)
	if err != nil {
		return config, err
	}

	if config.Http.Shutdown.Seconds() == 0 {
		config.Http.Shutdown = 30 * time.Second
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
	}

	slog.SetDefault(slog.New(logHandler))

	return config, nil
}

type HttpConfig struct {
	Port     string        `yaml:"port"`
	Shutdown time.Duration `yaml:"shutdown"`
	Driver   string        `yaml:"driver"`
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
	DatabaseName string `yaml:"db_name"`
	BusyTimeout  int    `yaml:"busy_timeout"`
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
}
type EtcdConfig struct {
	Path string `yaml:"path"`
	Sync bool   `yaml:"sync"`
}

type JobConfig struct {
	ProducerPartition int `yaml:"producer_partition"`
	ConsumerPartition int `yaml:"consumer_partition"`
}

type MetricConfig struct {
	Enable bool `yaml:"enable"`
}
