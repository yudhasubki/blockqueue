package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReadConfigFileExpandsEnvironmentAndAppliesDefaults(t *testing.T) {
	t.Setenv("BLOCKQUEUE_CONFIG_PASSWORD", "local-secret")
	path := filepath.Join(t.TempDir(), "config.yaml")
	contents := []byte(`
http:
  port: "8090"
logging:
  type: text
pgsql:
  password: "${BLOCKQUEUE_CONFIG_PASSWORD}"
`)
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}

	config, err := ReadConfigFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if config.PgSQL.Password != "local-secret" {
		t.Fatalf("password = %q", config.PgSQL.Password)
	}
	if config.Http.Driver != "sqlite" {
		t.Fatalf("driver = %q", config.Http.Driver)
	}
	if config.Http.Shutdown != 30*time.Second {
		t.Fatalf("shutdown = %s", config.Http.Shutdown)
	}
	if config.Http.Host != "127.0.0.1" || config.Http.Port != "8090" {
		t.Fatalf("listen address = %s:%s", config.Http.Host, config.Http.Port)
	}
	if config.Http.ReadHeaderTimeout != 5*time.Second || config.Http.IdleTimeout != 2*time.Minute ||
		config.Http.WriteTimeout != 65*time.Second {
		t.Fatalf("timeouts = read-header:%s idle:%s write:%s",
			config.Http.ReadHeaderTimeout, config.Http.IdleTimeout, config.Http.WriteTimeout)
	}
}

func TestReadConfigFileRejectsUnknownFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("http:\n  typo: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadConfigFile(path); err == nil {
		t.Fatal("expected unknown config field to fail")
	}
}

func TestConfiguredCheckpointIntervalIsSQLiteOnly(t *testing.T) {
	config := Config{Http: HTTPConfig{Driver: "sqlite"}, SQLite: SQLiteConfig{CheckpointInterval: "45s"}}
	interval, err := configuredCheckpointInterval(config)
	if err != nil {
		t.Fatal(err)
	}
	if interval != 45*time.Second {
		t.Fatalf("interval = %s", interval)
	}

	config.Http.Driver = "pgsql"
	interval, err = configuredCheckpointInterval(config)
	if err != nil || interval != 0 {
		t.Fatalf("postgres interval = %s, err = %v", interval, err)
	}
}

func TestExampleConfigurationsParse(t *testing.T) {
	paths := []string{
		filepath.Join("..", "..", "config.yaml.example"),
		filepath.Join("..", "..", "benchmark", "config-sqlite.yaml"),
		filepath.Join("..", "..", "benchmark", "config-single.yaml"),
		filepath.Join("..", "..", "benchmark", "config-postgres.yaml.example"),
	}
	for _, path := range paths {
		path := path
		t.Run(filepath.Base(path), func(t *testing.T) {
			if _, err := ReadConfigFile(path); err != nil {
				t.Fatal(err)
			}
		})
	}
}
