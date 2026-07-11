package postgres

import (
	"net/url"
	"strings"
	"testing"

	"github.com/yudhasubki/blockqueue/store"
)

func TestConnectionURLDefaultsAndEscapesCredentials(t *testing.T) {
	const password = "p@ss:/?# word"
	connectionURL, err := buildConnectionURL(Config{
		Host: "localhost", Port: 5432, Username: "queue user", Password: password, Name: "queue/db",
	})
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(connectionURL)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.User.Username() != "queue user" {
		t.Fatalf("username=%q", parsed.User.Username())
	}
	decodedPassword, ok := parsed.User.Password()
	if !ok || decodedPassword != password {
		t.Fatalf("password did not round-trip")
	}
	if strings.Contains(connectionURL, password) {
		t.Fatal("connection URL contains an unescaped password")
	}
	if parsed.Query().Get("sslmode") != "require" {
		t.Fatalf("sslmode=%q", parsed.Query().Get("sslmode"))
	}
	if parsed.Query().Get("options") != "-c synchronous_commit=on" {
		t.Fatalf("options=%q", parsed.Query().Get("options"))
	}
}

func TestBalancedDurabilityMustBeExplicit(t *testing.T) {
	connectionURL, err := buildConnectionURL(Config{Durability: store.DurabilityBalanced})
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(connectionURL)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Query().Get("options") != "-c synchronous_commit=local" {
		t.Fatalf("options=%q", parsed.Query().Get("options"))
	}
	if _, err := buildConnectionURL(Config{Durability: "unsafe"}); err == nil {
		t.Fatal("invalid durability mode was accepted")
	}
}

func TestConnectionURLRejectsInvalidSSLMode(t *testing.T) {
	if _, err := buildConnectionURL(Config{SSLMode: "trust-me"}); err == nil {
		t.Fatal("expected invalid SSL mode to fail")
	}
}

func TestConnectionURLSupportsIPv6Host(t *testing.T) {
	connectionURL, err := buildConnectionURL(Config{Host: "::1", Port: 5432, Name: "queue"})
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(connectionURL)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Hostname() != "::1" || parsed.Port() != "5432" {
		t.Fatalf("host=%q port=%q", parsed.Hostname(), parsed.Port())
	}
}

func TestOpenRejectsPoolTooSmallForMaintenanceLeadership(t *testing.T) {
	_, err := Open(Config{
		Host: "localhost", Username: "queue", Name: "queue", MaxOpenConns: 1,
	})
	if err == nil || !strings.Contains(err.Error(), "at least 2") {
		t.Fatalf("expected maintenance pool capacity error, got %v", err)
	}
}
