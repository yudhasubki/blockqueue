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
