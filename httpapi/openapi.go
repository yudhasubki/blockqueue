package httpapi

import (
	_ "embed"
	"net/http"
)

//go:embed openapi.json
var openAPIDocument []byte

func serveOpenAPI(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/vnd.oai.openapi+json;version=3.1")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(openAPIDocument)
}
