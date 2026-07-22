// Package httpapi exposes BlockQueue as a versioned, cross-language HTTP API.
// It provides strict JSON decoding, RFC 9457 errors, bounded cursor pagination,
// health endpoints, OpenAPI 3.1, and optional authentication middleware.
// Transactional queue methods intentionally remain Go-only because a remote
// request cannot join the caller's database transaction.
package httpapi
