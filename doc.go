// Package blockqueue provides a durable, embeddable, at-least-once message
// queue for Go applications. A Queue stores one canonical message and one
// receipt-fenced delivery per subscriber, supporting fan-out, retries, delayed
// delivery, recurring schedules, cancellation, and dead-letter queues.
//
// SQLite is suited to single-process deployments and PostgreSQL supports
// multi-process consumers. Publish and delivery completion can participate in
// caller-owned database transactions when application tables use the same
// database. The optional worker and httpapi packages provide a managed Go
// consumer runtime and a cross-language HTTP surface without changing the
// storage contract.
//
// Delivery is at-least-once: handlers must make external side effects
// idempotent or commit them atomically with AckDeliveryTx.
package blockqueue
