// Package persistence owns BlockQueue's durable SQL state and backend dialect
// differences. The public blockqueue package remains the runtime/API facade;
// database drivers are supplied through the public store package.
package persistence
