package main

const (
	commandHTTP    = "http"
	commandMigrate = "migrate"
	commandHelp    = "help"
)

const (
	storageDriverSQLite     = "sqlite"
	storageDriverTurso      = "turso"
	storageDriverPostgres   = "postgres"
	storageDriverPostgreSQL = "postgresql"
	storageDriverPGSQL      = "pgsql"
)

const (
	logLevelDebug   = "DEBUG"
	logLevelWarn    = "WARN"
	logLevelWarning = "WARNING"
	logLevelError   = "ERROR"
	logFormatJSON   = "json"
	logFormatText   = "text"
)

const (
	defaultHTTPHost = "127.0.0.1"
	defaultHTTPPort = "8080"
)
