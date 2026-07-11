package blockqueue

import (
	"embed"
	"io/fs"
)

//go:embed ui/*
var dashboardFiles embed.FS

// DashboardFS returns the embedded optional HTTP dashboard assets.
func DashboardFS() fs.FS {
	dashboard, err := fs.Sub(dashboardFiles, "ui")
	if err != nil {
		panic(err)
	}
	return dashboard
}
