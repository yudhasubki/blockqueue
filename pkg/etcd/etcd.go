package etcd

import (
	"github.com/nutsdb/nutsdb"
)

type Etcd struct {
	Database *nutsdb.DB
}

func New(dbName string) (*Etcd, error) {
	db, err := nutsdb.Open(
		nutsdb.DefaultOptions,
		nutsdb.WithDir(dbName),
		nutsdb.WithEntryIdxMode(nutsdb.HintKeyAndRAMIdxMode),
	)
	if err != nil {
		return &Etcd{}, err
	}

	return &Etcd{
		Database: db,
	}, nil
}

func (e *Etcd) Close() error {
	return e.Database.Close()
}
