package etcd

import (
	"github.com/nutsdb/nutsdb"
)

type Etcd struct {
	db *nutsdb.DB
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
		db: db,
	}, nil
}

func (e *Etcd) Database() *nutsdb.DB {
	return e.db
}

func (e *Etcd) Close() error {
	return e.db.Close()
}
