package etcd

import (
	"github.com/nutsdb/nutsdb"
)

type Etcd struct {
	db *nutsdb.DB
}

type option struct {
	// sync represents if call Sync() function.
	// if SyncEnable is false, high write performance but potential data loss likely.
	// if SyncEnable is true, slower but persistent.
	sync bool
}

type opt func(*option)

func WithSync(sync bool) opt {
	return func(o *option) {
		o.sync = sync
	}
}

func New(dbName string, opts ...opt) (*Etcd, error) {
	opt := &option{
		sync: true,
	}

	for _, o := range opts {
		o(opt)
	}

	db, err := nutsdb.Open(
		nutsdb.DefaultOptions,
		nutsdb.WithDir(dbName),
		nutsdb.WithEntryIdxMode(nutsdb.HintKeyAndRAMIdxMode),
		nutsdb.WithSyncEnable(opt.sync),
		nutsdb.WithHintKeyAndRAMIdxCacheSize(0),
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
