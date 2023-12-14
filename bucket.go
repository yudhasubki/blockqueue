package blockqueue

import (
	"errors"
	"log/slog"

	"github.com/nutsdb/nutsdb"
	"github.com/yudhasubki/blockqueue/pkg/cas"
	"github.com/yudhasubki/blockqueue/pkg/etcd"
)

type kv struct {
	mtx *cas.SpinLock
	db  *etcd.Etcd
}

func NewKV(etcd *etcd.Etcd) *kv {
	return &kv{
		mtx: cas.New(),
		db:  etcd,
	}
}

func (e *kv) readBucketTx(fn func(tx *nutsdb.Tx) error) error {
	e.mtx.Lock()
	defer e.mtx.Unlock()

	return e.db.Database().View(func(tx *nutsdb.Tx) error {
		return fn(tx)
	})
}

func (e *kv) updateBucketTx(fn func(tx *nutsdb.Tx) error) error {
	e.mtx.Lock()
	defer e.mtx.Unlock()

	return e.db.Database().Update(func(tx *nutsdb.Tx) error {
		return fn(tx)
	})
}

func (bucket *kv) createTxBucket(tx *nutsdb.Tx, structure uint16, bucketName string) error {
	err := tx.NewBucket(structure, bucketName)
	if err != nil {
		if errors.Is(err, nutsdb.ErrBucketAlreadyExist) {
			slog.Debug(
				"bucket exist. skip create the bucket",
				logPrefixErr, err,
				logPrefixBucket, bucketName,
			)
			return nil
		}

		return err
	}

	return nil
}
