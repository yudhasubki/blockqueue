package blockqueue

import (
	"errors"
	"log/slog"

	"github.com/nutsdb/nutsdb"
	"github.com/yudhasubki/blockqueue/pkg/etcd"
)

var Etcd *etcd.Etcd

func UpdateBucketTx(fn func(tx *nutsdb.Tx) error) error {
	return Etcd.Database.Update(func(tx *nutsdb.Tx) error {
		return fn(tx)
	})
}

func ReadBucketTx(fn func(tx *nutsdb.Tx) error) error {
	return Etcd.Database.View(func(tx *nutsdb.Tx) error {
		return fn(tx)
	})
}

func CreateTxBucket(tx *nutsdb.Tx, structure uint16, bucketName string) error {
	err := tx.NewBucket(structure, bucketName)
	if err != nil {
		if errors.Is(err, nutsdb.ErrBucketAlreadyExist) {
			slog.Debug(
				"bucket exist. skip create the bucket",
				LogPrefixErr, err,
				LogPrefixBucket, bucketName,
			)
			return nil
		}

		return err
	}

	return nil
}
