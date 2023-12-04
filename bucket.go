package blockqueue

import (
	"errors"
	"log/slog"

	"github.com/nutsdb/nutsdb"
	"github.com/yudhasubki/queuestream/pkg/etcd"
)

var Etcd *etcd.Etcd

func BucketTx(fn func(tx *nutsdb.Tx) error) error {
	return Etcd.Database.Update(func(tx *nutsdb.Tx) error {
		return fn(tx)
	})
}

func CreateTxBucket(tx *nutsdb.Tx, bucketName string) error {
	err := tx.NewBucket(nutsdb.DataStructureBTree, bucketName)
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

func DeleteMessageBucket(bucket, key string) error {
	return Etcd.Database.Update(func(tx *nutsdb.Tx) error {
		return tx.Delete(bucket, []byte(key))
	})
}

func CountBucketMessage(bucket string) (int, error) {
	total := 0
	err := Etcd.Database.View(func(tx *nutsdb.Tx) error {
		entries, err := tx.GetAll(bucket)
		if err != nil {
			if errors.Is(err, nutsdb.ErrBucketEmpty) {
				return nil
			}
			return err
		}

		total = len(entries)

		return nil
	})
	if err != nil {
		return 0, err
	}

	return total, nil
}

func GetBucketMessage(bucket string, fn func(position int, val []byte) error) error {
	return Etcd.Database.View(func(tx *nutsdb.Tx) error {
		entries, err := tx.GetAll(bucket)
		if err != nil {
			if errors.Is(err, nutsdb.ErrBucketEmpty) {
				return nil
			}

			return err
		}

		if len(entries) == 0 {
			return nil
		}

		for pos, entry := range entries {
			err = fn(pos, entry)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

func DeleteBuckets(buckets ...string) error {
	return Etcd.Database.Update(func(tx *nutsdb.Tx) error {
		for _, bucket := range buckets {
			err := tx.DeleteBucket(nutsdb.DataStructureBTree, bucket)
			if err != nil {
				if errors.Is(err, nutsdb.ErrNotFoundBucket) {
					return nil
				}
				return err
			}
		}

		return nil
	})
}
