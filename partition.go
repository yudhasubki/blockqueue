package blockqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/nutsdb/nutsdb"
	"github.com/yudhasubki/queuestream/pkg/bucket"
)

type Partition struct {
	Id         string
	JobId      string
	ListenerId string

	mtx *sync.Mutex
}

func NewPartition(id string, jobId, listenerId string) *Partition {
	partition := &Partition{
		Id:         id,
		JobId:      jobId,
		ListenerId: listenerId,
		mtx:        new(sync.Mutex),
	}

	return partition
}

func (partition *Partition) createBucket() error {
	partition.mtx.Lock()
	defer partition.mtx.Unlock()

	return BucketTx(func(tx *nutsdb.Tx) error {
		return CreateTxBucket(tx, partition.Bucket())
	})
}

func (p *Partition) Bucket() string {
	return fmt.Sprintf("%s:%s:%s", p.JobId, p.ListenerId, p.Id)
}

func (partition *Partition) delete() error {
	partition.mtx.Lock()
	defer partition.mtx.Unlock()

	return DeleteBuckets(partition.Bucket())
}

func (partition *Partition) storeTxBucketMessage(tx *nutsdb.Tx, messages bucket.Messages) error {
	partition.mtx.Lock()
	defer partition.mtx.Unlock()

	for _, message := range messages {
		b, err := json.Marshal(message)
		if err != nil {
			return err
		}

		tx.NewBucket(nutsdb.DataStructureBTree, partition.Bucket())
		err = tx.Put(partition.Bucket(), []byte(message.Id), b, nutsdb.Persistent)
		if err != nil {
			return err
		}
	}

	return nil
}

func (partition *Partition) deleteMessage(messageId string) error {
	partition.mtx.Lock()
	defer partition.mtx.Unlock()

	return BucketTx(func(tx *nutsdb.Tx) error {
		return tx.Delete(partition.Bucket(), []byte(messageId))
	})
}

func (partition *Partition) read(ctx context.Context) (bucket.Messages, error) {
	partition.mtx.Lock()
	defer partition.mtx.Unlock()

	messages := make(bucket.Messages, 0)

	err := GetBucketMessage(partition.Bucket(), func(position int, val []byte) error {
		message := bucket.Message{}
		err := json.Unmarshal(val, &message)
		if err != nil {
			return err
		}

		messages = append(messages, message)

		return nil
	})
	if err != nil {
		return messages, err
	}

	return messages, nil
}
