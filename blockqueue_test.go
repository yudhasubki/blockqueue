package blockqueue

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/yudhasubki/blockqueue/pkg/etcd"
	bqio "github.com/yudhasubki/blockqueue/pkg/io"
	"github.com/yudhasubki/blockqueue/pkg/sqlite"
)

const (
	persistenceDbName      = "blockqueuedb"
	persistenceDbNameShm   = "blockqueuedb-shm"
	persistenceDbNameWal   = "blockqueuedb-wal"
	persistenceBucketPath  = "blockqueuebucket"
	persistenceBusyTimeout = 5000
)

func runBlockQueueTest(t *testing.T, test func(bq *BlockQueue[chan bqio.ResponseMessages])) {
	sqlite, err := sqlite.New(persistenceDbName, sqlite.Config{
		BusyTimeout: persistenceBusyTimeout,
	})
	defer os.Remove(persistenceDbNameWal)
	defer os.Remove(persistenceDbNameShm)
	defer os.Remove(persistenceDbName)

	require.NoError(t, err)

	db := NewDb(sqlite)
	runMigrate(t, db.SQLite)

	bucket, err := etcd.New(persistenceBucketPath)
	defer os.RemoveAll(persistenceBucketPath)
	require.NoError(t, err)

	kv := NewKV(bucket)
	bq := New(db, kv)

	test(bq)

	t.Cleanup(func() {
		if bucket.Database().IsClose() {
			require.NoError(t, bucket.Database().Close())
		}

		sqlite.Database.Close()
	})
}

func runMigrate(t *testing.T, db *sqlite.SQLite) {
	_ = filepath.Walk("migration/", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		file, err := os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()

		var buf bytes.Buffer

		_, err = io.Copy(&buf, file)
		if err != nil {
			return err
		}

		_, err = db.Database.Exec(buf.String())
		if err != nil {
			slog.Error("failed migrate", "filename", path, "error", err)
			return err
		}
		slog.Info("successfully migrate", "filename", path)

		return nil
	})
}

func TestBlockQueueCreateJob(t *testing.T) {
	t.Run("success add new topic", func(t *testing.T) {
		runBlockQueueTest(t, func(bq *BlockQueue[chan bqio.ResponseMessages]) {
			var (
				ctx     = context.Background()
				request = bqio.Topic{
					Name: getRandomChar(1),
					Subscribers: bqio.Subscribers{
						{
							Name: getRandomChar(2),
						},
					},
				}
				topic = request.Topic()
			)

			bq.Run(ctx)
			testAddJob(t, ctx, bq, topic, request.Subscriber(topic.Id), nil)
		})
	})
}

func TestBlockQueueDeleteJob(t *testing.T) {
	t.Run("success delete topic", func(t *testing.T) {
		runBlockQueueTest(t, func(bq *BlockQueue[chan bqio.ResponseMessages]) {
			var (
				ctx     = context.Background()
				request = bqio.Topic{
					Name: getRandomChar(1),
					Subscribers: bqio.Subscribers{
						{
							Name: getRandomChar(2),
						},
					},
				}
				topic = request.Topic()
			)
			bq.Run(ctx)
			testAddJob(t, ctx, bq, topic, request.Subscriber(topic.Id), nil)
			testDeleteJob(t, ctx, bq, topic, nil)
			testPublish(t, ctx, bq, topic, bqio.Publish{
				Message: getRandomChar(1),
			}, ErrJobNotFound)
		})
	})

	t.Run("topic not found", func(t *testing.T) {
		runBlockQueueTest(t, func(bq *BlockQueue[chan bqio.ResponseMessages]) {
			var (
				ctx     = context.Background()
				request = bqio.Topic{
					Name: getRandomChar(1),
					Subscribers: bqio.Subscribers{
						{
							Name: getRandomChar(2),
						},
					},
				}
				topic = request.Topic()
			)
			bq.Run(ctx)
			testDeleteJob(t, ctx, bq, topic, ErrJobNotFound)
		})
	})
}

func TestBlockQueuePublishAndRead(t *testing.T) {
	t.Run("success publish, read, and ack", func(t *testing.T) {
		runBlockQueueTest(t, func(bq *BlockQueue[chan bqio.ResponseMessages]) {
			var (
				ctx     = context.Background()
				request = bqio.Topic{
					Name: getRandomChar(1),
					Subscribers: bqio.Subscribers{
						{
							Name: getRandomChar(2),
						},
					},
				}
				topic       = request.Topic()
				subscribers = request.Subscriber(topic.Id)
			)
			bq.Run(ctx)

			testAddJob(t, ctx, bq, topic, subscribers, nil)
			testPublish(t, ctx, bq, topic, bqio.Publish{
				Message: getRandomChar(3),
			}, ErrJobNotFound)
			response := testReadSubscriberMessage(t, ctx, bq, topic, getRandomChar(2), bqio.ResponseMessages{
				{
					Message: getRandomChar(3),
				},
			}, nil)
			testAckMessage(t, ctx, bq, topic, getRandomChar(2), response.Id, nil)
		})
	})

	t.Run("read message but listener deleted", func(t *testing.T) {
		runBlockQueueTest(t, func(bq *BlockQueue[chan bqio.ResponseMessages]) {
			var (
				ctx     = context.Background()
				request = bqio.Topic{
					Name: getRandomChar(1),
					Subscribers: bqio.Subscribers{
						{
							Name: getRandomChar(2),
						},
					},
				}
				topic       = request.Topic()
				subscribers = request.Subscriber(topic.Id)
			)
			bq.Run(ctx)

			testAddJob(t, ctx, bq, topic, subscribers, nil)
			go testReadSubscriberMessage(t, ctx, bq, topic, getRandomChar(2), nil, ErrListenerDeleted)
			time.Sleep(2 * time.Second)
			testDeleteSubscriber(t, ctx, bq, topic, getRandomChar(2), nil)
			time.Sleep(3 * time.Second)
		})
	})

	t.Run("read message but timeout or no message available", func(t *testing.T) {
		runBlockQueueTest(t, func(bq *BlockQueue[chan bqio.ResponseMessages]) {
			var (
				serverCtx          = context.Background()
				requestCtx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
				request            = bqio.Topic{
					Name: getRandomChar(1),
					Subscribers: bqio.Subscribers{
						{
							Name: getRandomChar(2),
						},
					},
				}
				topic       = request.Topic()
				subscribers = request.Subscriber(topic.Id)
			)
			bq.Run(serverCtx)

			testAddJob(t, requestCtx, bq, topic, subscribers, nil)
			go testReadSubscriberMessage(t, requestCtx, bq, topic, getRandomChar(2), nil, nil)
			time.Sleep(2 * time.Second)
			cancel()
			time.Sleep(3 * time.Second)
		})
	})

	t.Run("topic not found when publish a message", func(t *testing.T) {
		runBlockQueueTest(t, func(bq *BlockQueue[chan bqio.ResponseMessages]) {
			var (
				ctx     = context.Background()
				request = bqio.Topic{
					Name: getRandomChar(1),
					Subscribers: bqio.Subscribers{
						{
							Name: getRandomChar(2),
						},
					},
				}
				topic = request.Topic()
				// subscribers = request.Subscriber(topic.Id)
			)
			bq.Run(ctx)
			testPublish(t, ctx, bq, topic, bqio.Publish{
				Message: "test",
			}, ErrJobNotFound)
		})
	})
}

func TestBlockQueueCreateSubscriber(t *testing.T) {
	t.Run("success create subscriber", func(t *testing.T) {
		runBlockQueueTest(t, func(bq *BlockQueue[chan bqio.ResponseMessages]) {
			var (
				serverCtx = context.Background()
				request   = bqio.Topic{
					Name: getRandomChar(1),
					Subscribers: bqio.Subscribers{
						{
							Name: getRandomChar(2),
						},
					},
				}
				topic       = request.Topic()
				subscribers = request.Subscriber(topic.Id)
			)
			bq.Run(serverCtx)

			requestSubscriber := bqio.Subscribers{
				{
					Name: getRandomChar(3),
				},
			}

			testAddJob(t, serverCtx, bq, topic, subscribers, nil)
			testAddSubscriber(t, serverCtx, bq, topic, requestSubscriber.Subscriber(topic.Id), nil)
		})
	})

	t.Run("success create subscriber, publish, and read message", func(t *testing.T) {
		runBlockQueueTest(t, func(bq *BlockQueue[chan bqio.ResponseMessages]) {
			var (
				serverCtx = context.Background()
				request   = bqio.Topic{
					Name: getRandomChar(1),
					Subscribers: bqio.Subscribers{
						{
							Name: getRandomChar(2),
						},
					},
				}
				topic       = request.Topic()
				subscribers = request.Subscriber(topic.Id)
			)
			bq.Run(serverCtx)

			requestSubscriber := bqio.Subscribers{
				{
					Name: getRandomChar(3),
				},
			}

			testAddJob(t, serverCtx, bq, topic, subscribers, nil)
			testAddSubscriber(t, serverCtx, bq, topic, requestSubscriber.Subscriber(topic.Id), nil)
			testPublish(t, serverCtx, bq, topic, bqio.Publish{
				Message: "test 2",
			}, nil)
			go testReadSubscriberMessage(t, serverCtx, bq, topic, getRandomChar(3), bqio.ResponseMessages{{
				Message: "test 2",
			}}, nil)

			time.Sleep(3 * time.Second)
		})
	})

	t.Run("failed create subscriber job not found", func(t *testing.T) {
		runBlockQueueTest(t, func(bq *BlockQueue[chan bqio.ResponseMessages]) {
			var (
				serverCtx = context.Background()
				request   = bqio.Topic{
					Name: getRandomChar(1),
				}
				topic = request.Topic()
			)
			bq.Run(serverCtx)

			requestSubscriber := bqio.Subscribers{
				{
					Name: getRandomChar(3),
				},
			}

			testAddSubscriber(t, serverCtx, bq, topic, requestSubscriber.Subscriber(topic.Id), ErrJobNotFound)
		})
	})
}

func getRandomChar(i int) string {
	return fmt.Sprintf("blockqueue-%09d", i)
}
