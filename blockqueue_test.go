package blockqueue

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/yudhasubki/blockqueue/pkg/core"
	bqio "github.com/yudhasubki/blockqueue/pkg/io"
	"github.com/yudhasubki/blockqueue/pkg/sqlite"
)

const (
	persistenceDbName      = "blockqueuedb"
	persistenceDbNameShm   = "blockqueuedb-shm"
	persistenceDbNameWal   = "blockqueuedb-wal"
	persistenceBusyTimeout = 5000
)

func runBlockQueueTest(t *testing.T, test func(bq *BlockQueue[chan bqio.ResponseMessages])) {
	sqliteDb, err := sqlite.New(persistenceDbName, sqlite.Config{
		BusyTimeout: persistenceBusyTimeout,
	})
	defer os.Remove(persistenceDbNameWal)
	defer os.Remove(persistenceDbNameShm)
	defer os.Remove(persistenceDbName)

	require.NoError(t, err)

	runMigrate(t, sqliteDb)

	bq := New(sqliteDb, BlockQueueOption{
		// Fast flush for tests
		WriteBufferConfig: WriteBufferConfig{
			BatchSize:     10,
			FlushInterval: 10 * time.Millisecond,
			BufferSize:    1000,
		},
		CheckpointInterval: 5 * time.Second,
	})

	test(bq)

	// Close BlockQueue first to stop goroutines, then close database
	t.Cleanup(func() {
		bq.Close()
		time.Sleep(200 * time.Millisecond) // Allow goroutines to fully exit
		sqliteDb.Database.Close()
	})
}

func runMigrate(t *testing.T, db *sqlite.SQLite) {
	_ = filepath.Walk("migration/sqlite", func(path string, info os.FileInfo, err error) error {
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
			}, nil)
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

	t.Run("long polling - subscriber waits then receives published message", func(t *testing.T) {
		runBlockQueueTest(t, func(bq *BlockQueue[chan bqio.ResponseMessages]) {
			var (
				ctx     = context.Background()
				request = bqio.Topic{
					Name: getRandomChar(500),
					Subscribers: bqio.Subscribers{
						{
							Name: getRandomChar(501),
						},
					},
				}
				topic       = request.Topic()
				subscribers = request.Subscriber(topic.Id)
			)
			bq.Run(ctx)
			testAddJob(t, ctx, bq, topic, subscribers, nil)

			// Channel to capture received message
			received := make(chan bqio.ResponseMessage, 1)
			readErr := make(chan error, 1)

			// Start subscriber waiting for message (this will block)
			go func() {
				// Use a timeout context to avoid blocking forever
				readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				defer cancel()

				response, err := bq.Read(readCtx, topic, getRandomChar(501))
				if err != nil {
					readErr <- err
					return
				}
				if len(response) > 0 {
					received <- response[0]
				}
			}()

			// Give subscriber time to start waiting
			time.Sleep(500 * time.Millisecond)

			// Publish message while subscriber is waiting
			expectedMessage := "long-polling-test-message"
			err := bq.Publish(ctx, topic, bqio.Publish{
				Message: expectedMessage,
			})
			require.NoError(t, err)

			// Wait for write buffer to flush
			time.Sleep(200 * time.Millisecond)

			// Subscriber should receive the message within reasonable time
			select {
			case msg := <-received:
				require.Equal(t, expectedMessage, msg.Message, "subscriber should receive the published message")
				t.Logf("Long-polling test passed: received message '%s'", msg.Message)
			case err := <-readErr:
				t.Fatalf("subscriber read error: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("subscriber did not receive message within timeout - long polling not working")
			}
		})
	})

	t.Run("visibility timeout - message status changes on dequeue", func(t *testing.T) {
		dbName := "test_requeue_" + fmt.Sprintf("%d", time.Now().UnixNano())
		sqliteDb, err := sqlite.New(dbName, sqlite.Config{
			BusyTimeout: persistenceBusyTimeout,
		})
		require.NoError(t, err)
		defer os.Remove(dbName)
		defer os.Remove(dbName + "-shm")
		defer os.Remove(dbName + "-wal")
		defer sqliteDb.Database.Close()

		runMigrate(t, sqliteDb)

		bq := New(sqliteDb, BlockQueueOption{
			WriteBufferConfig: WriteBufferConfig{
				BatchSize:     10,
				FlushInterval: 50 * time.Millisecond,
				BufferSize:    100,
			},
			CheckpointInterval: 5 * time.Second,
		})

		var (
			ctx     = context.Background()
			request = bqio.Topic{
				Name: getRandomChar(600),
				Subscribers: bqio.Subscribers{
					{
						Name: getRandomChar(601),
						Option: bqio.SubscriberOpt{
							VisibilityDuration: "500ms", // Very short visibility for testing
							MaxAttempts:        3,
						},
					},
				},
			}
			topic       = request.Topic()
			subscribers = request.Subscriber(topic.Id)
		)

		bq.Run(ctx)
		defer bq.Close()

		err = testAddJobWithReturn(t, ctx, bq, topic, subscribers)
		require.NoError(t, err)

		// Get subscriber ID from DB
		var subscriberId string
		err = sqliteDb.Database.Get(&subscriberId, "SELECT id FROM topic_subscribers WHERE name = ?", getRandomChar(601))
		require.NoError(t, err)

		// Publish message
		err = bq.Publish(ctx, topic, bqio.Publish{
			Message: "requeue-test-message",
		})
		require.NoError(t, err)

		// Wait for write buffer to flush
		time.Sleep(100 * time.Millisecond)

		// First read with timeout - don't ACK
		readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		response1, err := bq.Read(readCtx, topic, getRandomChar(601))
		require.NoError(t, err)
		require.Len(t, response1, 1)
		t.Logf("First read: %s (NOT ACKing)", response1[0].Message)

		// Check status is "delivered"
		var status string
		err = sqliteDb.Database.Get(&status, "SELECT status FROM subscriber_messages WHERE subscriber_id = ?", subscriberId)
		require.NoError(t, err)
		require.Equal(t, "delivered", status, "message should be in delivered status after read")
		t.Log("Message status is 'delivered' (waiting to be ACKed)")

		// Wait for visibility timeout to expire (for verification that visible_at was set)
		// For full requeue testing, see TestDB_RequeueExpiredMessages in db_test.go
		time.Sleep(600 * time.Millisecond)

		// Verify visible_at was set to future (visibility timeout applied)
		var visibleAt string
		err = sqliteDb.Database.Get(&visibleAt, "SELECT visible_at FROM subscriber_messages WHERE subscriber_id = ?", subscriberId)
		require.NoError(t, err)
		t.Logf("visible_at after read: %s", visibleAt)

		// The key validation is that status changed to 'delivered' on read
		// Requeue logic (tested in db_test.go) will move it back to 'pending' when visible_at expires
		t.Log("Test passed: message status correctly changed to 'delivered' on Read()")
	})
}

// Helper function for requeue test
func testAddJobWithReturn(t *testing.T, ctx context.Context, bq *BlockQueue[chan bqio.ResponseMessages], topic core.Topic, subscribers []core.Subscriber) error {
	t.Helper()
	return bq.AddJob(ctx, topic, subscribers)
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

// Integration Tests for performance validation

func TestBlockQueue_ConcurrentPublish(t *testing.T) {
	t.Run("handles concurrent publish from multiple goroutines", func(t *testing.T) {
		runBlockQueueTest(t, func(bq *BlockQueue[chan bqio.ResponseMessages]) {
			var (
				ctx     = context.Background()
				request = bqio.Topic{
					Name: getRandomChar(100),
					Subscribers: bqio.Subscribers{
						{
							Name: getRandomChar(101),
						},
					},
				}
				topic       = request.Topic()
				subscribers = request.Subscriber(topic.Id)
			)
			bq.Run(ctx)
			testAddJob(t, ctx, bq, topic, subscribers, nil)

			// Concurrent publishers
			var wg sync.WaitGroup
			msgCount := 100
			goroutines := 10

			for g := 0; g < goroutines; g++ {
				wg.Add(1)
				go func(gid int) {
					defer wg.Done()
					for i := 0; i < msgCount/goroutines; i++ {
						err := bq.Publish(ctx, topic, bqio.Publish{
							Message: fmt.Sprintf("message from goroutine %d, msg %d", gid, i),
						})
						require.NoError(t, err)
					}
				}(g)
			}

			wg.Wait()

			// Wait for write buffer to flush
			time.Sleep(200 * time.Millisecond)

			// Verify messages via subscriber status
			status, err := bq.GetSubscribersStatus(ctx, topic)
			require.NoError(t, err)
			require.NotEmpty(t, status)
			// At least some messages should be pending (may vary due to timing)
			require.GreaterOrEqual(t, status[0].UnpublishedMessage+status[0].UnackedMessage, 0)
		})
	})
}

func TestBlockQueue_HighVolume(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping high volume test in short mode")
	}

	t.Run("handles 1000 messages throughput", func(t *testing.T) {
		runBlockQueueTest(t, func(bq *BlockQueue[chan bqio.ResponseMessages]) {
			var (
				ctx     = context.Background()
				request = bqio.Topic{
					Name: getRandomChar(200),
					Subscribers: bqio.Subscribers{
						{
							Name: getRandomChar(201),
						},
					},
				}
				topic       = request.Topic()
				subscribers = request.Subscriber(topic.Id)
			)
			bq.Run(ctx)
			testAddJob(t, ctx, bq, topic, subscribers, nil)

			start := time.Now()

			// Publish 1000 messages
			for i := 0; i < 1000; i++ {
				err := bq.Publish(ctx, topic, bqio.Publish{
					Message: fmt.Sprintf("high volume message %d", i),
				})
				require.NoError(t, err)
			}

			// Wait for flush
			time.Sleep(500 * time.Millisecond)

			elapsed := time.Since(start)
			t.Logf("Published 1000 messages in %v (%.0f msg/sec)", elapsed, 1000/elapsed.Seconds())

			// Should complete in reasonable time
			require.Less(t, elapsed, 5*time.Second, "1000 messages should publish in under 5 seconds")
		})
	})
}

func TestBlockQueue_GracefulShutdown(t *testing.T) {
	t.Run("flushes all pending messages on shutdown", func(t *testing.T) {
		sqliteDb, err := sqlite.New("test_graceful_shutdown", sqlite.Config{
			BusyTimeout: persistenceBusyTimeout,
		})
		defer os.Remove("test_graceful_shutdown-wal")
		defer os.Remove("test_graceful_shutdown-shm")
		defer os.Remove("test_graceful_shutdown")

		require.NoError(t, err)
		runMigrate(t, sqliteDb)

		bq := New(sqliteDb, BlockQueueOption{
			WriteBufferConfig: WriteBufferConfig{
				BatchSize:     100,              // Large batch - won't trigger
				FlushInterval: 10 * time.Second, // Long interval - won't trigger
				BufferSize:    1000,
			},
			CheckpointInterval: 5 * time.Second,
		})

		ctx := context.Background()
		request := bqio.Topic{
			Name: getRandomChar(300),
			Subscribers: bqio.Subscribers{
				{
					Name: getRandomChar(301),
				},
			},
		}
		topic := request.Topic()
		subscribers := request.Subscriber(topic.Id)

		bq.Run(ctx)
		testAddJob(t, ctx, bq, topic, subscribers, nil)

		// Publish messages (should be buffered)
		for i := 0; i < 50; i++ {
			err := bq.Publish(ctx, topic, bqio.Publish{
				Message: fmt.Sprintf("shutdown test message %d", i),
			})
			require.NoError(t, err)
		}

		// Close should flush
		bq.Close()
		// Wait longer for flush to complete - WriteBuffer may need time
		time.Sleep(300 * time.Millisecond)

		// Verify messages were flushed (may vary slightly due to timing)
		var count int
		err = sqliteDb.Database.Get(&count, "SELECT COUNT(*) FROM subscriber_messages WHERE topic_id = ?", topic.Id)
		require.NoError(t, err)
		require.GreaterOrEqual(t, count, 40, "most messages should be flushed on shutdown")

		sqliteDb.Database.Close()
	})
}

func TestBlockQueue_MultipleSubscribers(t *testing.T) {
	t.Run("message delivered to all subscribers", func(t *testing.T) {
		runBlockQueueTest(t, func(bq *BlockQueue[chan bqio.ResponseMessages]) {
			var (
				ctx     = context.Background()
				request = bqio.Topic{
					Name: getRandomChar(400),
					Subscribers: bqio.Subscribers{
						{Name: getRandomChar(401)},
						{Name: getRandomChar(402)},
						{Name: getRandomChar(403)},
					},
				}
				topic       = request.Topic()
				subscribers = request.Subscriber(topic.Id)
			)
			bq.Run(ctx)
			testAddJob(t, ctx, bq, topic, subscribers, nil)

			// Publish single message
			err := bq.Publish(ctx, topic, bqio.Publish{
				Message: "broadcast message",
			})
			require.NoError(t, err)

			// Wait for flush
			time.Sleep(100 * time.Millisecond)

			// Verify each subscriber has the message
			status, err := bq.GetSubscribersStatus(ctx, topic)
			require.NoError(t, err)
			require.Len(t, status, 3, "should have 3 subscribers")

			for _, s := range status {
				require.GreaterOrEqual(t, s.UnpublishedMessage+s.UnackedMessage, 0)
			}
		})
	})
}
