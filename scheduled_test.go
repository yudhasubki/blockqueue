package blockqueue

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	bqio "github.com/yudhasubki/blockqueue/pkg/io"
)

func TestBlockQueue_PublishDelayed(t *testing.T) {
	t.Run("message visible only after delay", func(t *testing.T) {
		runBlockQueueTest(t, func(bq *BlockQueue[chan bqio.ResponseMessages]) {
			var (
				ctx     = context.Background()
				request = bqio.Topic{
					Name: getRandomChar(700),
					Subscribers: bqio.Subscribers{
						{
							Name: getRandomChar(701),
						},
					},
				}
				topic       = request.Topic()
				subscribers = request.Subscriber(topic.Id)
			)
			bq.Run(ctx)
			testAddJob(t, ctx, bq, topic, subscribers, nil)

			// Publish with 2s delay
			err := bq.Publish(ctx, topic, bqio.Publish{
				Message: "delayed message",
				Delay:   "2s",
			})
			require.NoError(t, err)

			// Wait for flush
			time.Sleep(100 * time.Millisecond)

			// Read immediately - should be empty
			// Use short timeout
			readCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			defer cancel()
			response, err := bq.Read(readCtx, topic, getRandomChar(701))
			require.NoError(t, err)
			require.Empty(t, response, "message should not be visible yet")

			// Wait for delay to expire
			time.Sleep(2500 * time.Millisecond)

			// Read again - should be visible
			readCtx2, cancel2 := context.WithTimeout(ctx, 1*time.Second)
			defer cancel2()
			response2, err := bq.Read(readCtx2, topic, getRandomChar(701))
			require.NoError(t, err)
			require.Len(t, response2, 1, "message should be visible after delay")
			require.Equal(t, "delayed message", response2[0].Message)
		})
	})
}
