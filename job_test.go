package blockqueue

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/yudhasubki/blockqueue/pkg/core"
	"github.com/yudhasubki/blockqueue/pkg/io"
)

func testAddJob(t *testing.T, ctx context.Context, bq *BlockQueue[chan io.ResponseMessages], topic core.Topic, subscriber core.Subscribers, expectErr error) {
	err := bq.addJob(ctx, topic, subscriber)
	if err != nil {
		require.Equal(t, expectErr, err)
	} else {
		require.NoError(t, err)
	}
}

func testDeleteJob(t *testing.T, ctx context.Context, bq *BlockQueue[chan io.ResponseMessages], topic core.Topic, expectErr error) {
	err := bq.deleteJob(topic)
	if err != nil {
		require.Equal(t, expectErr, err)
	} else {
		require.NoError(t, err)
	}
}

func testPublish(t *testing.T, ctx context.Context, bq *BlockQueue[chan io.ResponseMessages], topic core.Topic, request io.Publish, expectErr error) {
	err := bq.publish(ctx, topic, request)
	if err != nil {
		require.Equal(t, expectErr, err)
	} else {
		require.NoError(t, err)
	}
}

func testReadSubscriberMessage(t *testing.T, ctx context.Context, bq *BlockQueue[chan io.ResponseMessages], topic core.Topic, subscriberName string, expectResponse io.ResponseMessages, expectErr error) io.ResponseMessage {
	response, err := bq.readSubscriberMessage(ctx, topic, subscriberName)
	if err != nil {
		require.Equal(t, expectErr, err)
	} else {
		require.NoError(t, err)
		if len(expectResponse) > 0 {
			require.EqualValues(t, expectResponse[0].Message, response[0].Message)
			return response[0]
		}
	}
	return io.ResponseMessage{}
}

func testAddSubscriber(t *testing.T, ctx context.Context, bq *BlockQueue[chan io.ResponseMessages], topic core.Topic, subscribers core.Subscribers, expectErr error) {
	err := bq.addSubscriber(ctx, topic, subscribers)
	if err != nil {
		require.Equal(t, expectErr, err)
	} else {
		require.NoError(t, err)
	}
}

func testDeleteSubscriber(t *testing.T, ctx context.Context, bq *BlockQueue[chan io.ResponseMessages], topic core.Topic, subscriberName string, expectErr error) {
	err := bq.deleteSubscriber(ctx, topic, subscriberName)
	if err != nil {
		require.Equal(t, expectErr, err)
	} else {
		require.NoError(t, err)
	}
}

func testAckMessage(t *testing.T, ctx context.Context, bq *BlockQueue[chan io.ResponseMessages], topic core.Topic, subscriberName, messageId string, expectErr error) {
	err := bq.ackMessage(ctx, topic, subscriberName, messageId)
	if err != nil {
		require.Equal(t, expectErr, err)
	} else {
		require.NoError(t, err)
	}
}
