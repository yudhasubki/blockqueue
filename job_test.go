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
