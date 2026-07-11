package worker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/yudhasubki/blockqueue"
)

func TestGroupRunsAndDrainsMultipleWorkers(t *testing.T) {
	firstClient := newFakeClient()
	secondClient := newFakeClient()
	firstTopic := blockqueue.NewTopic("orders")
	secondTopic := blockqueue.NewTopic("emails")
	first, err := New(firstClient, firstTopic, "fulfillment", HandlerFunc(func(context.Context, *Job) error {
		return nil
	}), testWorkerOptions())
	require.NoError(t, err)
	second, err := New(secondClient, secondTopic, "sender", HandlerFunc(func(context.Context, *Job) error {
		return nil
	}), testWorkerOptions())
	require.NoError(t, err)
	group, err := NewGroup(first, second)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- group.Run(ctx) }()
	firstClient.claims <- claimResult{deliveries: blockqueue.Deliveries{testDelivery("order")}}
	secondClient.claims <- claimResult{deliveries: blockqueue.Deliveries{testDelivery("email")}}
	<-firstClient.acks
	<-secondClient.acks
	cancel()
	require.NoError(t, <-result)
}

func TestGroupTerminalErrorCancelsPeerWorkers(t *testing.T) {
	firstClient := newFakeClient()
	secondClient := newFakeClient()
	first, err := New(firstClient, blockqueue.NewTopic("orders"), "fulfillment", HandlerFunc(func(context.Context, *Job) error {
		return nil
	}), testWorkerOptions())
	require.NoError(t, err)
	second, err := New(secondClient, blockqueue.NewTopic("emails"), "sender", HandlerFunc(func(context.Context, *Job) error {
		return nil
	}), testWorkerOptions())
	require.NoError(t, err)
	group, err := NewGroup(first, second)
	require.NoError(t, err)
	firstClient.claims <- claimResult{err: blockqueue.ErrSubscriberDeleted}
	result := make(chan error, 1)
	go func() { result <- group.Run(context.Background()) }()
	select {
	case runErr := <-result:
		require.ErrorIs(t, runErr, blockqueue.ErrSubscriberDeleted)
	case <-time.After(time.Second):
		require.Fail(t, "group did not stop peers after terminal worker error")
	}
}

func TestGroupValidatesRegistrationAndIsSingleUse(t *testing.T) {
	_, err := NewGroup()
	require.ErrorIs(t, err, ErrInvalidConfiguration)
	_, err = NewGroup(nil)
	require.ErrorIs(t, err, ErrInvalidConfiguration)
	client := newFakeClient()
	runner, err := New(client, blockqueue.NewTopic("orders"), "fulfillment", HandlerFunc(func(context.Context, *Job) error {
		return nil
	}), testWorkerOptions())
	require.NoError(t, err)
	_, err = NewGroup(runner, runner)
	require.ErrorIs(t, err, ErrInvalidConfiguration)
	group, err := NewGroup(runner)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, group.Run(ctx))
	require.ErrorIs(t, group.Run(context.Background()), ErrAlreadyStarted)
}
