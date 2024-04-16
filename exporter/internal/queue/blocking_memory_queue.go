// Copyright The OpenTelemetry Authors
// Copyright (c) 2019 The Jaeger Authors.
// Copyright (c) 2017 Uber Technologies, Inc.
// SPDX-License-Identifier: Apache-2.0

package queue // import "go.opentelemetry.io/collector/exporter/internal/queue"

import (
	"context"
	"sync"

	"go.opentelemetry.io/collector/component"
)

// BlockingMemoryQueue implements a producer-consumer exchange similar to a ring buffer queue,
// where the queue is bounded and if it fills up due to slow consumers, the producer is blocked
// until space is available again.
type BlockingMemoryQueue[T any] struct {
	component.StartFunc
	sync.Mutex
	*queueCapacityLimiter[T]
	items    chan queueRequest[T]
	fullCond *sync.Cond
	stopped  bool
}

// NewBlockingMemoryQueue constructs the new queue of specified capacity, and with an optional
// callback for dropped items (e.g. useful to emit metrics).
func NewBlockingMemoryQueue[T any](set MemoryQueueSettings[T]) Queue[T] {
	q := &BlockingMemoryQueue[T]{
		queueCapacityLimiter: newQueueCapacityLimiter[T](set.Sizer, set.Capacity),
		items:                make(chan queueRequest[T], set.Capacity),
		stopped:              false,
	}
	q.fullCond = sync.NewCond(q)
	return q
}

// Offer is used by the producer to submit new item to the queue. Calling this method on a stopped queue will panic.
func (q *BlockingMemoryQueue[T]) Offer(ctx context.Context, req T) error {
	if q.sizeOf(req) > uint64(q.Capacity()) {
		return ErrQueueIsFull
	}

	{
		q.Lock()
		defer q.Unlock()
		for !q.queueCapacityLimiter.claim(req) && !q.stopped {
			q.fullCond.Wait()
		}
		if q.stopped {
			return nil
		}
	}

	q.items <- queueRequest[T]{ctx: ctx, req: req}
	return nil
}

// Consume applies the provided function on the head of queue.
// The call blocks until there is an item available or the queue is stopped.
// The function returns true when an item is consumed or false if the queue is stopped and emptied.
func (q *BlockingMemoryQueue[T]) Consume(consumeFunc func(context.Context, T) error) bool {
	item, ok := <-q.items
	if !ok {
		return false
	}
	q.Lock()
	q.queueCapacityLimiter.release(item.req)
	q.fullCond.Broadcast()
	q.Unlock()
	// the memory queue doesn't handle consume errors
	_ = consumeFunc(item.ctx, item.req)
	return true
}

// Shutdown closes the queue channel to initiate draining of the queue.
func (q *BlockingMemoryQueue[T]) Shutdown(context.Context) error {
	close(q.items)
	q.Lock()
	q.stopped = true
	q.fullCond.Broadcast()
	q.Unlock()
	return nil
}
