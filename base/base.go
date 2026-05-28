package base

import (
	"context"
	"sync"
	"time"
)

type TaskMessage struct {
	// Id is a unique identifier for each task.
	Id int64

	// Type indicates the kind of the task to be performed.
	Type string

	// Payload holds data needed to process the task.
	Payload []byte

	// Headers holds additional metadata for the task.
	Headers map[string]string

	// Queue is a name this message should be enqueued to.
	Queue string

	// Retry is the max number of retry for this task.
	Retry int

	// Retried is the number of times we've retried this task so far.
	Retried int

	// Priority specifies task level, minium is 0.
	Priority int

	// Timeout specifies timeout in seconds.
	// Use zero to indicate no timeout.
	Timeout int64

	AvailableAt time.Time
}

// Broker is a message broker that supports operations to manage task queues.
type Broker interface {
	Ping() error
	Close() error

	Enqueue(ctx context.Context, msg *TaskMessage) error
	Dequeue(queue string) (*TaskMessage, error)
	Done(ctx context.Context, taskId int64, result string) error
	Requeue(ctx context.Context, taskId int64) error
	Retry(ctx context.Context, taskId int64, delay time.Duration, err error) error
}

// Cancelations is a collection that holds cancel functions for all active tasks.
//
// Cancelations are safe for concurrent use by multiple goroutines.
type Cancelations struct {
	mu          sync.Mutex
	cancelFuncs map[string]context.CancelFunc
}

// NewCancelations returns a Cancelations instance.
func NewCancelations() *Cancelations {
	return &Cancelations{
		cancelFuncs: make(map[string]context.CancelFunc),
	}
}

// Add adds a new cancel func to the collection.
func (c *Cancelations) Add(id string, fn context.CancelFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cancelFuncs[id] = fn
}

// Delete deletes a cancel func from the collection given an id.
func (c *Cancelations) Delete(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.cancelFuncs, id)
}

// Get returns a cancel func given an id.
func (c *Cancelations) Get(id string) (fn context.CancelFunc, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fn, ok = c.cancelFuncs[id]
	return fn, ok
}
