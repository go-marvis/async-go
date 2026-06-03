package async

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-marvis/async-go/base"
)

// A Client is responsible for scheduling tasks.
//
// A Client is used to register tasks that should be processed
// immediately or some time in the future.
//
// Clients are safe for concurrent use by multiple goroutines.
type Client struct {
	broker base.Broker
}

func NewClient(broker base.Broker) *Client {
	return &Client{broker}
}

// Enqueue enqueues the given task to a queue.
func (c *Client) Enqueue(ctx context.Context, task *Task, opts ...OptionFunc) error {
	if task == nil {
		return fmt.Errorf("task cannot be nil")
	}
	if strings.TrimSpace(task.Type()) == "" {
		return fmt.Errorf("task typename cannot be empty")
	}

	option := &Option{
		Queue:   base.DefaultQueueName,
		Retry:   defaultMaxRetry,
		Timeout: defaultTimeout,
	}

	for _, opt := range opts {
		opt(option)
	}

	msg := &base.TaskMessage{
		Type:     task.Type(),
		Payload:  task.Payload(),
		Headers:  task.Headers(),
		Queue:    option.Queue,
		Retry:    option.Retry,
		Priority: option.Priority,
		Timeout:  int64(option.Timeout.Seconds()),
	}

	if option.Delay > 0 {
		msg.AvailableAt = time.Now().Add(option.Delay)
	}

	return c.broker.Enqueue(ctx, msg)
}

type Option struct {
	Queue    string
	Retry    int
	Timeout  time.Duration
	Delay    time.Duration
	Priority int
}

type OptionFunc func(opt *Option)

func WithQueue(queue string) OptionFunc {
	return func(opt *Option) {
		opt.Queue = queue
	}
}

func WithMaxRetry(maxRetry int) OptionFunc {
	return func(opt *Option) {
		opt.Retry = maxRetry
	}
}

func WithTimeout(timeout time.Duration) OptionFunc {
	return func(opt *Option) {
		opt.Timeout = timeout
	}
}

func WithDelay(delay time.Duration) OptionFunc {
	return func(opt *Option) {
		opt.Delay = delay
	}
}

func WithPriority(priority int) OptionFunc {
	return func(opt *Option) {
		opt.Priority = priority
	}
}

const (
	// Default max retry count used if nothing is specified.
	defaultMaxRetry = 5

	// Default timeout used if timeout is specified.
	defaultTimeout = 30 * time.Minute
)
