package async

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"runtime"
	"sync"
	"time"

	"github.com/go-marvis/async-go/base"
	"github.com/go-marvis/async-go/store"
	"github.com/jmoiron/sqlx"
)

// Server is responsible for task processing and task lifecycle management.
//
// Server pulls tasks off queues and processes them.
// If the processing of a task is unsuccessful, server will schedule it for a retry.
type Server struct {
	broker base.Broker

	// wait group to wait for all goroutines to finish.
	wg        sync.WaitGroup
	processor *processor
}

// Config specifies the server's background-task processing behavior.
type Config struct {
	// Maximum number of concurrent processing of tasks.
	//
	// If set to a zero or negative value, NewServer will overwrite the value
	// to the number of CPUs usable by the current process.
	Concurrency int

	// BaseContext optionally specifies a function that returns the base context for Handler invocations on this server.
	//
	// If BaseContext is nil, the default is context.Background().
	// If this is defined, then it MUST return a non-nil context
	BaseContext func() context.Context

	// TaskCheckInterval specifies the interval between checks for new tasks to process when all queues are empty.
	//
	// If unset, zero or a negative value, the interval is set to 1 second.
	//
	// By default, TaskCheckInterval is set to 1 seconds.
	TaskCheckInterval time.Duration

	// Function to calculate retry delay for a failed task.
	//
	// By default, it uses exponential backoff algorithm to calculate the delay.
	RetryDelayFunc RetryDelayFunc

	// List of queues to process.
	//
	// If set to nil or not specified, the server will process only the "default" queue.
	Queues []string

	// ErrorHandler handles errors returned by the task handler.
	//
	// HandleError is invoked only if the task handler returns a non-nil error.
	ErrorHandler ErrorHandler

	// ShutdownTimeout specifies the duration to wait to let workers finish their tasks
	// before forcing them to abort when stopping the server.
	//
	// If unset or zero, default timeout of 8 seconds is used.
	ShutdownTimeout time.Duration
}

// An ErrorHandler handles an error occurred during task processing.
type ErrorHandler interface {
	HandleError(ctx context.Context, task *Task, err error)
}

// RetryDelayFunc calculates the retry delay duration for a failed task given
// the retry count, error, and the task.
//
// n is the number of times the task has been retried.
// e is the error returned by the task handler.
// t is the task in question.
type RetryDelayFunc func(n int, e error, t *Task) time.Duration

// DefaultRetryDelayFunc is the default RetryDelayFunc used if one is not specified in Config.
// It uses exponential back-off strategy to calculate the retry delay.
func DefaultRetryDelayFunc(n int, e error, t *Task) time.Duration {
	// Formula taken from https://github.com/mperham/sidekiq.
	s := int(math.Pow(float64(n), 4)) + 15 + (rand.IntN(30) * (n + 1))
	return time.Duration(s) * time.Second
}

const (
	defaultTaskCheckInterval = 1 * time.Second
	defaultShutdownTimeout   = 8 * time.Second
)

// NewServer returns a new Server
func NewServer(db *sqlx.DB, cfg Config) *Server {
	baseCtxFn := cfg.BaseContext
	if baseCtxFn == nil {
		baseCtxFn = context.Background
	}

	concurrency := cfg.Concurrency
	if concurrency < 1 {
		concurrency = runtime.NumCPU()
	}

	taskCheckInterval := cfg.TaskCheckInterval
	if taskCheckInterval <= 0 {
		taskCheckInterval = defaultTaskCheckInterval
	}

	delayFunc := cfg.RetryDelayFunc
	if delayFunc == nil {
		delayFunc = DefaultRetryDelayFunc
	}

	queues := cfg.Queues
	if len(queues) == 0 {
		queues = []string{base.DefaultQueueName}
	}

	shutdownTimeout := cfg.ShutdownTimeout
	if shutdownTimeout == 0 {
		shutdownTimeout = defaultShutdownTimeout
	}

	broker := store.NewMySQLStore(db)
	cancels := base.NewCancelations()

	proc := &processor{
		broker:            broker,
		retryDelayFunc:    delayFunc,
		taskCheckInterval: taskCheckInterval,
		baseCtxFn:         baseCtxFn,
		cancelations:      cancels,
		queues:            queues,
		sema:              make(chan struct{}, concurrency),
		done:              make(chan struct{}),
		quit:              make(chan struct{}),
		abort:             make(chan struct{}),
		errHandler:        cfg.ErrorHandler,
		shutdownTimeout:   shutdownTimeout,
	}

	return &Server{
		broker:    broker,
		processor: proc,
	}
}

// A Handler processes tasks.
//
// ProcessTask should return nil if the processing of a task
// is successful.
type Handler interface {
	ProcessTask(context.Context, *Task) error
}

// The HandlerFunc type is an adapter to allow the use of
// ordinary functions as a Handler. If f is a function
// with the appropriate signature, HandlerFunc(f) is a
// Handler that calls f.
type HandlerFunc func(context.Context, *Task) error

// ProcessTask calls fn(ctx, task)
func (fn HandlerFunc) ProcessTask(ctx context.Context, task *Task) error {
	return fn(ctx, task)
}

// Run starts the task processing and blocks until
// an os signal to exit the program is received. Once it receives
// a signal, it gracefully shuts down all active workers and other
// goroutines to process the tasks.
//
// Run returns any error encountered at server startup time.
func (srv *Server) Run(handler Handler) error {
	if err := srv.Start(handler); err != nil {
		return err
	}
	srv.waitForSignals()
	srv.Shutdown()
	return nil
}

// Start starts the worker server. Once the server has started,
// it pulls tasks off queues and starts a worker goroutine for each task
// and then call Handler to process it.
// Tasks are processed concurrently by the workers up to the number of
// concurrency specified in Config.Concurrency.
func (srv *Server) Start(handler Handler) error {
	if handler == nil {
		return fmt.Errorf("asynq: server cannot run with nil handler")
	}
	srv.processor.handler = handler

	slog.Info("Starting processing")
	srv.processor.start(&srv.wg)
	return nil
}

// Shutdown gracefully shuts down the server.
// It gracefully closes all active workers. The server will wait for
// active workers to finish processing tasks for duration specified in Config.ShutdownTimeout.
// If worker didn't finish processing a task during the timeout, the task will be pushed back to broker.
func (srv *Server) Shutdown() {

	slog.Info("Starting graceful shutdown")
	srv.processor.shutdown()
	srv.wg.Wait()

	slog.Info("Exiting")
}

// Stop signals the server to stop pulling new tasks off queues.
// Stop can be used before shutting down the server to ensure that all
// currently active tasks are processed before server shutdown.
//
// Stop does not shutdown the server, make sure to call Shutdown before exit.
func (srv *Server) Stop() {
	slog.Info("Stopping processor")
	srv.processor.stop()
	slog.Info("Processor stopped")
}
