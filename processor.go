package async

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/go-marvis/async-go/base"
	asynccontext "github.com/go-marvis/async-go/context"
	"github.com/go-marvis/async-go/errors"
)

type processor struct {
	broker base.Broker

	handler   Handler
	baseCtxFn func() context.Context

	queues []string

	taskCheckInterval time.Duration
	retryDelayFunc    RetryDelayFunc

	errHandler      ErrorHandler
	shutdownTimeout time.Duration

	// sema is a counting semaphore to ensure the number of active workers
	// does not exceed the limit.
	sema chan struct{}

	// channel to communicate back to the long running "processor" goroutine.
	// once is used to send value to the channel only once.
	done chan struct{}
	once sync.Once

	// quit channel is closed when the shutdown of the "processor" goroutine starts.
	quit chan struct{}

	// abort channel communicates to the in-flight worker goroutines to stop.
	abort chan struct{}

	// cancelations is a set of cancel functions for all active tasks.
	cancelations *base.Cancelations
}

// Note: stops only the "processor" goroutine, does not stop workers.
// It's safe to call this method multiple times.
func (p *processor) stop() {
	p.once.Do(func() {
		slog.Debug("Processor shutting down...")
		// Unblock if processor is waiting for sema token.
		close(p.quit)
		// Signal the processor goroutine to stop processing tasks
		// from the queue.
		p.done <- struct{}{}
	})
}

// NOTE: once shutdown, processor cannot be re-started.
func (p *processor) shutdown() {
	p.stop()

	time.AfterFunc(p.shutdownTimeout, func() { close(p.abort) })

	slog.Info("Waiting for all workers to finish...")
	// block until all workers have released the token
	for i := 0; i < cap(p.sema); i++ {
		p.sema <- struct{}{}
	}
	slog.Info("All workers have finished")
}

func (p *processor) start(wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-p.done:
				slog.Debug("Processor done")
				return
			default:
				p.exec()
			}
		}
	}()
}

// exec pulls a task out of the queue and starts a worker goroutine to
// process the task.
func (p *processor) exec() {
	select {
	case <-p.quit:
		return
	case p.sema <- struct{}{}: // acquire token
		msg, err := p.broker.Dequeue(p.queues...)
		if err != nil {
			slog.Debug("All queues are empty")
			// Queues are empty, this is a normal behavior.
			// Sleep to avoid slamming redis and let scheduler move tasks into queues.
			// Note: We are not using blocking pop operation and polling queues instead.
			// This adds significant load to redis.
			jitter := rand.N(p.taskCheckInterval)
			time.Sleep(p.taskCheckInterval/2 + jitter)
			<-p.sema // release token
			return
		}

		deadline := p.computeDeadline(msg)
		go func() {
			defer func() {
				<-p.sema // release token
			}()

			ctx, cancel := asynccontext.New(p.baseCtxFn(), msg, deadline)
			p.cancelations.Add(msg.ID, cancel)
			defer func() {
				cancel()
				p.cancelations.Delete(msg.ID)
			}()

			// check context before starting a worker goroutine.
			select {
			case <-ctx.Done():
				// already canceled (e.g. deadline exceeded).
				p.handleFailedMessage(ctx, msg, ctx.Err())
				return
			default:
			}

			resCh := make(chan error, 1)
			go func() {
				task := &Task{
					id:       msg.ID,
					typename: msg.Type,
					payload:  msg.Payload,
					headers:  msg.Headers,
					w: &ResultWriter{
						id:     msg.ID,
						qname:  msg.Queue,
						broker: p.broker,
						ctx:    ctx,
					},
				}
				resCh <- p.perform(ctx, task)
			}()

			select {
			case <-p.abort:
				// time is up, push the message back to queue and quit this worker goroutine.
				slog.Warn("Quitting worker.", "task id", msg.ID)
				p.requeue(msg)
				return
			case <-ctx.Done():
				p.handleFailedMessage(ctx, msg, ctx.Err())
				return
			case resErr := <-resCh:
				if resErr != nil {
					p.handleFailedMessage(ctx, msg, resErr)
					return
				}
				p.handleSuccessMessage(ctx, msg)
			}
		}()
	}
}

func (p *processor) perform(ctx context.Context, task *Task) (err error) {
	defer func() {
		if x := recover(); x != nil {
			slog.Error("recovering from panic.", "", string(debug.Stack()))
			_, file, line, ok := runtime.Caller(1) // skip the first frame (panic itself)
			if ok && strings.Contains(file, "runtime/") {
				// The panic came from the runtime, most likely due to incorrect
				// map/slice usage. The parent frame should have the real trigger.
				_, file, line, ok = runtime.Caller(2)
			}
			var errMsg string
			// Include the file and line number info in the error, if runtime.Caller returned ok.
			if ok {
				errMsg = fmt.Sprintf("panic [%s:%d]: %v", file, line, x)
			} else {
				errMsg = fmt.Sprintf("panic: %v", x)
			}
			err = &errors.PanicError{
				ErrMsg: errMsg,
			}
		}
	}()

	return p.handler.ProcessTask(ctx, task)
}

func (p *processor) requeue(msg *base.TaskMessage) {
	ctx := context.Background()
	err := p.broker.Requeue(ctx, msg)
	if err != nil {
		slog.Error("Could not push task back to queue.", "id", msg.ID, "err", err)
	} else {
		slog.Info("Pushed task back to queue.", "id", msg.ID)
	}
}

// SkipRetry is used as a return value from Handler.ProcessTask to indicate that
// the task should not be retried and should be archived instead.
var SkipRetry = errors.New("skip retry for the task")

func (p *processor) handleFailedMessage(ctx context.Context, msg *base.TaskMessage, err error) {
	task := NewTask(msg.Type, msg.Payload, msg.Headers)
	task.id = msg.ID
	if p.errHandler != nil {
		p.errHandler.HandleError(ctx, task, err)
	}

	switch {
	case msg.Retried >= msg.Retry || errors.Is(err, SkipRetry):
		slog.Warn("Retry exhausted.", "task id", msg.ID)
		p.broker.Archive(ctx, msg, err.Error())
	default:
		delay := p.retryDelayFunc(msg.Retried, err, task)
		retryAt := time.Now().Add(delay)
		p.broker.Retry(ctx, msg, retryAt, err.Error(), true)
	}
}

func (p *processor) handleSuccessMessage(ctx context.Context, msg *base.TaskMessage) {
	p.broker.Done(ctx, msg)
}

// computeDeadline returns the given task's deadline,
func (p *processor) computeDeadline(msg *base.TaskMessage) time.Time {
	if msg.Timeout == 0 {
		return time.Now().Add(defaultTimeout)
	}
	return time.Now().Add(time.Duration(msg.Timeout) * time.Second)
}
