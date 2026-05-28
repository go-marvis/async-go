package async

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"sync"
	"time"

	"github.com/go-marvis/async-go/base"
	asynccontext "github.com/go-marvis/async-go/context"
)

type processor struct {
	broker base.Broker

	handler   Handler
	contextFn func() context.Context

	queue string

	taskCheckInterval time.Duration
	retryDelayFn      func(int, *Task) time.Duration

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

	finished chan<- *base.TaskMessage
}

func newProcessor() *processor {
	return &processor{}
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
		msg, err := p.broker.Dequeue(p.queue)

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
				<-p.sema //
			}()

			taskId := strconv.FormatInt(msg.Id, 10)
			ctx, cancel := asynccontext.New(p.contextFn(), msg, deadline)
			p.cancelations.Add(taskId, cancel)
			defer func() {
				cancel()
				p.cancelations.Delete(taskId)
			}()

			resCh := make(chan error, 1)
			go func() {
				task := &Task{
					Type:    msg.Type,
					Payload: msg.Payload,
					Headers: msg.Headers,
				}

				resCh <- p.perform(ctx, task)
			}()

			select {
			case <-p.abort:
				// time is up, push the message back to queue and quit this worker goroutine.
				slog.Warn("Quitting worker.", "task id", msg.Id)
				p.requeue(msg)
				return
			case <-ctx.Done():
				p.handleFailedMessage(msg, ctx.Err())
				return
			case resErr := <-resCh:
				if resErr != nil {
					p.handleFailedMessage(msg, resErr)
					return
				}
				p.handleSuccessMessage(msg)
			}
		}()
	}
}

func (p *processor) perform(ctx context.Context, task *Task) error {
	return p.handler.ProcessTask(ctx, task)
}

func (p *processor) requeue(msg *base.TaskMessage) {
	ctx := context.Background()
	err := p.broker.Requeue(ctx, msg.Id)
	if err != nil {
		slog.Error("Could not push task back to queue.", "id", msg.Id, "err", err)
	} else {
		slog.Info("Pushed task back to queue.", "id", msg.Id)
	}
}

func (p *processor) handleFailedMessage(msg *base.TaskMessage, err error) {
	ctx := context.Background()

	delay := p.retryDelayFn(msg.Retried, &Task{
		Type:    msg.Type,
		Payload: msg.Payload,
		Headers: msg.Headers,
		Queue:   msg.Queue,
	})

	p.broker.Retry(ctx, msg.Id, delay, err)
}

func (p *processor) handleSuccessMessage(msg *base.TaskMessage) {
	ctx := context.Background()
	p.broker.Done(ctx, msg.Id, "")
}

// computeDeadline returns the given task's deadline,
func (p *processor) computeDeadline(msg *base.TaskMessage) time.Time {
	if msg.Timeout == 0 {
		return time.Now().Add(defaultTimeout)
	}
	return time.Now().Add(time.Duration(msg.Timeout) * time.Second)
}
