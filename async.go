package async

import (
	"context"
	"fmt"

	"github.com/go-marvis/async-go/base"
)

type Task struct {
	// unique id
	id string

	// typename indicates the type of task to be performed.
	typename string

	// payload holds data needed to perform the task.
	payload []byte

	// headers holds additional metadata for the task.
	headers map[string]string

	// w is the ResultWriter for the task.
	w *ResultWriter
}

func (t *Task) ID() string                 { return t.id }
func (t *Task) Type() string               { return t.typename }
func (t *Task) Payload() []byte            { return t.payload }
func (t *Task) Headers() map[string]string { return t.headers }

// ResultWriter returns a pointer to the ResultWriter associated with the task.
//
// Nil pointer is returned if called on a newly created task (i.e. task created by calling NewTask).
// Only the tasks passed to Handler.ProcessTask have a valid ResultWriter pointer.
func (t *Task) ResultWriter() *ResultWriter { return t.w }

// NewTask returns a new Task given a type name, payload data, and headers.
// Options can be passed to configure task processing behavior.
func NewTask(typename string, payload []byte, headers map[string]string) *Task {
	return &Task{
		typename: typename,
		payload:  payload,
		headers:  headers,
	}
}

// ResultWriter is a client interface to write result data for a task.
type ResultWriter struct {
	id     string // task ID this writer is responsible for
	qname  string // queue name the task belongs to
	broker base.Broker
	ctx    context.Context // context associated with the task
}

// Write writes the given data as a result of the task the ResultWriter is associated with.
func (w *ResultWriter) Write(data []byte) (n int, err error) {
	select {
	case <-w.ctx.Done():
		return 0, fmt.Errorf("failed to write task result: %w", w.ctx.Err())
	default:
	}
	return w.broker.WriteResult(w.qname, w.id, data)
}
