package async

import (
	"context"
)

// A Handler processes tasks.
//
// ProcessTask should return nil if the processing of a task
// is successful.
type Handler interface {
	ProcessTask(context.Context, *Task) error
}
