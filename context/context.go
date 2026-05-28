package context

import (
	"context"
	"time"

	"github.com/go-marvis/async-go/base"
)

type taskMetadata struct {
	id      int64
	retried int
	retry   int
}

const metadataCtxKey = 0

// New returns a context and cancel function for a given task message.
func New(base context.Context, msg *base.TaskMessage, deadline time.Time) (context.Context, context.CancelFunc) {
	metadata := taskMetadata{
		id:      msg.Id,
		retry:   msg.Retry,
		retried: msg.Retried,
	}
	ctx := context.WithValue(base, metadataCtxKey, metadata)
	return context.WithDeadline(ctx, deadline)
}

// GetTaskID extracts a task ID from a context, if any.
func GetTaskId(ctx context.Context) (int64, bool) {
	metadata, ok := ctx.Value(metadataCtxKey).(taskMetadata)
	if !ok {
		return 0, false
	}
	return metadata.id, true
}
