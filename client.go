package async

import "time"

const (
	// Default max retry count used if nothing is specified.
	defaultMaxRetry = 5

	// Default timeout used if timeout is specified.
	defaultTimeout = 30 * time.Minute
)
