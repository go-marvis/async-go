package async

type Task struct {
	Type    string
	Payload []byte
	Headers map[string]string
	Queue   string
}

// Task Status
// pending
// accepted
// completed
