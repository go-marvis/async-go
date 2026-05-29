package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-marvis/async-go/base"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type MySQLStore struct {
	db *sqlx.DB
}

func NewMySQLStore(db *sql.DB) *MySQLStore {
	return &MySQLStore{
		sqlx.NewDb(db, "mysql"),
	}
}

func (m *MySQLStore) Enqueue(ctx context.Context, msg *base.TaskMessage) error {

	if msg.Type == "" {
		return fmt.Errorf("Task type rerquired")
	}
	if msg.Queue == "" {
		return fmt.Errorf("Queue required")
	}

	availableAt := msg.AvailableAt
	if availableAt.IsZero() {
		availableAt = time.Now()
	}

	headers, _ := json.Marshal(msg.Headers)

	_, err := m.db.Exec(
		`INSERT INTO tasks(type, payload, headers, queue, retry, priority, timeout, available_at, status)
		 VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'pending')`,
		msg.Type,
		string(msg.Payload),
		string(headers),
		msg.Queue,
		msg.Retry,
		msg.Priority,
		msg.Timeout,
		availableAt,
	)

	return err
}
func (m *MySQLStore) Dequeue(queues ...string) (*base.TaskMessage, error) {

	var msg base.TaskMessage

	if len(queues) == 0 {
		return &msg, errors.New("Queue required.")
	}

	query, args, err := sqlx.In(`SELECT id, type, payload, headers, queue, retry, retried, priority, timeout, available_at FROM tasks
	WHERE queue IN (?) AND status='pending' AND available_at <= NOW() AND retried <= retry
	ORDER BY priority desc, id
	LIMIT 1 FOR UPDATE`, queues)
	if err != nil {
		return &msg, err
	}

	tx, err := m.db.Begin()
	if err != nil {
		return &msg, err
	}

	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	var headers string
	row := tx.QueryRow(query, args...)

	err = row.Scan(&msg.ID, &msg.Type, &msg.Payload, &headers, &msg.Queue, &msg.Retry, &msg.Retried, &msg.Priority, &msg.Timeout, &msg.AvailableAt)
	if err != nil {
		return &msg, err
	}

	_ = json.Unmarshal([]byte(headers), &msg.Headers)

	_, err = tx.Exec("UPDATE tasks SET status='accepted', accepted_at=NOW() WHERE id=?", msg.ID)
	if err != nil {
		return &msg, err
	}

	if err = tx.Commit(); err != nil {
		return &msg, err
	}

	return &msg, err
}

func (m *MySQLStore) Done(ctx context.Context, msg *base.TaskMessage) error {
	_, e := m.db.Exec(`UPDATE tasks SET status='completed', completed_at=NOW() WHERE id=?`, msg.ID)
	return e
}

func (m *MySQLStore) Requeue(ctx context.Context, msg *base.TaskMessage) error {
	_, err := m.db.Exec(`UPDATE tasks SET status='pending', accepted_at=NULL WHERE id=? AND status!='pending'`, msg.ID)
	return err
}

func (m *MySQLStore) Retry(ctx context.Context, msg *base.TaskMessage, processAt time.Time, errMsg string, isFailure bool) error {
	_, err := m.db.Exec(`UPDATE tasks SET available_at=?, retried=retried+1, status='pending', accepted_at=NULL, completed_at=NULL, result=? WHERE id=? AND status!='pending'`, processAt, errMsg, msg.ID)
	return err
}

func (m *MySQLStore) WriteResult(queue, id string, data []byte) (int, error) {
	_, err := m.db.Exec(`UPDATE tasks SET result=? WHERE id=?`, string(data), id)
	return len(data), err
}
