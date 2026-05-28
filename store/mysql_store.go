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
)

type MySQLStore struct {
	db *sql.DB
}

func NewDatabaseStore(db *sql.DB) *MySQLStore {
	return &MySQLStore{
		db,
	}
}

func (m *MySQLStore) Ping() error {
	return m.db.Ping()
}

func (m *MySQLStore) Close() error {
	return m.db.Close()
}

func (m *MySQLStore) Enqueue(ctx context.Context, msg *base.TaskMessage) error {

	if msg.Type == "" {
		return fmt.Errorf("Task type rerquired")
	}
	if msg.Queue == "" {
		return fmt.Errorf("Queue required")
	}

	headers, _ := json.Marshal(msg.Headers)

	_, err := m.db.ExecContext(ctx,
		`INSERT INTO tasks(type, payload, headers, queue, retry, priority, timeout, status, available_at)
		 VALUES(?, ?, ?, ?, ?, ?, ?, 'pending', ?)`,
		msg.Type,
		string(msg.Payload),
		string(headers),
		msg.Queue,
		msg.Retry,
		msg.Priority,
		msg.Timeout,
		msg.AvailableAt,
	)

	return err
}
func (m *MySQLStore) Dequeue(queue string) (*base.TaskMessage, error) {

	var msg base.TaskMessage

	if queue == "" {
		return &msg, errors.New("Queue required.")
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
	row := tx.QueryRow(`SELECT id, type, payload, headers, queue, retry, retried, priority, timeout, available_at FROM tasks
	WHERE queue=? AND status='pending' AND available_at <= NOW() AND retried <= retry
	ORDER BY priority desc, id
	LIMIT 1 FOR UPDATE`, queue)

	err = row.Scan(&msg.Id, &msg.Type, &msg.Payload, &headers, &msg.Queue, &msg.Retry, &msg.Retried, &msg.Priority, &msg.Timeout, &msg.AvailableAt)
	if err != nil {
		return &msg, err
	}

	_ = json.Unmarshal([]byte(headers), &msg.Headers)

	_, err = tx.Exec("UPDATE tasks SET status='accepted', accepted_at=NOW() WHERE id=?", msg.Id)
	if err != nil {
		return &msg, err
	}

	if err = tx.Commit(); err != nil {
		return &msg, err
	}

	return &msg, err
}

func (m *MySQLStore) Done(ctx context.Context, taskId int64, result string) error {
	_, e := m.db.ExecContext(ctx, "UPDATE tasks SET status='completed', completed_at=NOW(), result=? WHERE id=?", result, taskId)
	return e
}

func (m *MySQLStore) Requeue(ctx context.Context, taskId int64) error {
	_, err := m.db.ExecContext(ctx, "UPDATE tasks SET status='pending', accepted_at=NULL WHERE id=? AND status!='pending'", taskId)
	return err
}

func (m *MySQLStore) Retry(ctx context.Context, taskId int64, delay time.Duration, e error) error {
	var availableAt = time.Now()
	if delay > 0 {
		availableAt.Add(delay)
	}

	_, err := m.db.ExecContext(ctx, "UPDATE tasks SET available_at=?, retried=retried+1, status='pending', accepted_at=NULL, result=? WHERE id=? AND status!='pending'", availableAt, e.Error(), taskId)
	return err
}
