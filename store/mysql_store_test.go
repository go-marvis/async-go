package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/go-marvis/async-go/base"
	"github.com/stretchr/testify/assert"
)

func test_store() base.Broker {
	db, _ := sql.Open("mysql", "datamind:datamind@tcp(127.0.0.1:3306)/datamind?parseTime=true&loc=Local&charset=utf8mb4,utf8")

	return NewDatabaseStore(db)
}

func Test_DatabaseStore_Enqueue(t *testing.T) {
	store := test_store()

	data, err := json.Marshal(map[string]string{"name": "alice"})
	t.Log(err)
	assert.Nil(t, err)
	err = store.Enqueue(context.Background(), &base.TaskMessage{
		Type:    "test-task",
		Queue:   "test-queue",
		Payload: data,
		Retry:   3,
	})
	assert.Nil(t, err)
}

func Test_DatabaseStore_Dequeue(t *testing.T) {
	t.Log(time.Now().Local())

	store := test_store()

	tc, err := store.Dequeue("test-queue")
	assert.Nil(t, err)
	t.Log(*tc)

	store.Done(context.Background(), tc.Id, "OK")
}

func Test_DatabaseStore_Retry(t *testing.T) {
	store := test_store()
	ctx := context.Background()

	for i := range 10 {
		err := store.Retry(ctx, int64(i), time.Second, errors.New("args err"))
		assert.Nil(t, err)
	}
}
