package async

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/go-marvis/async-go/store"
)

func Test_Server(t *testing.T) {
	db, _ := sql.Open("mysql", "datamind:datamind@tcp(127.0.0.1:3306)/datamind?parseTime=true&loc=Local&charset=utf8mb4,utf8")
	broker := store.NewMySQLStore(db)
	mux := NewServeMux()
	mux.HandleFunc("test-task", func(ctx context.Context, t *Task) error {
		fmt.Println(*t)
		t.ResultWriter().Write([]byte("Good work"))
		return nil
	})

	srv := NewServer(broker, Config{
		Queues:            []string{"test-queue"},
		TaskCheckInterval: 5 * time.Second,
	})
	srv.Run(mux)
	srv.Shutdown()
}
