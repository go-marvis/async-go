package async

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
)

func Test_Server(t *testing.T) {
	db, _ := sqlx.Connect("mysql", "datamind:datamind@tcp(127.0.0.1:3306)/datamind?parseTime=true&loc=Local&charset=utf8mb4,utf8")
	mux := NewServeMux()
	mux.HandleFunc("test-task", func(ctx context.Context, t *Task) error {
		fmt.Println(*t)
		t.ResultWriter().Write([]byte("Good work"))
		return nil
	})

	srv := NewServer(db, Config{
		Queues:            []string{"test-queue"},
		TaskCheckInterval: 5 * time.Second,
	})
	srv.Run(mux)
	srv.Shutdown()
}
