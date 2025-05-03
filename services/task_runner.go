package services

import (
	"context"
	"log"
	"time"
)

type TaskRunner struct {
	Task     TaskInterface
	Interval time.Duration
}

func NewTaskRunner(task TaskInterface, interval time.Duration) *TaskRunner {
	return &TaskRunner{
		Task:     task,
		Interval: interval,
	}
}

func (r *TaskRunner) Run(ctx context.Context) {
	ticker := time.NewTicker(r.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("Stopping task runner")
			return
		case <-ticker.C:
			log.Printf("Executing task")
			r.Task.Execute()
		}
	}
}
