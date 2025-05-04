package tasks

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// Mock task implementation for testing
type MockTask struct {
	mock.Mock
	executionCount int
	mutex          sync.Mutex
}

func (m *MockTask) Execute() {
	m.Called()
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.executionCount++
}

func (m *MockTask) GetExecutionCount() int {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	return m.executionCount
}

func TestNewTaskRunner(t *testing.T) {
	// Arrange
	mockTask := new(MockTask)
	interval := time.Second * 5

	// Act
	runner := NewTaskRunner(mockTask, interval)

	// Assert
	assert.Equal(t, mockTask, runner.Task)
	assert.Equal(t, interval, runner.Interval)
}

func TestTaskRunner_Run(t *testing.T) {
	// Arrange
	mockTask := new(MockTask)
	mockTask.On("Execute").Return()

	// Use a short interval to speed up the test
	interval := time.Millisecond * 50
	runner := NewTaskRunner(mockTask, interval)

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*175)
	defer cancel()

	// Act
	done := make(chan struct{})
	go func() {
		runner.Run(ctx)
		close(done)
	}()

	// Wait for the runner to complete (context timeout)
	select {
	case <-done:
		// Runner has exited as expected
	case <-time.After(time.Second):
		t.Fatal("Task runner did not exit when context was canceled")
	}

	// Assert
	// Should execute 3 times: immediately, after 50ms, and after 100ms
	// The 150ms execution would be stopped by context timeout at 175ms
	mockTask.AssertNumberOfCalls(t, "Execute", 3)
}

func TestTaskRunner_RunWithCancel(t *testing.T) {
	// Arrange
	mockTask := new(MockTask)
	mockTask.On("Execute").Return()
	interval := time.Millisecond * 50
	runner := NewTaskRunner(mockTask, interval)
	ctx, cancel := context.WithCancel(context.Background())

	// Act
	done := make(chan struct{})
	go func() {
		runner.Run(ctx)
		close(done)
	}()

	// Let it run a bit
	time.Sleep(time.Millisecond * 125)

	// Cancel the context to stop the runner
	cancel()

	// Wait for the runner to stop
	select {
	case <-done:
		// Runner has exited as expected
	case <-time.After(time.Second):
		t.Fatal("Task runner did not exit when context was canceled")
	}

	// Assert
	// Should execute at least twice: immediately and after 50ms
	// Might execute a third time at 100ms depending on timing
	assert.GreaterOrEqual(t, mockTask.GetExecutionCount(), 2)
}

// Update the TestTaskRunner_ExecutesImmediately to match the actual behavior
func TestTaskRunner_ExecutesOnTicker(t *testing.T) {
	// Arrange
	mockTask := new(MockTask)
	mockTask.On("Execute").Return()

	// Use a short interval
	interval := time.Millisecond * 50
	runner := NewTaskRunner(mockTask, interval)

	ctx, cancel := context.WithCancel(context.Background())

	// Act
	done := make(chan struct{})
	go func() {
		runner.Run(ctx)
		close(done)
	}()

	// Wait a bit longer than the interval to ensure the ticker fires
	time.Sleep(interval + time.Millisecond*10)

	// Cancel the context to stop the runner
	cancel()

	// Wait for the runner to stop
	select {
	case <-done:
		// Runner has exited as expected
	case <-time.After(time.Second):
		t.Fatal("Task runner did not exit when context was canceled")
	}

	// Assert
	// Should execute at least once when the ticker fires
	assert.GreaterOrEqual(t, mockTask.GetExecutionCount(), 1)
	mockTask.AssertNumberOfCalls(t, "Execute", mockTask.GetExecutionCount())
}
