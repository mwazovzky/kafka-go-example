package tasks

import (
	"errors"
	"io/fs"
	"kafka-go-example/infra/config"
	"os"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockRepository struct {
	mock.Mock
}

func (m *MockRepository) Stream(since time.Time) (<-chan TestModel, <-chan error) {
	args := m.Called(since)
	return args.Get(0).(chan TestModel), args.Get(1).(chan error)
}

type MockSyncRepository struct {
	mock.Mock
}

func (m *MockSyncRepository) Get(task string) (time.Time, error) {
	args := m.Called(task)
	return args.Get(0).(time.Time), args.Error(1)
}

func (m *MockSyncRepository) Set(task string, syncedAt time.Time) error {
	args := m.Called(task, syncedAt)
	return args.Error(0)
}

type MockSerializer struct {
	mock.Mock
}

func (m *MockSerializer) Serialize(schema string, data interface{}) ([]byte, error) {
	args := m.Called(schema, data)
	return args.Get(0).([]byte), args.Error(1)
}

type MockProducer struct {
	mock.Mock
}

func (m *MockProducer) ProduceMessage(topic string, payload []byte, key []byte) error {
	args := m.Called(topic, payload, key)
	return args.Error(0)
}

func (m *MockProducer) Close() {
	m.Called()
}

type TestModel struct {
	ID   int    `db:"id"`
	Name string `db:"name"`
}

func TestNewTask(t *testing.T) {
	// Create a temporary file with SQL query
	tmpFile, err := os.CreateTemp("", "test-query-*.sql")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	// Write test SQL to the file
	testSQL := "SELECT * FROM test"
	_, err = tmpFile.WriteString(testSQL)
	assert.NoError(t, err)
	tmpFile.Close()

	// Setup test dependencies
	db, _, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	serializer := new(MockSerializer)
	producer := new(MockProducer)

	config := config.TaskConfig{
		Name:      "test",
		QueryFile: tmpFile.Name(),
		Topic:     "test-topic",
		Schema:    "test-schema",
		Interval:  10 * time.Second,
	}

	// Act
	task, err := NewTask[TestModel](sqlxDB, config, serializer, producer)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, task)
	assert.Equal(t, config, task.Config)
	assert.NotNil(t, task.Repository)
	assert.Equal(t, serializer, task.Serializer)
	assert.Equal(t, producer, task.Producer)
}

func TestNewTask_QueryFileError(t *testing.T) {
	// Setup with non-existent query file
	db, _, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	serializer := new(MockSerializer)
	producer := new(MockProducer)

	config := config.TaskConfig{
		Name:      "test",
		QueryFile: "non-existent-file.sql",
		Topic:     "test-topic",
		Schema:    "test-schema",
		Interval:  10 * time.Second,
	}

	// Act
	task, err := NewTask[TestModel](sqlxDB, config, serializer, producer)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, task)
	assert.True(t, errors.Is(err, fs.ErrNotExist) || err.Error() == "failed to open query file non-existent-file.sql: open non-existent-file.sql: no such file or directory")
}

func TestExecute_Success(t *testing.T) {
	// Arrange
	mockRepo := new(MockRepository)
	mockSyncRepo := new(MockSyncRepository)
	mockSerializer := new(MockSerializer)
	mockProducer := new(MockProducer)

	dataChan := make(chan TestModel, 2)
	errChan := make(chan error, 1)

	testItem1 := TestModel{ID: 1, Name: "Test 1"}
	testItem2 := TestModel{ID: 2, Name: "Test 2"}

	mockRepo.On("Stream", mock.AnythingOfType("time.Time")).Return(dataChan, errChan)
	mockSyncRepo.On("Get", "test").Return(time.Now().Add(-time.Hour), nil)
	mockSyncRepo.On("Set", "test", mock.AnythingOfType("time.Time")).Return(nil)
	mockSerializer.On("Serialize", "test-schema", &testItem1).Return([]byte("serialized1"), nil)
	mockSerializer.On("Serialize", "test-schema", &testItem2).Return([]byte("serialized2"), nil)
	mockProducer.On("ProduceMessage", "test-topic", []byte("serialized1"), mock.Anything).Return(nil)
	mockProducer.On("ProduceMessage", "test-topic", []byte("serialized2"), mock.Anything).Return(nil)

	task := &Task[TestModel]{
		Config: config.TaskConfig{
			Name:   "test",
			Topic:  "test-topic",
			Schema: "test-schema",
		},
		Repository: mockRepo,
		SyncRepo:   mockSyncRepo,
		Serializer: mockSerializer,
		Producer:   mockProducer,
	}

	// Act
	go func() {
		dataChan <- testItem1
		dataChan <- testItem2
		close(dataChan)
		close(errChan)
	}()

	task.Execute()

	// Assert
	mockRepo.AssertExpectations(t)
	mockSyncRepo.AssertExpectations(t)
	mockSerializer.AssertExpectations(t)
	mockProducer.AssertExpectations(t)
}

func TestExecute_SerializeError(t *testing.T) {
	// Arrange
	mockRepo := new(MockRepository)
	mockSyncRepo := new(MockSyncRepository)
	mockSerializer := new(MockSerializer)
	mockProducer := new(MockProducer)

	dataChan := make(chan TestModel, 2)
	errChan := make(chan error, 1)

	testItem1 := TestModel{ID: 1, Name: "Test 1"}
	testItem2 := TestModel{ID: 2, Name: "Test 2"}

	mockRepo.On("Stream", mock.AnythingOfType("time.Time")).Return(dataChan, errChan)
	mockSyncRepo.On("Get", "test").Return(time.Now().Add(-time.Hour), nil)
	mockSyncRepo.On("Set", "test", mock.AnythingOfType("time.Time")).Return(nil)
	mockSerializer.On("Serialize", "test-schema", &testItem1).Return([]byte{}, errors.New("serialize error"))
	mockSerializer.On("Serialize", "test-schema", &testItem2).Return([]byte("serialized2"), nil)
	mockProducer.On("ProduceMessage", "test-topic", []byte("serialized2"), mock.Anything).Return(nil)

	task := &Task[TestModel]{
		Config: config.TaskConfig{
			Name:   "test",
			Topic:  "test-topic",
			Schema: "test-schema",
		},
		Repository: mockRepo,
		SyncRepo:   mockSyncRepo,
		Serializer: mockSerializer,
		Producer:   mockProducer,
	}

	// Act
	go func() {
		dataChan <- testItem1
		dataChan <- testItem2
		close(dataChan)
		close(errChan)
	}()

	task.Execute()

	// Assert
	mockRepo.AssertExpectations(t)
	mockSyncRepo.AssertExpectations(t)
	mockSerializer.AssertExpectations(t)
	mockProducer.AssertExpectations(t)
}

func TestExecute_ProduceError(t *testing.T) {
	// Arrange
	mockRepo := new(MockRepository)
	mockSyncRepo := new(MockSyncRepository)
	mockSerializer := new(MockSerializer)
	mockProducer := new(MockProducer)

	dataChan := make(chan TestModel, 2)
	errChan := make(chan error, 1)

	testItem1 := TestModel{ID: 1, Name: "Test 1"}
	testItem2 := TestModel{ID: 2, Name: "Test 2"}

	mockRepo.On("Stream", mock.AnythingOfType("time.Time")).Return(dataChan, errChan)
	mockSyncRepo.On("Get", "test").Return(time.Now().Add(-time.Hour), nil)
	mockSyncRepo.On("Set", "test", mock.AnythingOfType("time.Time")).Return(nil)
	mockSerializer.On("Serialize", "test-schema", &testItem1).Return([]byte("serialized1"), nil)
	mockSerializer.On("Serialize", "test-schema", &testItem2).Return([]byte("serialized2"), nil)
	mockProducer.On("ProduceMessage", "test-topic", []byte("serialized1"), mock.Anything).Return(errors.New("produce error"))
	mockProducer.On("ProduceMessage", "test-topic", []byte("serialized2"), mock.Anything).Return(nil)

	task := &Task[TestModel]{
		Config: config.TaskConfig{
			Name:   "test",
			Topic:  "test-topic",
			Schema: "test-schema",
		},
		Repository: mockRepo,
		SyncRepo:   mockSyncRepo,
		Serializer: mockSerializer,
		Producer:   mockProducer,
	}

	// Act
	go func() {
		dataChan <- testItem1
		dataChan <- testItem2
		close(dataChan)
		close(errChan)
	}()

	task.Execute()

	// Assert
	mockRepo.AssertExpectations(t)
	mockSyncRepo.AssertExpectations(t)
	mockSerializer.AssertExpectations(t)
	mockProducer.AssertExpectations(t)
}

func TestExecute_StreamError(t *testing.T) {
	// Arrange
	mockRepo := new(MockRepository)
	mockSyncRepo := new(MockSyncRepository)
	mockSerializer := new(MockSerializer)
	mockProducer := new(MockProducer)

	dataChan := make(chan TestModel)
	errChan := make(chan error, 1)

	mockRepo.On("Stream", mock.AnythingOfType("time.Time")).Return(dataChan, errChan)
	mockSyncRepo.On("Get", "test").Return(time.Now(), nil)
	mockSyncRepo.On("Set", "test", mock.AnythingOfType("time.Time")).Return(nil)

	task := &Task[TestModel]{
		Config: config.TaskConfig{
			Name:   "test",
			Topic:  "test-topic",
			Schema: "test-schema",
		},
		Repository: mockRepo,
		SyncRepo:   mockSyncRepo,
		Serializer: mockSerializer,
		Producer:   mockProducer,
	}

	// Act
	go func() {
		close(dataChan)
		errChan <- errors.New("stream error")
		close(errChan)
	}()

	task.Execute()

	// Assert
	mockRepo.AssertExpectations(t)
	mockSyncRepo.AssertExpectations(t)
}

func TestCreateTask_Success(t *testing.T) {
	// Create a temporary file with SQL query
	tmpFile, err := os.CreateTemp("", "test-query-*.sql")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	// Write test SQL to the file
	_, err = tmpFile.WriteString("SELECT * FROM users")
	assert.NoError(t, err)
	tmpFile.Close()

	// Arrange
	db, _, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	serializer := new(MockSerializer)
	producer := new(MockProducer)

	config := config.TaskConfig{
		Name:      "user",
		QueryFile: tmpFile.Name(),
		Topic:     "user-topic",
		Schema:    "user-schema",
	}

	// Act
	task, err := CreateTask(sqlxDB, config, serializer, producer)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, task)
	_, ok := task.(TaskInterface)
	assert.True(t, ok)
}

func TestCreateTask_UnsupportedType(t *testing.T) {
	// Arrange
	db, _, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	serializer := new(MockSerializer)
	producer := new(MockProducer)

	config := config.TaskConfig{
		Name: "unknown",
	}

	// Act
	task, err := CreateTask(sqlxDB, config, serializer, producer)

	// Assert
	assert.Error(t, err)
	assert.Nil(t, task)
	assert.Contains(t, err.Error(), "unsupported task name")
}

func TestLoadQueryFromFile_Success(t *testing.T) {
	// Create a temporary file
	tmpFile, err := os.CreateTemp("", "test-query-*.sql")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	// Write test SQL to the file
	testSQL := "SELECT * FROM test"
	_, err = tmpFile.WriteString(testSQL)
	assert.NoError(t, err)
	tmpFile.Close()

	// Act
	sql, err := loadQueryFromFile(tmpFile.Name())

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, testSQL, sql)
}

func TestLoadQueryFromFile_Error(t *testing.T) {
	// Act
	sql, err := loadQueryFromFile("non-existent-file.sql")

	// Assert
	assert.Error(t, err)
	assert.Empty(t, sql)
	assert.Contains(t, err.Error(), "failed to open query file")
}

func TestExecute_SyncRepoIntegration(t *testing.T) {
	// Arrange
	mockRepo := new(MockRepository)
	mockSyncRepo := new(MockSyncRepository)
	mockSerializer := new(MockSerializer)
	mockProducer := new(MockProducer)

	dataChan := make(chan TestModel, 2)
	errChan := make(chan error, 1)

	testItem1 := TestModel{ID: 1, Name: "Test 1"}
	testItem2 := TestModel{ID: 2, Name: "Test 2"}

	mockRepo.On("Stream", mock.AnythingOfType("time.Time")).Return(dataChan, errChan)
	mockSyncRepo.On("Get", "test").Return(time.Now().Add(-time.Hour), nil)
	mockSyncRepo.On("Set", "test", mock.AnythingOfType("time.Time")).Return(nil)
	mockSerializer.On("Serialize", "test-schema", &testItem1).Return([]byte("serialized1"), nil)
	mockSerializer.On("Serialize", "test-schema", &testItem2).Return([]byte("serialized2"), nil)
	mockProducer.On("ProduceMessage", "test-topic", []byte("serialized1"), mock.Anything).Return(nil)
	mockProducer.On("ProduceMessage", "test-topic", []byte("serialized2"), mock.Anything).Return(nil)

	task := &Task[TestModel]{
		Config: config.TaskConfig{
			Name:   "test",
			Topic:  "test-topic",
			Schema: "test-schema",
		},
		Repository: mockRepo,
		SyncRepo:   mockSyncRepo,
		Serializer: mockSerializer,
		Producer:   mockProducer,
	}

	// Act
	go func() {
		dataChan <- testItem1
		dataChan <- testItem2
		close(dataChan)
		close(errChan)
	}()

	task.Execute()

	// Assert
	mockRepo.AssertExpectations(t)
	mockSyncRepo.AssertExpectations(t)
	mockSerializer.AssertExpectations(t)
	mockProducer.AssertExpectations(t)
}
