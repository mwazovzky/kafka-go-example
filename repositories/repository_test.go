package repositories

import (
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
)

type TestModel struct {
	ID   int    `db:"id"`
	Name string `db:"name"`
}

func TestStream_Success(t *testing.T) {
	// Arrange
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	query := "SELECT id, name FROM test_table WHERE updated_at > ?"
	repo := NewRepository[TestModel](sqlxDB, query)
	rows := sqlmock.NewRows([]string{"id", "name"}).
		AddRow(1, "Alice").
		AddRow(2, "Bob")

	mock.ExpectQuery(query).WithArgs(sqlmock.AnyArg()).WillReturnRows(rows)

	// Act
	since := time.Now().Add(-24 * time.Hour)
	out, errs := repo.Stream(since)
	var results []TestModel
	for item := range out {
		results = append(results, item)
	}

	// Assert
	assert.NoError(t, <-errs)
	assert.Equal(t, 2, len(results))
	assert.Equal(t, TestModel{ID: 1, Name: "Alice"}, results[0])
	assert.Equal(t, TestModel{ID: 2, Name: "Bob"}, results[1])
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestStream_QueryError(t *testing.T) {
	// Arrange
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	query := "SELECT id, name FROM test_table WHERE updated_at > ?"
	repo := NewRepository[TestModel](sqlxDB, query)

	mock.ExpectQuery(query).WithArgs(sqlmock.AnyArg()).WillReturnError(assert.AnError)

	// Act
	since := time.Now().Add(-24 * time.Hour)
	out, errs := repo.Stream(since)

	// Assert
	_, ok := <-out
	assert.False(t, ok) // Ensure the output channel is closed.
	assert.Error(t, <-errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestStream_ScanError(t *testing.T) {
	// Arrange
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	query := "SELECT id, name FROM test_table WHERE updated_at > ?"
	repo := NewRepository[TestModel](sqlxDB, query)

	rows := sqlmock.NewRows([]string{"id", "name"}).
		AddRow("invalid_id", "Alice") // Simulate a scan error.

	mock.ExpectQuery(query).WithArgs(sqlmock.AnyArg()).WillReturnRows(rows)

	// Act
	since := time.Now().Add(-24 * time.Hour)
	out, errs := repo.Stream(since)

	// Assert
	_, ok := <-out
	assert.False(t, ok) // Ensure the output channel is closed.
	assert.Error(t, <-errs)
	assert.NoError(t, mock.ExpectationsWereMet())
}
