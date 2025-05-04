package repositories

import (
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
)

func TestGet_RecordExists(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	repo := NewSyncRepository(sqlxDB)

	task := "test_task"
	expectedTime := time.Now().UTC().Truncate(time.Second)

	rows := sqlmock.NewRows([]string{"synced_at"}).
		AddRow(expectedTime)
	mock.ExpectQuery("SELECT synced_at FROM sync WHERE task = \\?").
		WithArgs(task).
		WillReturnRows(rows)

	result, err := repo.Get(task)
	assert.NoError(t, err)
	assert.Equal(t, expectedTime, result)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGet_RecordDoesNotExists(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	repo := NewSyncRepository(sqlxDB)

	task := "test_task"
	mock.ExpectQuery("SELECT synced_at FROM sync WHERE task = \\?").
		WithArgs(task).
		WillReturnError(sql.ErrNoRows)

	result, err := repo.Get(task)
	assert.NoError(t, err)
	assert.True(t, result.IsZero())
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestGet_DBError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	repo := NewSyncRepository(sqlxDB)

	task := "test_task"
	dbError := errors.New("db error")
	mock.ExpectQuery("SELECT synced_at FROM sync WHERE task = \\?").
		WithArgs(task).
		WillReturnError(dbError)

	result, err := repo.Get(task)
	assert.Error(t, err)
	assert.True(t, result.IsZero())
	assert.Contains(t, err.Error(), "failed to get last sync time for task")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSet_ExistingRecord(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	repo := NewSyncRepository(sqlxDB)

	task := "test_task"
	syncedAt := time.Now().UTC()
	mock.ExpectExec("INSERT INTO sync \\(task, synced_at\\) VALUES \\(\\?, \\?\\) ON DUPLICATE KEY UPDATE synced_at = VALUES\\(synced_at\\)").
		WithArgs(task, syncedAt).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = repo.Set(task, syncedAt)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSet_Error(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	sqlxDB := sqlx.NewDb(db, "sqlmock")
	repo := NewSyncRepository(sqlxDB)

	task := "test_task"
	syncedAt := time.Now().UTC()
	expectedError := errors.New("insert error")
	mock.ExpectExec("INSERT INTO sync \\(task, synced_at\\) VALUES \\(\\?, \\?\\) ON DUPLICATE KEY UPDATE synced_at = VALUES\\(synced_at\\)").
		WithArgs(task, syncedAt).
		WillReturnError(expectedError)

	err = repo.Set(task, syncedAt)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to update sync time")
	assert.NoError(t, mock.ExpectationsWereMet())
}
