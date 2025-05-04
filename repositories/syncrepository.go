package repositories

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

type SyncRepository struct {
	db *sqlx.DB
}

func NewSyncRepository(db *sqlx.DB) *SyncRepository {
	return &SyncRepository{db: db}
}

const query = "SELECT synced_at FROM sync WHERE task = ?"

const upsert = "INSERT INTO sync (task, synced_at) VALUES (?, ?) ON DUPLICATE KEY UPDATE synced_at = VALUES(synced_at)"

// Get retrieves the last sync time for a task.
// Returns zero time if no record exists.
func (r *SyncRepository) Get(task string) (time.Time, error) {
	var syncedAt sql.NullTime
	err := r.db.Get(&syncedAt, query, task)
	if err != nil {
		if err == sql.ErrNoRows {
			return time.Time{}, nil
		}
		return time.Time{}, fmt.Errorf("failed to get last sync time for task %s: %w", task, err)
	}
	return syncedAt.Time, nil
}

// Set updates or creates a sync time entry for a task.
func (r *SyncRepository) Set(task string, syncedAt time.Time) error {
	_, err := r.db.Exec(upsert, task, syncedAt)
	if err != nil {
		return fmt.Errorf("failed to update sync time: %w", err)
	}
	return nil
}
