package services

import (
	"fmt"

	"github.com/jmoiron/sqlx"
)

func NewDatabase(cfg DatabaseConfig) (*sqlx.DB, error) {
	// Added parseTime=true to DSN for proper datetime scanning into time.Time
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)
	return sqlx.Connect("mysql", dsn)
}
