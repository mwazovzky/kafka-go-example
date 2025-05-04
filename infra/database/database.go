package database

import (
	"fmt"
	"kafka-go-example/infra/config"

	"github.com/jmoiron/sqlx"
)

func NewDatabase(cfg config.DatabaseConfig) (*sqlx.DB, error) {
	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?parseTime=true",
		cfg.User,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		cfg.Database,
	)
	return sqlx.Connect("mysql", dsn)
}
