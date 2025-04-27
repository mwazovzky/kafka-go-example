package models

import (
	"strconv"
	"time"
)

type User struct {
	ID        int64     `db:"id" avro:"user_id"`
	Status    string    `db:"status" avro:"status"`
	Name      string    `db:"name" avro:"name"`
	Country   Country   `db:"country" avro:"country"`
	CreatedAt time.Time `db:"created_at" avro:"created_at"`
	UpdatedAt time.Time `db:"updated_at" avro:"updated_at"`
}

func (u User) GetID() string {
	return strconv.FormatInt(u.ID, 10)
}
