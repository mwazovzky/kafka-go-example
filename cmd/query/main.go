package main

import (
	"fmt"
	"log"
	"time"

	"kafka-go-example/models"
	"kafka-go-example/repositories"
	"kafka-go-example/services"

	_ "github.com/go-sql-driver/mysql"
)

const query = `
SELECT 
	u.id as id, 
	u.status as status, 
	u.name as name, 
	u.created_at as created_at, 
	u.updated_at as updated_at, 
	c.code as "country.code", 
	c.name as "country.name"
FROM users u
JOIN countries c ON u.country_id = c.id
WHERE u.updated_at > ?`

func main() {
	// Load configurations
	dbCfg := services.LoadDatabaseConfig()

	// Create database connection
	db, err := services.NewDatabase(dbCfg)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Initialize repositories
	userRepo := repositories.NewRepository[models.User](db, query)

	users, errs := userRepo.Stream(time.Now().Add(-24 * time.Hour))

	for user := range users {
		fmt.Printf("User: %+v\n\n", user)
	}

	if err, ok := <-errs; err != nil {
		log.Printf("Error streaming users: %v", err)
	} else if ok {
		log.Printf("No errors occurred while streaming users")
	}
}
