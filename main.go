package main

import (
	"database/sql"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rossmackay/rockhoppers-db/models"
)

func main() {
	// Open database connection
	db, err := sql.Open("sqlite3", "./rmc_sqlite.db")
	if err != nil {
		log.Fatal("Failed to open database:", err)
	}
	defer db.Close()

	// Test database connection
	if err := db.Ping(); err != nil {
		log.Fatal("Failed to ping database:", err)
	}

	// Set up Gin router
	r := gin.Default()

	// Get all meets
	r.GET("/meets", func(c *gin.Context) {
		meets, err := models.GetAllMeets(db)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, meets)
	})

	// Get single meet by ID
	r.GET("/meets/:id", func(c *gin.Context) {
		id := c.Param("id")
		meet, err := models.GetMeetByID(db, id)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "Meet not found"})
			return
		}
		c.JSON(http.StatusOK, meet)
	})

	// Get calendar in iCalendar format
	r.GET("/calendar", func(c *gin.Context) {
		icsData, err := models.GenerateCalendar(db)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		// Set appropriate headers for iCalendar file
		c.Header("Content-Type", "text/calendar; charset=utf-8")
		c.Header("Content-Disposition", "attachment; filename=rockhoppers-meets.ics")
		c.String(http.StatusOK, icsData)
	})

	// Start server
	log.Println("Starting server on http://localhost:8080")
	if err := r.Run(":8080"); err != nil {
		log.Fatal("Failed to start server:", err)
	}
}
