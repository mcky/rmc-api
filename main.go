package main

import (
	"database/sql"
	"log"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
	_ "github.com/mattn/go-sqlite3"
	"github.com/rossmackay/rockhoppers-db/models"
)

func validateAPIKey(db *sql.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		apiKey := c.Query("api_key")
		if apiKey == "" {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "API key is required"})
			return
		}

		var exists bool
		query := "SELECT 1 FROM api_keys WHERE api_key = ?"
		err := db.QueryRow(query, apiKey).Scan(&exists)

		if err != nil {
			log.Println(err)
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "Invalid API key"})
			return
		}

		c.Next()
	}
}

func main() {
	dbPath := os.Getenv("DB_PATH")

	log.Println("Attempting to connect to sqlite db at:", dbPath)

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		log.Println("Failed to open database:", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Println("Failed to ping database:", err)
	}

	r := gin.Default()

	api := r.Group("/")
	api.Use(validateAPIKey(db))

	{
		api.GET("/meets", func(c *gin.Context) {
			meets, err := models.GetAllMeets(db)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, meets)
		})

		api.GET("/meets/:id", func(c *gin.Context) {
			id := c.Param("id")
			meet, err := models.GetMeetByID(db, id)
			if err != nil {
				c.JSON(http.StatusNotFound, gin.H{"error": "Meet not found"})
				return
			}
			c.JSON(http.StatusOK, meet)
		})

		api.GET("/socials", func(c *gin.Context) {
			socials, err := models.GetAllSocials(db)
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, socials)
		})

		api.GET("/socials/:id", func(c *gin.Context) {
			id := c.Param("id")
			social, err := models.GetSocialByID(db, id)
			if err != nil {
				c.JSON(http.StatusNotFound, gin.H{"error": "Social not found"})
				return
			}
			c.JSON(http.StatusOK, social)
		})
	}

	r.GET("/calendar", func(c *gin.Context) {
		icsData, err := models.GenerateCalendar(db)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}

		c.Header("Content-Type", "text/calendar; charset=utf-8")
		c.Header("Content-Disposition", "attachment; filename=rockhoppers-meets.ics")
		c.String(http.StatusOK, icsData)
	})

	log.Println("Starting server on http://localhost:8080")

	if err := r.Run(":8080"); err != nil {
		log.Fatal("Failed to start server:", err)
	}
}
