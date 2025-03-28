package models

import (
	"database/sql"
	"time"
)

type Social struct {
	ID          int64      `json:"id"`
	Title       string     `json:"title"`
	Speaker     string     `json:"speaker"`
	StartDate   *time.Time `json:"start_date"`
	StartTime   string     `json:"start_time"`
	Location    string     `json:"location"`
	CreatedAt   *time.Time `json:"created_at"`
	UpdatedAt   *time.Time `json:"updated_at"`
	Description string     `json:"description"`
}

func ScanSocial(scanner interface {
	Scan(dest ...interface{}) error
}) (*Social, error) {
	var s Social
	var startDate, createdAt, updatedAt sql.NullString
	var speaker, startTime, location sql.NullString

	err := scanner.Scan(
		&s.ID,
		&s.Title,
		&speaker,
		&startDate,
		&startTime,
		&location,
		&createdAt,
		&updatedAt,
		&s.Description,
	)
	if err != nil {
		return nil, err
	}

	// Handle NULL values
	if speaker.Valid {
		s.Speaker = speaker.String
	}
	if startTime.Valid {
		s.StartTime = startTime.String
	}
	if location.Valid {
		s.Location = location.String
	}

	s.StartDate = parseDate(startDate, "start_date")
	s.CreatedAt = parseDate(createdAt, "created_at")
	s.UpdatedAt = parseDate(updatedAt, "updated_at")

	return &s, nil
}

func GetAllSocials(db *sql.DB) ([]Social, error) {
	rows, err := db.Query("SELECT * FROM socials")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var socials []Social
	for rows.Next() {
		social, err := ScanSocial(rows)
		if err != nil {
			return nil, err
		}
		socials = append(socials, *social)
	}

	return socials, nil
}

func GetSocialByID(db *sql.DB, id string) (*Social, error) {
	row := db.QueryRow("SELECT * FROM socials WHERE id = ?", id)
	return ScanSocial(row)
}
