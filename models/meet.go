package models

import (
	"database/sql"
	"fmt"
	"time"
)

type Meet struct {
	ID                         int64      `json:"id"`
	Title                      string     `json:"title"`
	Description                string     `json:"description"`
	BookingsOpenDate           *time.Time `json:"bookings_open_date"`
	StartDate                  *time.Time `json:"start_date"`
	EndDate                    *time.Time `json:"end_date"`
	DateNotes                  string     `json:"date_notes"`
	MeetStewardNotes           string     `json:"meet_steward_notes"`
	LocationURL                string     `json:"location_url"`
	SpacesAvailable            *int       `json:"spaces_available"`
	TotalSpaces                *int       `json:"total_spaces"`
	CreatedAt                  *time.Time `json:"created_at"`
	UpdatedAt                  *time.Time `json:"updated_at"`
	MeetStewardID              *int64     `json:"meet_steward_id"`
	Bookable                   *int       `json:"bookable"`
	SelfOrganisingLifts        *int       `json:"self_organising_lifts"`
	NonLMC                     *int       `json:"nonlmc"`
	WaitingListSpacesAvailable *int       `json:"waiting_list_spaces_available"`
	WaitingListTotalSpaces     *int       `json:"waiting_list_total_spaces"`
	AllowGuests                int        `json:"allow_guests"`
	WebsiteURL                 string
}

// parseDate attempts to parse a date string using multiple formats
func parseDate(dateStr sql.NullString, fieldName string) *time.Time {
	if !dateStr.Valid {
		return nil
	}

	// For start, end, and bookings_open dates
	simpleFormats := []string{time.RFC3339, "2006-01-02"}

	// For created_at and updated_at
	timestampFormats := []string{time.RFC3339, "2006-01-02 15:04:05", "2006-01-02T15:04:05Z"}

	formats := simpleFormats
	if fieldName == "created_at" || fieldName == "updated_at" {
		formats = timestampFormats
	}

	for _, layout := range formats {
		if t, err := time.Parse(layout, dateStr.String); err == nil {
			return &t
		}
	}

	fmt.Printf("Failed to parse %s: '%s'\n", fieldName, dateStr.String)
	return nil
}

// nullIntToPtr converts sql.NullInt64 to *int
func nullIntToPtr(n sql.NullInt64) *int {
	if !n.Valid {
		return nil
	}
	val := int(n.Int64)
	return &val
}

func ScanMeet(scanner interface {
	Scan(dest ...interface{}) error
}) (*Meet, error) {
	var m Meet
	var bookingsOpenDate, startDate, endDate, createdAt, updatedAt sql.NullString
	var spacesAvailable, totalSpaces, meetStewardID, bookable, selfOrganisingLifts, nonLMC,
		waitingListSpacesAvailable, waitingListTotalSpaces sql.NullInt64

	err := scanner.Scan(
		&m.ID,
		&m.Title,
		&m.Description,
		&bookingsOpenDate,
		&startDate,
		&endDate,
		&m.DateNotes,
		&m.MeetStewardNotes,
		&m.LocationURL,
		&spacesAvailable,
		&totalSpaces,
		&createdAt,
		&updatedAt,
		&meetStewardID,
		&bookable,
		&selfOrganisingLifts,
		&nonLMC,
		&waitingListSpacesAvailable,
		&waitingListTotalSpaces,
		&m.AllowGuests,
	)
	if err != nil {
		return nil, err
	}

	m.StartDate = parseDate(startDate, "start_date")
	m.EndDate = parseDate(endDate, "end_date")
	m.BookingsOpenDate = parseDate(bookingsOpenDate, "bookings_open_date")
	m.CreatedAt = parseDate(createdAt, "created_at")
	m.UpdatedAt = parseDate(updatedAt, "updated_at")

	m.SpacesAvailable = nullIntToPtr(spacesAvailable)
	m.TotalSpaces = nullIntToPtr(totalSpaces)
	m.Bookable = nullIntToPtr(bookable)
	m.SelfOrganisingLifts = nullIntToPtr(selfOrganisingLifts)
	m.NonLMC = nullIntToPtr(nonLMC)
	m.WaitingListSpacesAvailable = nullIntToPtr(waitingListSpacesAvailable)
	m.WaitingListTotalSpaces = nullIntToPtr(waitingListTotalSpaces)

	if meetStewardID.Valid {
		m.MeetStewardID = &meetStewardID.Int64
	}

	return &m, nil
}

func GetAllMeets(db *sql.DB) ([]Meet, error) {
	rows, err := db.Query("SELECT * FROM meets")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var meets []Meet
	for rows.Next() {
		meet, err := ScanMeet(rows)
		if err != nil {
			return nil, err
		}
		meets = append(meets, *meet)
	}

	return meets, nil
}

func GetMeetByID(db *sql.DB, id string) (*Meet, error) {
	row := db.QueryRow("SELECT * FROM meets WHERE id = ?", id)
	return ScanMeet(row)
}
