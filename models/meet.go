package models

import (
	"database/sql"
	"fmt"
	"time"
)

// Meet represents a row in the meets table
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

// GetAllMeets fetches all meets from the database
func GetAllMeets(db *sql.DB) ([]Meet, error) {
	rows, err := db.Query("SELECT * FROM meets")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var meets []Meet
	for rows.Next() {
		var m Meet
		var bookingsOpenDate, startDate, endDate, createdAt, updatedAt sql.NullString
		var spacesAvailable, totalSpaces, meetStewardID, bookable, selfOrganisingLifts, nonLMC, waitingListSpacesAvailable, waitingListTotalSpaces sql.NullInt64

		err := rows.Scan(
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

		// Handle date fields with ISO 8601 format
		if startDate.Valid {
			t, err := time.Parse(time.RFC3339, startDate.String)
			if err != nil {
				// Try simple date format as fallback
				t, err = time.Parse("2006-01-02", startDate.String)
				if err != nil {
					fmt.Printf("Failed to parse start_date: '%s'\n", startDate.String)
				} else {
					m.StartDate = &t
				}
			} else {
				m.StartDate = &t
			}
		}
		if endDate.Valid {
			t, err := time.Parse(time.RFC3339, endDate.String)
			if err != nil {
				// Try simple date format as fallback
				t, err = time.Parse("2006-01-02", endDate.String)
				if err != nil {
					fmt.Printf("Failed to parse end_date: '%s'\n", endDate.String)
				} else {
					m.EndDate = &t
				}
			} else {
				m.EndDate = &t
			}
		}
		if bookingsOpenDate.Valid {
			t, err := time.Parse(time.RFC3339, bookingsOpenDate.String)
			if err != nil {
				// Try simple date format as fallback
				t, err = time.Parse("2006-01-02", bookingsOpenDate.String)
				if err != nil {
					fmt.Printf("Failed to parse bookings_open_date: '%s'\n", bookingsOpenDate.String)
				} else {
					m.BookingsOpenDate = &t
				}
			} else {
				m.BookingsOpenDate = &t
			}
		}
		if createdAt.Valid {
			layouts := []string{time.RFC3339, "2006-01-02 15:04:05", "2006-01-02T15:04:05Z"}
			parsed := false
			for _, layout := range layouts {
				t, err := time.Parse(layout, createdAt.String)
				if err == nil {
					m.CreatedAt = &t
					parsed = true
					break
				}
			}
			if !parsed {
				fmt.Printf("Failed to parse created_at date: '%s'\n", createdAt.String)
			}
		}
		if updatedAt.Valid {
			layouts := []string{time.RFC3339, "2006-01-02 15:04:05", "2006-01-02T15:04:05Z"}
			parsed := false
			for _, layout := range layouts {
				t, err := time.Parse(layout, updatedAt.String)
				if err == nil {
					m.UpdatedAt = &t
					parsed = true
					break
				}
			}
			if !parsed {
				fmt.Printf("Failed to parse updated_at date: '%s'\n", updatedAt.String)
			}
		}

		if spacesAvailable.Valid {
			val := int(spacesAvailable.Int64)
			m.SpacesAvailable = &val
		}
		if totalSpaces.Valid {
			val := int(totalSpaces.Int64)
			m.TotalSpaces = &val
		}
		if meetStewardID.Valid {
			m.MeetStewardID = &meetStewardID.Int64
		}
		if bookable.Valid {
			val := int(bookable.Int64)
			m.Bookable = &val
		}
		if selfOrganisingLifts.Valid {
			val := int(selfOrganisingLifts.Int64)
			m.SelfOrganisingLifts = &val
		}
		if nonLMC.Valid {
			val := int(nonLMC.Int64)
			m.NonLMC = &val
		}
		if waitingListSpacesAvailable.Valid {
			val := int(waitingListSpacesAvailable.Int64)
			m.WaitingListSpacesAvailable = &val
		}
		if waitingListTotalSpaces.Valid {
			val := int(waitingListTotalSpaces.Int64)
			m.WaitingListTotalSpaces = &val
		}

		meets = append(meets, m)
	}

	return meets, nil
}

// GetMeetByID fetches a single meet by ID
func GetMeetByID(db *sql.DB, id string) (*Meet, error) {
	row := db.QueryRow("SELECT * FROM meets WHERE id = ?", id)

	var m Meet
	var bookingsOpenDate, startDate, endDate, createdAt, updatedAt sql.NullString
	var spacesAvailable, totalSpaces, meetStewardID, bookable, selfOrganisingLifts, nonLMC, waitingListSpacesAvailable, waitingListTotalSpaces sql.NullInt64

	err := row.Scan(
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

	// Handle date fields with ISO 8601 format
	if startDate.Valid {
		t, err := time.Parse(time.RFC3339, startDate.String)
		if err != nil {
			// Try simple date format as fallback
			t, err = time.Parse("2006-01-02", startDate.String)
			if err != nil {
				fmt.Printf("Failed to parse start_date: '%s'\n", startDate.String)
			} else {
				m.StartDate = &t
			}
		} else {
			m.StartDate = &t
		}
	}
	if endDate.Valid {
		t, err := time.Parse(time.RFC3339, endDate.String)
		if err != nil {
			// Try simple date format as fallback
			t, err = time.Parse("2006-01-02", endDate.String)
			if err != nil {
				fmt.Printf("Failed to parse end_date: '%s'\n", endDate.String)
			} else {
				m.EndDate = &t
			}
		} else {
			m.EndDate = &t
		}
	}
	if bookingsOpenDate.Valid {
		t, err := time.Parse(time.RFC3339, bookingsOpenDate.String)
		if err != nil {
			// Try simple date format as fallback
			t, err = time.Parse("2006-01-02", bookingsOpenDate.String)
			if err != nil {
				fmt.Printf("Failed to parse bookings_open_date: '%s'\n", bookingsOpenDate.String)
			} else {
				m.BookingsOpenDate = &t
			}
		} else {
			m.BookingsOpenDate = &t
		}
	}
	if createdAt.Valid {
		layouts := []string{time.RFC3339, "2006-01-02 15:04:05", "2006-01-02T15:04:05Z"}
		parsed := false
		for _, layout := range layouts {
			t, err := time.Parse(layout, createdAt.String)
			if err == nil {
				m.CreatedAt = &t
				parsed = true
				break
			}
		}
		if !parsed {
			fmt.Printf("Failed to parse created_at date: '%s'\n", createdAt.String)
		}
	}
	if updatedAt.Valid {
		layouts := []string{time.RFC3339, "2006-01-02 15:04:05", "2006-01-02T15:04:05Z"}
		parsed := false
		for _, layout := range layouts {
			t, err := time.Parse(layout, updatedAt.String)
			if err == nil {
				m.UpdatedAt = &t
				parsed = true
				break
			}
		}
		if !parsed {
			fmt.Printf("Failed to parse updated_at date: '%s'\n", updatedAt.String)
		}
	}

	if spacesAvailable.Valid {
		val := int(spacesAvailable.Int64)
		m.SpacesAvailable = &val
	}
	if totalSpaces.Valid {
		val := int(totalSpaces.Int64)
		m.TotalSpaces = &val
	}
	if meetStewardID.Valid {
		m.MeetStewardID = &meetStewardID.Int64
	}
	if bookable.Valid {
		val := int(bookable.Int64)
		m.Bookable = &val
	}
	if selfOrganisingLifts.Valid {
		val := int(selfOrganisingLifts.Int64)
		m.SelfOrganisingLifts = &val
	}
	if nonLMC.Valid {
		val := int(nonLMC.Int64)
		m.NonLMC = &val
	}
	if waitingListSpacesAvailable.Valid {
		val := int(waitingListSpacesAvailable.Int64)
		m.WaitingListSpacesAvailable = &val
	}
	if waitingListTotalSpaces.Valid {
		val := int(waitingListTotalSpaces.Int64)
		m.WaitingListTotalSpaces = &val
	}

	return &m, nil
}
