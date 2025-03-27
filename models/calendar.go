package models

import (
	"database/sql"
	"fmt"

	ics "github.com/arran4/golang-ical"
)

// GenerateCalendar creates an ICS calendar with all meets
func GenerateCalendar(db *sql.DB) (string, error) {
	// Create a new calendar
	cal := ics.NewCalendar()
	cal.SetMethod(ics.MethodPublish)
	cal.SetProductId("-//Rockhoppers//Meet Calendar//EN")
	cal.SetName("Rockhoppers Meets")
	cal.SetDescription("Calendar of all Rockhoppers meets")
	cal.SetXWRCalName("Rockhoppers Meets")
	cal.SetXWRCalDesc("Calendar of all Rockhoppers meets")

	// Get all meets
	meets, err := GetAllMeets(db)
	if err != nil {
		return "", err
	}

	for _, meet := range meets {
		event := cal.AddEvent(fmt.Sprintf("meet-%d@rockhoppers.org", meet.ID))

		// event.SetCreatedTime(time.Now())
		// event.SetDtStampTime(time.Now())
		// event.SetModifiedAt(time.Now())

		event.SetSummary(meet.Title)

		description := meet.Description
		if meet.MeetStewardNotes != "" {
			description = fmt.Sprintf("%s\n\nSteward Notes: %s", description, meet.MeetStewardNotes)
		}
		if meet.DateNotes != "" {
			description = fmt.Sprintf("%s\n\nDate Notes: %s", description, meet.DateNotes)
		}
		if meet.SpacesAvailable != nil {
			description = fmt.Sprintf("%s\n\nSpaces Available: %d", description, *meet.SpacesAvailable)
		}
		if meet.BookingsOpenDate != nil {
			description = fmt.Sprintf("%s\n\nBookings Open: %s", description, meet.BookingsOpenDate)
		}
		event.SetDescription(description)

		if meet.LocationURL != "" {
			event.SetLocation(meet.LocationURL)
		}

		if meet.StartDate != nil {
			// For all-day events, we need to set the date without time component
			event.SetAllDayStartAt(*meet.StartDate)

			// If end date is available, use it; otherwise use start date
			if meet.EndDate != nil {
				// Add one day to end date for all-day events (iCal convention)
				endDate := meet.EndDate.AddDate(0, 0, 1)
				event.SetAllDayEndAt(endDate)
			} else {
				// Default to one day event
				endDate := meet.StartDate.AddDate(0, 0, 1)
				event.SetAllDayEndAt(endDate)
			}
		}
	}

	// Serialize to string
	return cal.Serialize(), nil
}
