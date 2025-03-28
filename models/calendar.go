package models

import (
	"database/sql"
	"fmt"
	"time"

	ics "github.com/arran4/golang-ical"
)

func createCalendarEvent(meet Meet, lastSyncTime time.Time) *ics.VEvent {
	event := ics.NewEvent(fmt.Sprintf("meet-%d@rockhoppers.org", meet.ID))

	event.SetSummary(meet.Title)

	description := meet.Description

	description = fmt.Sprintf("%s\n\n%s", description, meet.WebsiteURL)

	if meet.MeetStewardNotes != "" {
		description = fmt.Sprintf("%s\n\nSteward Notes: %s", description, meet.MeetStewardNotes)
	}
	if meet.DateNotes != "" {
		description = fmt.Sprintf("%s\n\nDate Notes: %s", description, meet.DateNotes)
	}
	if meet.SpacesAvailable != nil {
		if *meet.SpacesAvailable == 0 {
			description = fmt.Sprintf("%s\n\nMeet Full", description)
		} else {
			description = fmt.Sprintf("%s\n\nSpaces Available: %d", description, *meet.SpacesAvailable)
		}
	}
	if meet.BookingsOpenDate != nil {
		description = fmt.Sprintf("%s\n\nBookings are open from %s.", description, meet.BookingsOpenDate.Format("2 January 2006"))
	}

	description = fmt.Sprintf("%s\n\nLast Sync: %s", description, lastSyncTime.Format("2 January 2006 15:04:05"))

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

	return event
}

func GenerateCalendar(db *sql.DB) (string, error) {
	cal := ics.NewCalendar()
	cal.SetMethod(ics.MethodPublish)
	cal.SetProductId("-//Rockhoppers//Meet Calendar//EN")
	cal.SetName("Rockhoppers Meets")
	cal.SetDescription("Calendar of all Rockhoppers meets")
	cal.SetXWRCalName("Rockhoppers Meets")
	cal.SetXWRCalDesc("Calendar of all Rockhoppers meets")

	meets, err := GetAllMeets(db)

	var lastSyncTime time.Time
	err = db.QueryRow("SELECT last_sync_time FROM sync_metadata WHERE table_name = 'meets'").Scan(&lastSyncTime)
	if err != nil {
		return "", err
	}

	for _, meet := range meets {
		event := createCalendarEvent(meet, lastSyncTime)
		cal.AddVEvent(event)
	}

	return cal.Serialize(), nil
}
