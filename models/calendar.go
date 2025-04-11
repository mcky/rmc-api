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

	description = fmt.Sprintf(
		"%s\n\nNote: Meet details and availability correct as of last sync: %s",
		description,
		lastSyncTime.Format("2 January 2006 15:04:05"),
	)

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

func createSocialCalendarEvent(social Social, lastSyncTime time.Time) *ics.VEvent {
	event := ics.NewEvent(fmt.Sprintf("social-%d@rockhoppers.org", social.ID))

	event.SetSummary(social.Title)

	description := social.Description

	if social.Speaker != "" {
		description = fmt.Sprintf("%s\n\nSpeaker: %s", description, social.Speaker)
	}

	description = fmt.Sprintf("%s\n\nLocation: %s", description, social.Location)

	if social.StartTime != "" {
		description = fmt.Sprintf("%s\n\nTime: %s", description, social.StartTime)
	}

	description = fmt.Sprintf(
		"%s\n\nNote: Meet details and availability correct as of last sync: %s",
		description,
		lastSyncTime.Format("2 January 2006 15:04:05"),
	)

	event.SetDescription(description)
	event.SetLocation(social.Location)

	if social.StartDate != nil {
		// For all-day events, we need to set the date without time component
		event.SetAllDayStartAt(*social.StartDate)

		// Default to one day event
		endDate := social.StartDate.AddDate(0, 0, 1)
		event.SetAllDayEndAt(endDate)
	}

	return event
}

func GenerateCalendar(db *sql.DB, member_id ...string) (string, error) {
	cal := ics.NewCalendar()
	cal.SetMethod(ics.MethodPublish)
	cal.SetProductId("-//Rockhoppers//Events Calendar//EN")
	cal.SetName("Rockhoppers meets & socials")
	cal.SetDescription("Calendar of all Rockhoppers events")
	cal.SetXWRCalName("Rockhoppers meets & socials")
	cal.SetXWRCalDesc("Calendar of all Rockhoppers events")

	meets, err := GetAllMeets(db)
	if err != nil {
		return "", err
	}

	var meetsLastSyncTime time.Time
	err = db.QueryRow("SELECT last_sync_time FROM sync_metadata WHERE table_name = 'meets'").Scan(&meetsLastSyncTime)
	if err != nil {
		meetsLastSyncTime = time.Now() // Default to current time if no sync time available
	}

	for _, meet := range meets {
		event := createCalendarEvent(meet, meetsLastSyncTime)
		cal.AddVEvent(event)
	}

	socials, err := GetAllSocials(db)
	if err != nil {
		return "", err
	}

	var socialsLastSyncTime time.Time
	err = db.QueryRow("SELECT last_sync_time FROM sync_metadata WHERE table_name = 'socials'").Scan(&socialsLastSyncTime)
	if err != nil {
		socialsLastSyncTime = time.Now() // Default to current time if no sync time available
	}

	for _, social := range socials {
		event := createSocialCalendarEvent(social, socialsLastSyncTime)
		cal.AddVEvent(event)
	}

	return cal.Serialize(), nil
}
