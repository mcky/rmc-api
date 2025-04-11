package models

import (
	"database/sql"
	"time"
)

type SyncMetadata struct {
	TableName    string    `json:"table_name"`
	LastSyncTime time.Time `json:"last_sync_time"`
}

func GetAllSyncMetadata(db *sql.DB) ([]SyncMetadata, error) {
	rows, err := db.Query("SELECT table_name, last_sync_time FROM sync_metadata")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var metadata []SyncMetadata
	for rows.Next() {
		var m SyncMetadata
		var lastSyncStr sql.NullString

		err := rows.Scan(&m.TableName, &lastSyncStr)
		if err != nil {
			return nil, err
		}

		if lastSyncStr.Valid {
			parsedTime, err := time.Parse("2006-01-02 15:04:05", lastSyncStr.String)
			if err == nil {
				m.LastSyncTime = parsedTime
			} else {
				parsedTime, err = time.Parse(time.RFC3339, lastSyncStr.String)
				if err == nil {
					m.LastSyncTime = parsedTime
				} else {
					m.LastSyncTime = time.Now()
				}
			}
		} else {
			m.LastSyncTime = time.Now()
		}

		metadata = append(metadata, m)
	}

	return metadata, nil
}
