package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
)

type TableInfo struct {
	Name    string
	Columns []ColumnInfo
	PK      string
}

type ColumnInfo struct {
	Name     string
	Type     string
	Nullable bool
}

func main() {
	mysqlDSN := os.Getenv("MYSQL_DSN")
	if mysqlDSN == "" {
		log.Fatalf("MYSQL_DSN environment variable is missing")
	}

	sqliteFile := os.Getenv("DB_PATH")
	if sqliteFile == "" {
		log.Fatalf("DB_PATH environment variable is missing")
	}

	mysqlDB, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		log.Fatalf("Failed to connect to MySQL: %v", err)
	}
	defer mysqlDB.Close()

	if err := mysqlDB.Ping(); err != nil {
		log.Fatalf("Failed to ping MySQL: %v", err)
	}

	sqliteDB, err := sql.Open("sqlite3", sqliteFile)
	if err != nil {
		log.Fatalf("Failed to open SQLite database: %v", err)
	}
	defer sqliteDB.Close()

	initSQLiteMetadata(sqliteDB)

	tables, err := getTableList(mysqlDB)
	if err != nil {
		log.Fatalf("Failed to get table list: %v", err)
	}

	for _, tableName := range tables {
		log.Printf("Processing table: %s", tableName)

		tableInfo, err := getTableInfo(mysqlDB, tableName)
		if err != nil {
			log.Printf("Error getting structure for table %s: %v", tableName, err)
			continue
		}

		ensureTableInSQLite(sqliteDB, tableInfo)

		lastSync, err := getLastSyncInfo(sqliteDB, tableName)
		if err != nil {
			log.Printf("Error getting last sync info for %s: %v", tableName, err)
		}

		syncTableData(mysqlDB, sqliteDB, tableInfo, lastSync)
		updateSyncMetadata(sqliteDB, tableName)
	}

	err = ensureAPIKeysTable(sqliteDB)
	if err != nil {
		log.Printf("Error ensuring API keys table: %v", err)
	}

	log.Println("Sync completed successfully")
}

func initSQLiteMetadata(db *sql.DB) {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS sync_metadata (
			table_name TEXT PRIMARY KEY,
			last_sync_time TIMESTAMP,
			row_count INTEGER
		)
	`)
	if err != nil {
		log.Fatalf("Failed to create metadata table: %v", err)
	}
}

func getTableList(db *sql.DB) ([]string, error) {
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		tables = append(tables, tableName)
	}

	return tables, nil
}

func getTableInfo(db *sql.DB, tableName string) (TableInfo, error) {
	rows, err := db.Query(fmt.Sprintf("DESCRIBE %s", tableName))
	if err != nil {
		return TableInfo{}, err
	}
	defer rows.Close()

	tableInfo := TableInfo{Name: tableName}
	for rows.Next() {
		var field, fieldType, null, key, extra, defaultValue sql.NullString
		if err := rows.Scan(&field, &fieldType, &null, &key, &defaultValue, &extra); err != nil {
			return TableInfo{}, err
		}

		col := ColumnInfo{
			Name:     field.String,
			Type:     fieldType.String,
			Nullable: null.String == "YES",
		}
		tableInfo.Columns = append(tableInfo.Columns, col)

		if key.String == "PRI" {
			tableInfo.PK = field.String
		}
	}

	if tableInfo.PK == "" && len(tableInfo.Columns) > 0 {
		tableInfo.PK = tableInfo.Columns[0].Name
		log.Printf("Warning: No primary key found for table %s, using first column %s as key", tableName, tableInfo.PK)
	}

	return tableInfo, nil
}

func ensureTableInSQLite(db *sql.DB, tableInfo TableInfo) {
	var count int
	err := db.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?", tableInfo.Name).Scan(&count)
	if err != nil {
		log.Printf("Error checking if table %s exists in SQLite: %v", tableInfo.Name, err)
		return
	}

	if count == 0 {
		createTableInSQLite(db, tableInfo)
	} else {
		updateTableInSQLite(db, tableInfo)
	}
}

func createTableInSQLite(db *sql.DB, tableInfo TableInfo) {
	var columnDefs []string
	for _, col := range tableInfo.Columns {
		sqlType := mapMySQLTypeToSQLite(col.Type)
		nullConstraint := ""
		if !col.Nullable {
			nullConstraint = " NOT NULL"
		}

		pkConstraint := ""
		if col.Name == tableInfo.PK {
			pkConstraint = " PRIMARY KEY"
		}

		columnDefs = append(columnDefs, fmt.Sprintf("%s %s%s%s", col.Name, sqlType, nullConstraint, pkConstraint))
	}

	createSQL := fmt.Sprintf("CREATE TABLE %s (%s)", tableInfo.Name, strings.Join(columnDefs, ", "))

	_, err := db.Exec(createSQL)
	if err != nil {
		log.Printf("Error creating table %s in SQLite: %v", tableInfo.Name, err)
		log.Printf("SQL: %s", createSQL)
	} else {
		log.Printf("Created new table %s in SQLite", tableInfo.Name)
	}
}

func updateTableInSQLite(db *sql.DB, tableInfo TableInfo) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", tableInfo.Name))
	if err != nil {
		log.Printf("Error getting SQLite table schema for %s: %v", tableInfo.Name, err)
		return
	}
	defer rows.Close()

	existingColumns := make(map[string]ColumnInfo)

	for rows.Next() {
		var cid int
		var name, typeName string
		var notNull, pk int
		var dfltValue interface{}

		if err := rows.Scan(&cid, &name, &typeName, &notNull, &dfltValue, &pk); err != nil {
			log.Printf("Error scanning column info: %v", err)
			continue
		}

		existingColumns[name] = ColumnInfo{
			Name:     name,
			Type:     typeName,
			Nullable: notNull == 0,
		}
	}

	for _, col := range tableInfo.Columns {
		if _, exists := existingColumns[col.Name]; !exists {
			sqlType := mapMySQLTypeToSQLite(col.Type)
			nullConstraint := ""
			if !col.Nullable {
				nullConstraint = " NOT NULL"
			}

			alterSQL := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s%s",
				tableInfo.Name, col.Name, sqlType, nullConstraint)

			_, err := db.Exec(alterSQL)
			if err != nil {
				log.Printf("Error adding column %s to table %s: %v", col.Name, tableInfo.Name, err)
			} else {
				log.Printf("Added column %s to table %s", col.Name, tableInfo.Name)
			}
		}
	}

}

func mapMySQLTypeToSQLite(mysqlType string) string {
	mysqlType = strings.ToUpper(mysqlType)

	baseType := strings.Split(mysqlType, "(")[0]

	switch baseType {
	case "INT", "TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT":
		return "INTEGER"
	case "CHAR", "VARCHAR", "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT", "ENUM", "SET":
		return "TEXT"
	case "FLOAT", "DOUBLE", "DECIMAL":
		return "REAL"
	case "DATE", "DATETIME", "TIMESTAMP", "TIME", "YEAR":
		return "TEXT"
	case "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB":
		return "BLOB"
	default:
		return "TEXT"
	}
}

func getLastSyncInfo(db *sql.DB, tableName string) (map[string]interface{}, error) {
	var lastSyncTime string
	var rowCount int
	err := db.QueryRow("SELECT last_sync_time, row_count FROM sync_metadata WHERE table_name = ?", tableName).Scan(&lastSyncTime, &rowCount)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil // No prior sync
		}
		return nil, err
	}

	return map[string]interface{}{
		"last_sync_time": lastSyncTime,
		"row_count":      rowCount,
	}, nil
}

func syncTableData(mysqlDB *sql.DB, sqliteDB *sql.DB, tableInfo TableInfo, lastSync map[string]interface{}) {
	var columnNames []string
	for _, col := range tableInfo.Columns {
		columnNames = append(columnNames, col.Name)
	}
	columnsStr := strings.Join(columnNames, ", ")

	var rowCount int
	err := mysqlDB.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableInfo.Name)).Scan(&rowCount)
	if err != nil {
		log.Printf("Error getting row count for %s: %v", tableInfo.Name, err)
		rowCount = -1
	}

	if lastSync != nil && rowCount > 0 {
		lastRowCount, ok := lastSync["row_count"].(int)
		if ok && lastRowCount == rowCount {
			log.Printf("Table %s: Row count unchanged (%d rows), skipping full sync", tableInfo.Name, rowCount)
			return
		}
	}

	query := fmt.Sprintf("SELECT %s FROM %s", columnsStr, tableInfo.Name)
	rows, err := mysqlDB.Query(query)
	if err != nil {
		log.Printf("Error querying data from %s: %v", tableInfo.Name, err)
		return
	}
	defer rows.Close()

	tx, err := sqliteDB.Begin()
	if err != nil {
		log.Printf("Error starting SQLite transaction: %v", err)
		return
	}

	placeholders := make([]string, len(columnNames))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	insertSQL := fmt.Sprintf(
		"INSERT OR REPLACE INTO %s (%s) VALUES (%s)",
		tableInfo.Name,
		columnsStr,
		strings.Join(placeholders, ", "),
	)
	stmt, err := tx.Prepare(insertSQL)
	if err != nil {
		log.Printf("Error preparing insert statement: %v", err)
		tx.Rollback()
		return
	}
	defer stmt.Close()

	updatedRows := 0

	for rows.Next() {
		values := make([]interface{}, len(columnNames))
		valuePtrs := make([]interface{}, len(columnNames))

		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			log.Printf("Error scanning row: %v", err)
			continue
		}

		rowValues := make([]interface{}, len(columnNames))
		for i, v := range values {
			if byteValue, ok := v.([]byte); ok {
				rowValues[i] = string(byteValue)
			} else {
				rowValues[i] = v
			}
		}

		_, err = stmt.Exec(rowValues...)
		if err != nil {
			log.Printf("Error upserting row: %v", err)
			continue
		}

		updatedRows++

		if updatedRows%1000 == 0 {
			log.Printf("Table %s: Upserted %d rows so far", tableInfo.Name, updatedRows)
		}
	}

	if err := tx.Commit(); err != nil {
		log.Printf("Error committing transaction: %v", err)
		tx.Rollback()
		return
	}

	log.Printf("Table %s: Synced %d rows", tableInfo.Name, updatedRows)
}

func updateSyncMetadata(db *sql.DB, tableName string) {
	var rowCount int
	err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&rowCount)
	if err != nil {
		log.Printf("Error getting row count for metadata: %v", err)
		rowCount = -1
	}

	now := time.Now().Format(time.RFC3339)
	_, err = db.Exec(
		"INSERT OR REPLACE INTO sync_metadata (table_name, last_sync_time, row_count) VALUES (?, ?, ?)",
		tableName,
		now,
		rowCount,
	)
	if err != nil {
		log.Printf("Error updating sync metadata: %v", err)
	}
}

func ensureAPIKeysTable(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS api_keys (
			member_id INTEGER PRIMARY KEY,
			api_key TEXT NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		return fmt.Errorf("failed to create api_keys table: %v", err)
	}

	log.Println("Checking for members without API keys...")

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("error starting transaction: %v", err)
	}

	query := `
		SELECT m.id 
		FROM members m
		LEFT JOIN api_keys a ON m.id = a.member_id
		WHERE a.api_key IS NULL
	`

	rows, err := tx.Query(query)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("error querying members without API keys: %v", err)
	}
	defer rows.Close()

	stmt, err := tx.Prepare("INSERT INTO api_keys (member_id, api_key) VALUES (?, ?)")
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("error preparing insert statement: %v", err)
	}
	defer stmt.Close()

	keysGenerated := 0
	for rows.Next() {
		var memberID int
		if err := rows.Scan(&memberID); err != nil {
			log.Printf("Error scanning member ID: %v", err)
			continue
		}

		id, err := uuid.NewRandom()
		if err != nil {
			log.Printf("Error generating API key for member %d: %v", memberID, err)
			continue
		}

		apiKey := id.String()
		_, err = stmt.Exec(memberID, apiKey)
		if err != nil {
			log.Printf("Error inserting API key for member %d: %v", memberID, err)
			continue
		}

		keysGenerated++
	}

	if err := tx.Commit(); err != nil {
		tx.Rollback()
		return fmt.Errorf("error committing API keys: %v", err)
	}

	log.Printf("Generated %d new API keys", keysGenerated)
	return nil
}
