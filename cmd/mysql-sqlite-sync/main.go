package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
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

	sqliteFile := os.Getenv("SQLITE_FILE")
	if sqliteFile == "" {
		log.Fatalf("SQLITE_FILE environment variable is missing")
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
	
	log.Println("Sync completed successfully")
}

func initSQLiteMetadata(db *sql.DB) {
	// Create sync_metadata table to track last sync times
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
	// Get column information
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

		// Identify primary key
		if key.String == "PRI" {
			tableInfo.PK = field.String
		}
	}

	// If no primary key found, set a default
	if tableInfo.PK == "" && len(tableInfo.Columns) > 0 {
		tableInfo.PK = tableInfo.Columns[0].Name
		log.Printf("Warning: No primary key found for table %s, using first column %s as key", tableName, tableInfo.PK)
	}

	return tableInfo, nil
}

func ensureTableInSQLite(db *sql.DB, tableInfo TableInfo) {
	// Check if table exists
	var count int
	err := db.QueryRow("SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?", tableInfo.Name).Scan(&count)
	if err != nil {
		log.Printf("Error checking if table %s exists in SQLite: %v", tableInfo.Name, err)
		return
	}

	if count == 0 {
		// Table doesn't exist, create it
		createTableInSQLite(db, tableInfo)
	} else {
		// Table exists, check if schema needs updating
		updateTableInSQLite(db, tableInfo)
	}
}

func createTableInSQLite(db *sql.DB, tableInfo TableInfo) {
	// Build CREATE TABLE statement for SQLite
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
	// Get current columns in SQLite
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", tableInfo.Name))
	if err != nil {
		log.Printf("Error getting SQLite table schema for %s: %v", tableInfo.Name, err)
		return
	}
	defer rows.Close()

	// Map to store existing columns
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

	// Check for missing columns that need to be added
	for _, col := range tableInfo.Columns {
		if _, exists := existingColumns[col.Name]; !exists {
			// Column doesn't exist, add it
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

	// Extract base type without size constraints
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
	// Check if we have prior sync data
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
	// Get all column names
	var columnNames []string
	for _, col := range tableInfo.Columns {
		columnNames = append(columnNames, col.Name)
	}
	columnsStr := strings.Join(columnNames, ", ")

	// Get row count for this table in MySQL
	var rowCount int
	err := mysqlDB.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", tableInfo.Name)).Scan(&rowCount)
	if err != nil {
		log.Printf("Error getting row count for %s: %v", tableInfo.Name, err)
		rowCount = -1
	}

	// Check if table data likely changed (by comparing row counts)
	if lastSync != nil && rowCount > 0 {
		lastRowCount, ok := lastSync["row_count"].(int)
		if ok && lastRowCount == rowCount {
			log.Printf("Table %s: Row count unchanged (%d rows), skipping full sync", tableInfo.Name, rowCount)
			return
		}
	}

	// Query all rows
	query := fmt.Sprintf("SELECT %s FROM %s", columnsStr, tableInfo.Name)
	rows, err := mysqlDB.Query(query)
	if err != nil {
		log.Printf("Error querying data from %s: %v", tableInfo.Name, err)
		return
	}
	defer rows.Close()

	// Begin transaction for SQLite operations
	tx, err := sqliteDB.Begin()
	if err != nil {
		log.Printf("Error starting SQLite transaction: %v", err)
		return
	}

	// Prepare statement for upserting rows (INSERT OR REPLACE)
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

	// Process each row
	updatedRows := 0

	for rows.Next() {
		// Prepare scan targets
		values := make([]interface{}, len(columnNames))
		valuePtrs := make([]interface{}, len(columnNames))

		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			log.Printf("Error scanning row: %v", err)
			continue
		}

		// Convert []byte to string for better SQLite compatibility
		rowValues := make([]interface{}, len(columnNames))
		for i, v := range values {
			if byteValue, ok := v.([]byte); ok {
				rowValues[i] = string(byteValue)
			} else {
				rowValues[i] = v
			}
		}

		// Insert or replace row in SQLite
		_, err = stmt.Exec(rowValues...)
		if err != nil {
			log.Printf("Error upserting row: %v", err)
			continue
		}

		updatedRows++

		// Log progress periodically
		if updatedRows%1000 == 0 {
			log.Printf("Table %s: Upserted %d rows so far", tableInfo.Name, updatedRows)
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		log.Printf("Error committing transaction: %v", err)
		tx.Rollback()
		return
	}

	log.Printf("Table %s: Synced %d rows", tableInfo.Name, updatedRows)
}

func updateSyncMetadata(db *sql.DB, tableName string) {
	// Get current row count
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
