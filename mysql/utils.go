package mysql

import (
	"crypto/sha256"
	"database/sql"
	"fmt"
	"log"
)

func hashSum(contents interface{}) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(contents.(string))))
}

func databaseExists(databaseName string, meta interface{}) (exists bool, err error) {
	db, err := meta.(*MySQLConfiguration).GetDbConn()
	if err != nil {
		return false, err
	}

	stmtSQL := "SHOW DATABASES LIKE " + quoteIdentifier(databaseName)

	log.Println("Executing statement:", stmtSQL)
	var _database string
	err = db.QueryRow(stmtSQL).Scan(&_database)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, err
		}
		return false, err
	}
	return true, err
}
