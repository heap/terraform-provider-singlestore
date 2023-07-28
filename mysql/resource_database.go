package mysql

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
)

const defaultPartitionsKeyword = "PARTITIONS "
const defaultS3PathKeyword = "ON S3 "
const defaultConfigKeyword = "CONFIG "
const defaultCredentialsKeyword = "CREDENTIALS '{}'"
const unknownDatabaseErrCode = 1049

func resourceDatabase() *schema.Resource {
	return &schema.Resource{
		Create: CreateDatabase,
		Update: UpdateDatabase,
		Read:   ReadDatabase,
		Delete: DeleteDatabase,
		Importer: &schema.ResourceImporter{
			State: ImportDatabase,
		},
		Schema: map[string]*schema.Schema{
			"name": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},

			"partitions": {
				Type:     schema.TypeInt,
				Optional: true,
				Default:  0,
			},

			"s3_path": {
				Type:     schema.TypeString,
				Optional: true,
				Default:  "",
			},

			"config": {
				Type:     schema.TypeString,
				Optional: true,
				Default:  "{\"region\":\"us-east-1\"}",
			},
		},
	}
}

func CreateDatabase(d *schema.ResourceData, meta interface{}) error {
	db, err := meta.(*MySQLConfiguration).GetDbConn()
	if err != nil {
		return err
	}

	stmtSQL := databaseConfigSQL("CREATE", d)
	log.Println("Executing statement:", stmtSQL)

	_, err = db.Exec(stmtSQL)
	if err != nil {
		return err
	}

	d.SetId(d.Get("name").(string))

	return ReadDatabase(d, meta)
}

func UpdateDatabase(d *schema.ResourceData, meta interface{}) error {
	db, err := meta.(*MySQLConfiguration).GetDbConn()
	if err != nil {
		return err
	}

	stmtSQL := databaseConfigSQL("ALTER", d)
	log.Println("Executing statement:", stmtSQL)

	_, err = db.Exec(stmtSQL)
	if err != nil {
		return err
	}

	return ReadDatabase(d, meta)
}

func ReadDatabase(d *schema.ResourceData, meta interface{}) error {
	db, err := meta.(*MySQLConfiguration).GetDbConn()
	if err != nil {
		return err
	}

	// This is kinda flimsy-feeling, since it depends on the formatting
	// of the SHOW DATABASES output... but this data doesn't seem
	// to be available any other way, so hopefully MySQL keeps this
	// compatible in future releases.

	name := d.Id()
	stmtSQL := "SHOW DATABASES LIKE " + quoteIdentifier(name)

	log.Println("Executing statement:", stmtSQL)
	var _database string
	err = db.QueryRow(stmtSQL).Scan(&_database)
	if err != nil {
		if err == sql.ErrNoRows {
			d.SetId("")
			return nil
		}
		return fmt.Errorf("Error during show databases: %s", err)
	}

	d.Set("name", name)

	return nil
}

func DeleteDatabase(d *schema.ResourceData, meta interface{}) error {
	db, err := meta.(*MySQLConfiguration).GetDbConn()
	if err != nil {
		return err
	}

	name := d.Id()
	s3Path := d.Get("s3_path").(string)
	var verb string

	// bottomless databases need to be "DETACH"ed.
	if s3Path != "" {
		verb = "DETACH"
	} else {
		verb = "DROP"
	}

	stmtSQL := fmt.Sprintf("%s DATABASE %s", verb, quoteIdentifier(name))
	log.Println("Executing statement:", stmtSQL)

	_, err = db.Exec(stmtSQL)
	if err == nil {
		d.SetId("")
	}
	return err
}

func databaseConfigSQL(verb string, d *schema.ResourceData) string {
	name := d.Get("name").(string)
	defaultPartitions := d.Get("partitions").(int)
	defaultS3Path := d.Get("s3_path").(string)
	defaultConfig := d.Get("config").(string)

	var defaultPartitionsClause string
	var defaultS3PathClause string
	var defaultConfigClause string
	var bottomlessStatement string

	if defaultPartitions != 0 {
		defaultPartitionsClause = defaultPartitionsKeyword + strconv.Itoa(defaultPartitions)
	}
	if defaultS3Path != "" {
		// bottomless database
		defaultS3PathClause = defaultS3PathKeyword + quoteIdentifier(defaultS3Path)
		if defaultConfig != "" {
			defaultConfigClause = defaultConfigKeyword + quote(defaultConfig)
		}
		bottomlessStatement = fmt.Sprintf("%s %s %s", defaultS3PathClause, defaultConfigClause, defaultCredentialsKeyword)
	}

	return fmt.Sprintf(
		"%s DATABASE %s %s %s",
		verb,
		quoteIdentifier(name),
		defaultPartitionsClause,
		bottomlessStatement,
	)
}

func extractIdentAfter(sql string, keyword string) string {
	charsetIndex := strings.Index(sql, keyword)
	if charsetIndex != -1 {
		charsetIndex += len(keyword)
		remain := sql[charsetIndex:]
		spaceIndex := strings.IndexRune(remain, ' ')
		return remain[:spaceIndex]
	}

	return ""
}

func ImportDatabase(d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {
	err := ReadDatabase(d, meta)

	if err != nil {
		return nil, err
	}

	return []*schema.ResourceData{d}, nil
}
