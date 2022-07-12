package mysql

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
)

func resourcePipeline() *schema.Resource {
	return &schema.Resource{
		Create: CreatePipeline,
		Update: UpdatePipeline,
		Read:   ReadPipeline,
		Delete: DeletePipeline,
		Importer: &schema.ResourceImporter{
			State: ImportPipeline,
		},
		Schema: map[string]*schema.Schema{
			"name": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},

			"database_name": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},

			"kafka_endpoint": {
				Type:     schema.TypeString,
				Optional: true,
				ForceNew: true,
				Default:  "",
			},

			"kafka_topic": {
				Type:     schema.TypeString,
				Optional: true,
				ForceNew: true,
				Default:  "",
			},

			"config": {
				Type:     schema.TypeString,
				Optional: true,
				Default:  "",
			},

			"table_mapping": {
				Type:     schema.TypeString,
				Optional: true,
				Default:  "",
			},

			"schema": {
				Type:     schema.TypeString,
				Optional: true,
				Default:  "",
			},

			"start_pipeline": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  true,
			},
		},
	}
}

func CreatePipeline(d *schema.ResourceData, meta interface{}) error {
	db, err := meta.(*MySQLConfiguration).GetDbConn()
	if err != nil {
		return err
	}

	name := d.Get("name").(string)
	startPieline := d.Get("start_pipeline").(bool)

	stmtSQL := pipelineConfigSQL("CREATE", d)
	log.Println("Executing statement:", stmtSQL)

	_, err = db.Exec(stmtSQL)
	if err != nil {
		return err
	}

	if startPieline {
		startSQL := fmt.Sprintf("START PIPELINE %s", name)
		log.Println("Executing statement:", startSQL)

		_, err = db.Exec(startSQL)
		if err != nil {
			return err
		}
	}

	d.SetId(name)

	return ReadPipeline(d, meta)
}

func UpdatePipeline(d *schema.ResourceData, meta interface{}) error {
	db, err := meta.(*MySQLConfiguration).GetDbConn()
	if err != nil {
		return err
	}

	stmtSQL := pipelineConfigSQL("CREATE OR REPLACE", d)
	log.Println("Executing statement:", stmtSQL)

	_, err = db.Exec(stmtSQL)
	if err != nil {
		return err
	}

	return ReadPipeline(d, meta)
}

func ReadPipeline(d *schema.ResourceData, meta interface{}) error {
	db, err := meta.(*MySQLConfiguration).GetDbConn()
	if err != nil {
		return err
	}

	// This is kinda flimsy-feeling, since it depends on the formatting
	// of the SHOW PIPELINES output... but this data doesn't seem
	// to be available any other way, so hopefully MySQL keeps this
	// compatible in future releases.

	name := d.Id()
	databaseName := d.Get("database_name").(string)

	exists, err := databaseExists(databaseName, meta)
	if err != nil {
		return fmt.Errorf("Error checking if database exists: %s", err)
	}
	if !exists {
		d.SetId("")
		return nil
	}

	stmtSQL := fmt.Sprintf("BEGIN; USE %s; SHOW PIPELINES LIKE %s; COMMIT;", databaseName, quoteIdentifier(name))
	log.Println("Executing statement:", stmtSQL)
	var _database string
	var _state string
	var _scheduled string
	err = db.QueryRow(stmtSQL).Scan(&_database, &_state, &_scheduled)
	if err != nil {
		if err == sql.ErrNoRows {
			d.SetId("")
			return nil
		}
		return fmt.Errorf("Error during show pipelines: %s", err)
	}

	d.Set("name", name)

	return nil
}

func DeletePipeline(d *schema.ResourceData, meta interface{}) error {
	db, err := meta.(*MySQLConfiguration).GetDbConn()
	if err != nil {
		return err
	}

	name := d.Id()
	databaseName := d.Get("database_name").(string)
	stmtSQL := fmt.Sprintf("BEGIN; USE %s; DROP PIPELINE %s; COMMIT;", databaseName, name)
	log.Println("Executing statement:", stmtSQL)

	_, err = db.Exec(stmtSQL)
	if err == nil {
		d.SetId("")
	}
	return err
}

func pipelineConfigSQL(verb string, d *schema.ResourceData) string {
	name := d.Get("name").(string)
	databaseName := d.Get("database_name").(string)
	defaultKafkaEndpoint := d.Get("kafka_endpoint").(string)
	defaultKafkaTopic := d.Get("kafka_topic").(string)
	defaultConfig := d.Get("config").(string)
	defaultTableMapping := d.Get("table_mapping").(string)
	defaultSchema := d.Get("schema").(string)

	var pipelineClause string
	var tableMappingClause string
	var schemaClause string

	if defaultKafkaEndpoint != "" {
		pipelineClause = fmt.Sprintf("KAFKA '%s/%s' %s", defaultKafkaEndpoint, defaultKafkaTopic, defaultConfig)
	}
	if defaultTableMapping != "" {
		tableMappingClause = fmt.Sprintf("FORMAT AVRO (%s)", defaultTableMapping)
	}
	if defaultSchema != "" {
		schemaClause = fmt.Sprintf("SCHEMA '%s'", defaultSchema)
	}

	return fmt.Sprintf(
		"BEGIN; USE %s; %s PIPELINE %s AS LOAD DATA %s INTO TABLE %s %s %s; COMMIT;",
		databaseName,
		verb,
		name,
		pipelineClause,
		name,
		tableMappingClause,
		schemaClause,
	)
}

func ImportPipeline(d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {
	err := ReadPipeline(d, meta)

	if err != nil {
		return nil, err
	}

	return []*schema.ResourceData{d}, nil
}
