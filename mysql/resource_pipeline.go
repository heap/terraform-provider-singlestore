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

			"table_name": {
				Type:     schema.TypeString,
				Optional: true,
				ForceNew: false,
				Default:  "",
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

			"mapping_format": {
				Type:     schema.TypeString,
				Optional: true,
				ForceNew: false,
				Default:  "AVRO",
			},

			"schema": {
				Type:          schema.TypeString,
				Optional:      true,
				Default:       "",
				ConflictsWith: []string{"schema_registry"},
			},

			"schema_registry": {
				Type:          schema.TypeString,
				Optional:      true,
				Default:       "",
				ConflictsWith: []string{"schema"},
			},

			"on_duplicate_key_update": {
				Type:     schema.TypeString,
				Optional: true,
				Default:  "",
			},

			"set": {
				Type:     schema.TypeString,
				Optional: true,
				Default:  "",
			},

			"where": {
				Type:     schema.TypeString,
				Optional: true,
				Default:  "",
			},

			"procedure": {
				Type:     schema.TypeString,
				Optional: true,
				Default:  "",
			},

			"start_pipeline": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  true,
			},

			"resource_pool": {
				Type:     schema.TypeString,
				Optional: true,
				Default:  "",
			},

			"max_partitions_per_batch": {
				Type:     schema.TypeInt,
				Optional: true,
				Default:  0,
			},

			"batch_interval_ms": {
				Type:     schema.TypeInt,
				Optional: true,
				Default:  0,
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
		return fmt.Errorf("error checking if database exists: %s", err)
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
		return fmt.Errorf("error during show pipelines: %s", err)
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
	defaultTableName := d.Get("table_name").(string)
	defaultConfig := d.Get("config").(string)
	defaultTableMapping := d.Get("table_mapping").(string)
	defaultMappingFormat := d.Get("mapping_format").(string)
	defaultSchema := d.Get("schema").(string)
	defaultSchemaRegistry := d.Get("schema_registry").(string)
	defaultOnDuplicateKeyUpdate := d.Get("on_duplicate_key_update").(string)
	defaultSet := d.Get("set").(string)
	defaultWhere := d.Get("where").(string)
	defaultProcedure := d.Get("procedure").(string)
	defaultResourcePool := d.Get("resource_pool").(string)
	defaultMaxPartitionsPerBatch := d.Get("max_partitions_per_batch").(int)
	defaultBatchIntervalMs := d.Get("batch_interval_ms").(int)

	var pipelineClause string
	var skipConstraintErrorClause string
	var tableMappingClause string
	var schemaClause string
	var onDuplicateKeyUpdateClause string
	var setClause string
	var whereClause string
	var tableName string
	var intoStatement string
	var resourcePool string
	var maxPartitionsPerBatch string
	var batchInterval string

	if defaultKafkaEndpoint != "" {
		pipelineClause = fmt.Sprintf("KAFKA '%s/%s' %s", defaultKafkaEndpoint, defaultKafkaTopic, defaultConfig)
	}
	if defaultTableMapping != "" {
		tableMappingClause = fmt.Sprintf("FORMAT %s (%s)", defaultMappingFormat, defaultTableMapping)
	}
	if defaultSchema != "" {
		schemaClause = fmt.Sprintf("SCHEMA '%s'", defaultSchema)
	}
	if defaultSchemaRegistry != "" {
		schemaClause = fmt.Sprintf("SCHEMA REGISTRY '%s'", defaultSchemaRegistry)
	}
	if defaultOnDuplicateKeyUpdate != "" {
		onDuplicateKeyUpdateClause = fmt.Sprintf("ON DUPLICATE KEY UPDATE %s", defaultOnDuplicateKeyUpdate)
	}
	if defaultSet != "" {
		setClause = fmt.Sprintf("SET %s", defaultSet)
	}

	if defaultWhere != "" {
		whereClause = fmt.Sprintf("WHERE %s", defaultWhere)
	}

	if defaultTableName != "" {
		tableName = defaultTableName
	} else {
		tableName = name
	}
	if defaultProcedure == "" {
		intoStatement = fmt.Sprintf("INTO TABLE %s", tableName)
		// pipelines into procedures don't support SKIP CONSTRAINT ERRORS
		skipConstraintErrorClause = "SKIP CONSTRAINT ERRORS"
	} else {
		intoStatement = fmt.Sprintf("INTO PROCEDURE %s", defaultProcedure)
	}
	if defaultResourcePool != "" {
		resourcePool = fmt.Sprintf("RESOURCE POOL %s", defaultResourcePool)
	}
	if defaultBatchIntervalMs != 0 {
		batchInterval = fmt.Sprintf("BATCH_INTERVAL %d", defaultBatchIntervalMs)
	}
	if defaultMaxPartitionsPerBatch != 0 {
		maxPartitionsPerBatch = fmt.Sprintf("MAX_PARTITIONS_PER_BATCH %d", defaultMaxPartitionsPerBatch)
	}

	return fmt.Sprintf(
		"BEGIN; USE %s; %s PIPELINE %s AS LOAD DATA %s %s %s %s %s %s %s %s %s %s %s; COMMIT;",
		databaseName,
		verb,
		name,
		pipelineClause,
		batchInterval,
		maxPartitionsPerBatch,
		resourcePool,
		skipConstraintErrorClause,
		intoStatement,
		tableMappingClause,
		schemaClause,
		setClause,
		whereClause,
		onDuplicateKeyUpdateClause,
	)
}

func ImportPipeline(d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {
	err := ReadPipeline(d, meta)

	if err != nil {
		return nil, err
	}

	return []*schema.ResourceData{d}, nil
}
