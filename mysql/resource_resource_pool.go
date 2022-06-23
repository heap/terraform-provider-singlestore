package mysql

import (
	"fmt"
	"log"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
)

func resourceResourcePool() *schema.Resource {
	return &schema.Resource{
		Create: CreateResourcePool,
		Update: UpdateResourcePool,
		Read:   ReadResourcePool,
		Delete: DeleteResourcePool,
		Importer: &schema.ResourceImporter{
			State: ImportResourcePool,
		},

		Schema: map[string]*schema.Schema{
			"name": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},

			"memory_percentage": {
				Type:     schema.TypeInt,
				Optional: true,
				Default:  0,
			},

			"query_timeout": {
				Type:     schema.TypeInt,
				Optional: true,
				Default:  0,
			},

			"soft_cpu_limit_percentage": {
				Type:     schema.TypeInt,
				Optional: true,
				Default:  0,
			},

			"hard_cpu_limit_percentage": {
				Type:     schema.TypeInt,
				Optional: true,
				Default:  0,
			},

			"max_concurrency": {
				Type:     schema.TypeInt,
				Optional: true,
				Default:  0,
			},

			"max_queue_depth": {
				Type:     schema.TypeInt,
				Optional: true,
				Default:  0,
			},
		},
	}
}
func CreateResourcePool(d *schema.ResourceData, meta interface{}) error {
	db, err := meta.(*MySQLConfiguration).GetDbConn()
	if err != nil {
		return err
	}
	stmtSQL := resourcePoolConfigSQL("CREATE", d)

	log.Println("Executing statement:", stmtSQL)
	_, err = db.Exec(stmtSQL)
	if err != nil {
		return err
	}
	d.SetId(d.Get("name").(string))

	return ReadResourcePool(d, meta)
}

func resourcePoolConfigSQL(verb string, d *schema.ResourceData) string {
	name := d.Get("name").(string)

	var configStatement string

	memory := d.Get("memory_percentage").(int)
	if memory != 0 {
		configStatement = fmt.Sprintf("MEMORY_PERCENTAGE=%d, %s", memory, configStatement)
	}
	timeout := d.Get("query_timeout").(int)
	if timeout != 0 {
		configStatement = fmt.Sprintf("QUERY_TIMEOUT=%d, %s", timeout, configStatement)
	}
	soft_cpu_limit := d.Get("soft_cpu_limit_percentage").(int)
	if soft_cpu_limit != 0 {
		configStatement = fmt.Sprintf("SOFT_CPU_LIMIT_PERCENTAGE=%d, %s", soft_cpu_limit, configStatement)
	}
	hard_cpu_limit := d.Get("hard_cpu_limit_percentage").(int)
	if hard_cpu_limit != 0 {
		configStatement = fmt.Sprintf("HARD_CPU_LIMIT_PERCENTAGE=%d, %s", hard_cpu_limit, configStatement)
	}
	concurrency := d.Get("max_concurrency").(int)
	if concurrency != 0 {
		configStatement = fmt.Sprintf("MAX_CONCURRENCY=%d, %s", concurrency, configStatement)
	}
	queue_depth := d.Get("max_queue_depth").(int)
	if queue_depth != 0 {
		configStatement = fmt.Sprintf("MAX_QUEUE_DEPTH=%d, %s", queue_depth, configStatement)
	}

	stmtSQL := fmt.Sprintf("%s RESOURCE POOL %s",
		verb,
		name,
	)
	if configStatement != "" {
		if verb == "CREATE" {
			stmtSQL = fmt.Sprintf("%s WITH %s", stmtSQL, strings.Trim(configStatement, ", "))
		} else if verb == "ALTER" {
			stmtSQL = fmt.Sprintf("%s SET %s", stmtSQL, strings.Trim(configStatement, ", "))
		}
	}

	return stmtSQL
}

func UpdateResourcePool(d *schema.ResourceData, meta interface{}) error {
	db, err := meta.(*MySQLConfiguration).GetDbConn()
	if err != nil {
		return err
	}

	stmtSQL := resourcePoolConfigSQL("ALTER", d)

	log.Println("Executing statement:", stmtSQL)
	_, err = db.Exec(stmtSQL)
	if err != nil {
		return err
	}

	return nil
}

func ReadResourcePool(d *schema.ResourceData, meta interface{}) error {
	db, err := meta.(*MySQLConfiguration).GetDbConn()
	if err != nil {
		return err
	}

	stmtSQL := fmt.Sprintf("SELECT * FROM information_schema.RESOURCE_POOLS WHERE POOL_NAME='%s'",
		d.Get("name").(string))

	log.Println("Executing statement:", stmtSQL)

	rows, err := db.Query(stmtSQL)
	if err != nil {
		return err
	}
	defer rows.Close()

	if !rows.Next() && rows.Err() == nil {
		d.SetId("")
	}
	return rows.Err()
}

func DeleteResourcePool(d *schema.ResourceData, meta interface{}) error {
	db, err := meta.(*MySQLConfiguration).GetDbConn()
	if err != nil {
		return err
	}

	stmtSQL := fmt.Sprintf("DROP RESOURCE POOL %s",
		d.Get("name").(string))

	log.Println("Executing statement:", stmtSQL)

	_, err = db.Exec(stmtSQL)
	if err == nil {
		d.SetId("")
	}
	return err
}

func ImportResourcePool(d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {
	name := d.Id()

	db, err := meta.(*MySQLConfiguration).GetDbConn()
	if err != nil {
		return nil, err
	}

	var count int
	err = db.QueryRow("SELECT COUNT(1) FROM information_schema.resource_pools WHERE pool_name = ?", name).Scan(&count)

	if err != nil {
		return nil, err
	}

	if count == 0 {
		return nil, fmt.Errorf("Resource pool '%s' not found", name)
	}

	d.Set("name", name)

	return []*schema.ResourceData{d}, nil
}
