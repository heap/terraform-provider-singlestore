package mysql

import (
	"fmt"
	"log"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
)

func resourceSql() *schema.Resource {
	return &schema.Resource{
		Create: CreateSql,
		Read:   ReadSql,
		Update: UpdateSql,
		Delete: DeleteSql,

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
			"create_sql": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"update_sql": {
				Type:     schema.TypeString,
				Optional: true,
				Default:  "select 1",
			},
			"delete_sql": {
				Type:     schema.TypeString,
				Optional: true,
				Default:  "select 1",
			},
		},
	}
}

func CreateSql(d *schema.ResourceData, meta interface{}) error {
	db, err := meta.(*MySQLConfiguration).GetDbConn()
	if err != nil {
		return err
	}
	name := d.Get("name").(string)
	databaseName := d.Get("database_name").(string)
	createSql := fmt.Sprintf("BEGIN; USE %s; %s COMMIT;", databaseName, d.Get("create_sql").(string))

	log.Println("Executing SQL", createSql)

	_, err = db.Exec(createSql)

	if err != nil {
		return err
	}

	d.SetId(name)

	return nil
}

func UpdateSql(d *schema.ResourceData, meta interface{}) error {
	db, err := meta.(*MySQLConfiguration).GetDbConn()
	if err != nil {
		return err
	}
	name := d.Get("name").(string)
	databaseName := d.Get("database_name").(string)
	updateSql := fmt.Sprintf("BEGIN; USE %s; %s COMMIT;", databaseName, d.Get("update_sql").(string))

	log.Println("Executing SQL", updateSql)

	_, err = db.Exec(updateSql)

	if err != nil {
		return err
	}

	d.SetId(name)

	return nil
}

func ReadSql(d *schema.ResourceData, meta interface{}) error {
	return nil
}

func DeleteSql(d *schema.ResourceData, meta interface{}) error {
	db, err := meta.(*MySQLConfiguration).GetDbConn()
	if err != nil {
		return err
	}
	databaseName := d.Get("database_name").(string)
	delete_sql := fmt.Sprintf("BEGIN; USE %s; %s COMMIT;", databaseName, d.Get("delete_sql").(string))

	log.Println("Executing SQL:", delete_sql)

	_, err = db.Exec(delete_sql)

	if err == nil {
		d.SetId("")
	}

	return err
}
