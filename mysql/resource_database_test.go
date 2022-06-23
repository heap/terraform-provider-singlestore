package mysql

import (
	"fmt"
	"testing"

	"github.com/go-sql-driver/mysql"

	"github.com/hashicorp/terraform-plugin-sdk/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/terraform"
)

func TestAccDatabase(t *testing.T) {
	dbName := "terraform_acceptance_test"

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccDatabaseCheckDestroy(dbName),
		Steps: []resource.TestStep{
			{
				Config: testAccDatabaseConfig_basic(dbName),
				Check: testAccDatabaseCheck_basic(
					"singlestore_database.test", dbName,
				),
			},
		},
	})
}

func TestAccDatabase_collationChange(t *testing.T) {
	dbName := "terraform_acceptance_test"

	resourceName := "singlestore_database.test"

	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccDatabaseCheckDestroy(dbName),
		Steps: []resource.TestStep{
			{
				Config: testAccDatabaseConfig_full(dbName),
				Check: resource.ComposeTestCheckFunc(
					testAccDatabaseCheck_full("singlestore_database.test", dbName),
				),
			},
			{
				ResourceName:      resourceName,
				ImportState:       true,
				ImportStateVerify: true,
			},
			{
				Config: testAccDatabaseConfig_full(dbName),
				Check: resource.ComposeTestCheckFunc(
					testAccDatabaseCheck_full(resourceName, dbName),
				),
			},
		},
	})
}

func testAccDatabaseCheck_basic(rn string, name string) resource.TestCheckFunc {
	return testAccDatabaseCheck_full(rn, name)
}

func testAccDatabaseCheck_full(rn string, name string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs, ok := s.RootModule().Resources[rn]
		if !ok {
			return fmt.Errorf("resource not found: %s", rn)
		}

		if rs.Primary.ID == "" {
			return fmt.Errorf("database id not set")
		}

		db, err := connectToMySQL(testAccProvider.Meta().(*MySQLConfiguration))
		if err != nil {
			return err
		}

		var _name, createSQL string
		err = db.QueryRow(fmt.Sprintf("SHOW CREATE DATABASE %s", name)).Scan(&_name, &createSQL)
		if err != nil {
			return fmt.Errorf("error reading database: %s", err)
		}

		return nil
	}
}

func testAccDatabaseCheckDestroy(name string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		db, err := connectToMySQL(testAccProvider.Meta().(*MySQLConfiguration))
		if err != nil {
			return err
		}

		var _name, createSQL string
		err = db.QueryRow(fmt.Sprintf("SHOW CREATE DATABASE %s", name)).Scan(&_name, &createSQL)
		if err == nil {
			return fmt.Errorf("database still exists after destroy")
		}

		if mysqlErr, ok := err.(*mysql.MySQLError); ok {
			if mysqlErr.Number == unknownDatabaseErrCode {
				return nil
			}
		}

		return fmt.Errorf("got unexpected error: %s", err)
	}
}

func testAccDatabaseConfig_basic(name string) string {
	return testAccDatabaseConfig_full(name)
}

func testAccDatabaseConfig_full(name string) string {
	return fmt.Sprintf(`
resource "singlestore_database" "test" {
    name = "%s"
}`, name)
}
