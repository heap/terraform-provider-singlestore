package mysql

import (
	"github.com/hashicorp/terraform-plugin-sdk/helper/resource"
	"testing"
)

func TestAccUserPassword_basic(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:     func() { testAccPreCheck(t) },
		Providers:    testAccProviders,
		CheckDestroy: testAccUserCheckDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccUserPasswordConfig_basic,
				Check: resource.ComposeTestCheckFunc(
					testAccUserExists("singlestore_user.test"),
					resource.TestCheckResourceAttr("singlestore_user_password.test", "user", "jdoe"),
					resource.TestCheckResourceAttrSet("singlestore_user_password.test", "encrypted_password"),
				),
			},
		},
	})
}

const testAccUserPasswordConfig_basic = `
resource "singlestore_user" "test" {
  user = "jdoe"
}

resource "singlestore_user_password" "test" {
  user    = "${singlestore_user.test.user}"
  pgp_key = "keybase:joestump"
}
`
