package main

import (
	"github.com/hashicorp/terraform-plugin-sdk/plugin"
	"github.com/heap/terraform-provider-singlestore/mysql"
)

func main() {
	plugin.Serve(&plugin.ServeOpts{
		ProviderFunc: mysql.Provider})
}
