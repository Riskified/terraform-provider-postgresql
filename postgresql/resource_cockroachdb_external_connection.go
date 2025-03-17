package postgresql

import (
	"fmt"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
)

const (
	ConnName = "connection_name"
	ConnUrl  = "connection_url"
)

func resourceCockroachDBExternalConnection() *schema.Resource {
	return &schema.Resource{
		Create: PGResourceFunc(resourceCockroachDBExternalConnectionCreate),
		Read:   PGResourceFunc(resourceCockroachDBExternalConnectionRead),
		Delete: PGResourceFunc(resourceCockroachDBExternalConnectionDelete),
		Exists: PGResourceExistsFunc(resourceCockroachDBExternalConnectionExists),
		Schema: map[string]*schema.Schema{
			ConnName: {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "Sets the connection name",
			},
			ConnUrl: {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "Sets the connection url",
			},
		},
	}
}
func resourceCockroachDBExternalConnectionCreate(db *DBConnection, d *schema.ResourceData) error {
	connName := d.Get(ConnName).(string)
	connUrl := d.Get(ConnUrl).(string)
	database := db.client.databaseName
	txn, err := startTransaction(db.client, database)
	if err != nil {
		return fmt.Errorf("Error starting transaction: %w", err)
	}
	if _, err = txn.Exec("CREATE EXTERNAL CONNECTION " + connName + " AS https://'" + connUrl + "'"); err != nil {
		return fmt.Errorf("Error creating EXTERNAL CONNECTION confluent_registry: %w", err)
	}
	if err = txn.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}
	d.SetId(connName)
	d.Set(ConnUrl, connUrl)
	return nil
}

func resourceCockroachDBExternalConnectionRead(db *DBConnection, d *schema.ResourceData) error {
	return nil
}

func resourceCockroachDBExternalConnectionDelete(db *DBConnection, d *schema.ResourceData) error {
	return nil
}

func resourceCockroachDBExternalConnectionExists(db *DBConnection, d *schema.ResourceData) (bool, error) {
	return false, nil
}
