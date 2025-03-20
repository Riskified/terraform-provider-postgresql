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
	if _, err = txn.Exec(fmt.Sprintf("CREATE EXTERNAL CONNECTION %s AS '%s'", connName, connUrl)); err != nil {
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
	return resourceCockroachDBExternalConnectionReadImpl(db, d)
}

func resourceCockroachDBExternalConnectionReadImpl(db *DBConnection, d *schema.ResourceData) error {
	connName := d.Get(ConnName).(string)
	database := db.client.databaseName
	txn, err := startTransaction(db.client, database)
	if err != nil {
		return fmt.Errorf("Error starting transaction: %w", err)
	}
	var connUrl string
	if err := txn.QueryRow(fmt.Sprintf("select connection_uri from [show external connection %s]", connName)).Scan(&connUrl); err != nil {
		return fmt.Errorf("Error reading EXTERNAL CONNECTION: %w", err)
	}
	d.Set(ConnName, connName)
	return nil
}

func resourceCockroachDBExternalConnectionDelete(db *DBConnection, d *schema.ResourceData) error {
	connName := d.Get(ConnName).(string)
	database := db.client.databaseName
	txn, err := startTransaction(db.client, database)
	if err != nil {
		return fmt.Errorf("Error starting transaction: %w", err)
	}
	if _, err = txn.Exec(fmt.Sprintf("DROP EXTERNAL CONNECTION %s", connName)); err != nil {
		return fmt.Errorf("Error deleting EXTERNAL CONNECTION: %w", err)
	}
	if err = txn.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}
	d.SetId("")
	return nil
}

func resourceCockroachDBExternalConnectionExists(db *DBConnection, d *schema.ResourceData) (bool, error) {
	txn, err := startTransaction(db.client, "")
	if err != nil {
		return false, err
	}
	defer deferredRollback(txn)
	return connExists(txn, d.Id())
}

func connExists(db QueryAble, connName string) (bool, error) {
	var exists bool
	if err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM system.external_connections WHERE connection_name = $1);", connName).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}
