package postgresql

import (
	"fmt"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"strings"
)

const (
	CDCtableList              = "table_list"
	CDCKafkaConnectionName    = "kafka_connection_name"
	CDCAvroSchemaPrefix       = "avro_schema_prefix"
	CDCRegistryConnectionName = "registry_connection_name"
	CDCStartFrom              = "start_from"
)

func resourceCockroachDBChangefeed() *schema.Resource {
	return &schema.Resource{
		Create: PGResourceFunc(resourceCockroachDBChangefeedCreate),
		Read:   PGResourceFunc(resourceCockroachDBChangefeedRead),
		Delete: PGResourceFunc(resourceCockroachDBChangefeedDelete),
		//Update: PGResourceFunc(resourceCockroachDBChangefeedUpdate),
		Exists: PGResourceExistsFunc(resourceCockroachDBChangefeedExists),
		//Importer: &schema.ResourceImporter{
		//	StateContext: schema.ImportStatePassthroughContext,
		//},
		Schema: map[string]*schema.Schema{
			CDCtableList: {
				Type:        schema.TypeList,
				Required:    true,
				ForceNew:    true,
				MinItems:    1,
				Elem:        &schema.Schema{Type: schema.TypeString},
				Description: "Sets the tables list to create the changefeed for",
			},
			CDCKafkaConnectionName: {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "kafka user name",
			},
			CDCAvroSchemaPrefix: {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validation.StringIsNotEmpty,
				Description:  "avro schema prefix",
			},
			CDCRegistryConnectionName: {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validation.StringIsNotEmpty,
				Description:  "schema registry url",
			},
			CDCStartFrom: {
				Type:        schema.TypeString,
				Optional:    true,
				ForceNew:    true,
				Description: "cdc start from cursor",
			},
		},
	}
}

func resourceCockroachDBChangefeedCreate(db *DBConnection, d *schema.ResourceData) error {
	tableListInterface := d.Get(CDCtableList).([]interface{})
	kafkaConnectionName := d.Get(CDCKafkaConnectionName).(string)
	registryConnectionName := d.Get(CDCRegistryConnectionName).(string)
	avroSchemaPrefix := d.Get(CDCAvroSchemaPrefix).(string)

	startFrom := d.Get(CDCStartFrom).(string)

	database := db.client.databaseName
	txn, err := startTransaction(db.client, database)
	if err != nil {
		return fmt.Errorf("Error starting transaction: %w", err)
	}

	var cursorClause string
	if startFrom != "" {
		cursorClause = fmt.Sprintf("cursor='%s',", startFrom)
	}

	tableList := make([]string, len(tableListInterface))
	for i, v := range tableListInterface {
		tableList[i] = v.(string)
	}
	tableListStr := strings.Join(tableList, ", ")
	sqlChangefeed := fmt.Sprintf(
		`CREATE CHANGEFEED FOR TABLE %v INTO "external://%s" WITH updated, %s diff, on_error='pause', format = avro, avro_schema_prefix='%s_', confluent_schema_registry = 'external://%s'`,
		tableListStr, kafkaConnectionName, cursorClause, avroSchemaPrefix, registryConnectionName,
	)
	txn, err = startTransaction(db.client, database)
	result, err := txn.Exec(sqlChangefeed)
	if err != nil {
		return fmt.Errorf("Error creating changefeed: %w", err)
	}
	if err = txn.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	// Process the result to return a different value
	rowsAffected, err := result.RowsAffected()
	return fmt.Errorf("rows affected %w", rowsAffected)

	d.SetId("1")
	d.Set(CDCAvroSchemaPrefix, fmt.Sprintf("%s_", avroSchemaPrefix))
	d.Set(CDCRegistryConnectionName, registryConnectionName)
	d.Set(CDCKafkaConnectionName, kafkaConnectionName)

	//// Return the number of rows affected as a string
	return nil
}

func resourceCockroachDBChangefeedRead(db *DBConnection, d *schema.ResourceData) error {
	return nil
}

func resourceCockroachDBChangefeedDelete(db *DBConnection, d *schema.ResourceData) error {
	return nil
}

func resourceCockroachDBChangefeedExists(db *DBConnection, d *schema.ResourceData) (bool, error) {
	return false, nil
}
