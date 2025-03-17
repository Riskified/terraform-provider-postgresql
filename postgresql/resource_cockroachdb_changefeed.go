package postgresql

import (
	"fmt"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"strings"
)

const (
	CDCtableList        = "table_list"
	CDCKafkaUser        = "kafka_user"
	CDCKafkaPass        = "kafka_password"
	CDCTopicPrefix      = "topic_prefix"
	CDCAvroSchemaPrefix = "avro_schema_prefix"
	CDCKafkaUrl         = "kafka_url"
	CDCRegistryUrl      = "registry_url"
	CDCStartFrom        = "start_from"
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
			CDCKafkaUser: {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "kafka user name",
			},
			CDCKafkaPass: {
				Type:        schema.TypeString,
				Optional:    true,
				Computed:    true,
				ForceNew:    true,
				Description: "Sets kafka password",
			},
			CDCTopicPrefix: {
				Type:         schema.TypeString,
				Optional:     true,
				Computed:     true,
				ForceNew:     true,
				Description:  "Sets the topic prefix",
				ValidateFunc: validation.StringIsNotEmpty,
			},
			CDCAvroSchemaPrefix: {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validation.StringIsNotEmpty,
				Description:  "avro schema prefix",
			},
			CDCKafkaUrl: {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validation.StringIsNotEmpty,
				Description:  "kafka url",
			},
			CDCRegistryUrl: {
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
	kafkaUser := d.Get(CDCKafkaUser).(string)
	kafkaPass := d.Get(CDCKafkaPass).(string)
	topicPrefix := d.Get(CDCTopicPrefix).(string)
	avroSchemaPrefix := d.Get(CDCAvroSchemaPrefix).(string)
	kafkaUrl := d.Get(CDCKafkaUrl).(string)
	registryUrl := d.Get(CDCRegistryUrl).(string)
	startFrom := d.Get(CDCStartFrom).(string)

	database := db.client.databaseName
	txn, err := startTransaction(db.client, database)
	if err != nil {
		return fmt.Errorf("Error starting transaction: %w", err)
	}

	sqlConfluentRegistry := fmt.Sprintf("CREATE EXTERNAL CONNECTION confluent_registry_%s AS \"https://%s\";", avroSchemaPrefix, registryUrl)
	if _, err := txn.Exec(sqlConfluentRegistry); err != nil {
		return fmt.Errorf("Error creating EXTERNAL CONNECTION confluent_registry: %w", err)
	}
	if err = txn.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	txn, err = startTransaction(db.client, database)
	sqlKafka := fmt.Sprintf("CREATE EXTERNAL CONNECTION kafka_%s AS \"kafka://%s?topic_prefix=%s_&tls_enabled=true&sasl_enabled=true&sasl_user=%s&sasl_password=%s&sasl_mechanism=PLAIN\"", topicPrefix, kafkaUrl, topicPrefix, kafkaUser, kafkaPass)
	if _, err := txn.Exec(sqlKafka); err != nil {
		return fmt.Errorf("Error creating EXTERNAL CONNECTION kafka: %w", err)
	}
	if err = txn.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
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
		`CREATE CHANGEFEED FOR TABLE %v INTO "external://kafka_%s" WITH updated, %s diff, on_error='pause', format = avro, avro_schema_prefix='%s_', confluent_schema_registry = 'external://confluent_registry_%s'`,
		tableListStr, topicPrefix, cursorClause, avroSchemaPrefix, avroSchemaPrefix,
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

	d.Set(CDCAvroSchemaPrefix, fmt.Sprintf("confluent_registry_%s", avroSchemaPrefix))
	d.Set(CDCTopicPrefix, fmt.Sprintf("kafka_%s", topicPrefix))
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
