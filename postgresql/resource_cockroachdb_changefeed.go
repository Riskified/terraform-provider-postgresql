package postgresql

import (
	"fmt"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"regexp"
	"strings"
	"time"
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
		Update: PGResourceFunc(resourceCockroachDBChangefeedUpdate),
		Exists: PGResourceExistsFunc(resourceCockroachDBChangefeedExists),
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Schema: map[string]*schema.Schema{
			CDCtableList: {
				Type:        schema.TypeList,
				Required:    true,
				MinItems:    1,
				Elem:        &schema.Schema{Type: schema.TypeString},
				Description: "Sets the tables list to create the changefeed for",
			},
			CDCKafkaConnectionName: {
				Type:        schema.TypeString,
				Required:    true,
				Description: "kafka user name",
			},
			CDCAvroSchemaPrefix: {
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: validation.StringIsNotEmpty,
				Description:  "avro schema prefix",
			},
			CDCRegistryConnectionName: {
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: validation.StringIsNotEmpty,
				Description:  "schema registry url",
			},
			CDCStartFrom: {
				Type:        schema.TypeString,
				Optional:    true,
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
	var jobID string
	err = txn.QueryRow(sqlChangefeed).Scan(&jobID)
	if err != nil {
		return fmt.Errorf("Error creating changefeed: %w", err)
	}
	if err = txn.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	d.SetId(jobID)
	d.Set(CDCAvroSchemaPrefix, fmt.Sprintf("%s", avroSchemaPrefix))
	d.Set(CDCRegistryConnectionName, registryConnectionName)
	d.Set(CDCKafkaConnectionName, kafkaConnectionName)
	d.Set(CDCtableList, tableList)

	return nil
}

func resourceCockroachDBChangefeedRead(db *DBConnection, d *schema.ResourceData) error {
	return resourceCockroachDBChangefeedReadImpl(db, d)
	//return nil
}

func resourceCockroachDBChangefeedReadImpl(db *DBConnection, d *schema.ResourceData) error {
	jobID := d.Id()
	var sinkUri, jobTableString, description string
	err := db.QueryRow(fmt.Sprintf("select sink_uri,topics,description from [show changefeed job %s];", jobID)).Scan(&sinkUri, &jobTableString, &description)
	if err != nil {
		return fmt.Errorf("Can't retrieve job details: %w", err)
	}

	// Setting the table list
	currentTableListInterface := d.Get(CDCtableList)
	if len(currentTableListInterface.([]interface{})) == 0 {
		d.Set(CDCtableList, strings.Split(jobTableString, ","))
	} else {
		currentTableList := strings.Split(currentTableListInterface.([]interface{})[0].(string), ",")
		tablesToAdd, tablesToRemove := findTableChanges(currentTableList, strings.Split(jobTableString, ","))
		if len(tablesToAdd) == 0 && len(tablesToRemove) == 0 {
			tableList := Interface2StringList(currentTableListInterface)
			d.Set(CDCtableList, tableList)
		}
	}
	// setting the sink uri
	d.Set(CDCKafkaConnectionName, strings.TrimPrefix(sinkUri, "external://"))

	// setting the avro schema prefix and confluent schema registry
	avroSchemaPrefix, confluentSchemaRegistry := extractAvroDetails(description)
	d.Set(CDCAvroSchemaPrefix, fmt.Sprintf("%s", strings.TrimSuffix(avroSchemaPrefix, "_")))
	d.Set(CDCRegistryConnectionName, confluentSchemaRegistry)

	return nil
}

func resourceCockroachDBChangefeedDelete(db *DBConnection, d *schema.ResourceData) error {
	txn, err := startTransaction(db.client, "")
	if err != nil {
		return err
	}
	defer deferredRollback(txn)
	txn.Exec(fmt.Sprintf("CANCEL JOB %s", d.Id()))
	if err = txn.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}
	d.SetId("")
	return nil

}

func resourceCockroachDBChangefeedExists(db *DBConnection, d *schema.ResourceData) (bool, error) {
	txn, err := startTransaction(db.client, "")
	if err != nil {
		return false, err
	}
	defer deferredRollback(txn)
	return jobExists(txn, d.Id())
}

func jobExists(db QueryAble, jobID string) (bool, error) {
	var jobIDExists string
	err := db.QueryRow("SELECT job_id FROM [SHOW JOBS] WHERE job_id = $1", jobID).Scan(&jobIDExists)
	if err != nil {
		return false, err
	}
	return jobIDExists == jobID, nil
}

func resourceCockroachDBChangefeedUpdate(db *DBConnection, d *schema.ResourceData) error {
	if !d.HasChange(CDCtableList) {
		return nil
	}

	currentTableListInterface, newTableListInterface := d.GetChange(CDCtableList)

	currentTableList := strings.Split(currentTableListInterface.([]interface{})[0].(string), ",")
	newTableList := strings.Split(newTableListInterface.([]interface{})[0].(string), ",")

	tablesToAdd, tablesToRemove := findTableChanges(currentTableList, newTableList)

	jobID := d.Id()
	txn, err := startTransaction(db.client, "")
	if err != nil {
		return fmt.Errorf("Error starting transaction: %w", err)
	}
	defer deferredRollback(txn)

	if len(tablesToAdd) > 0 || len(tablesToRemove) > 0 {
		// Pause the job
		_, err = txn.Exec(fmt.Sprintf("PAUSE JOB %s", jobID))
		if err != nil {
			return fmt.Errorf("Error pausing changefeed job: %w", err)
		}
		if err = txn.Commit(); err != nil {
			return fmt.Errorf("could not commit transaction: %w", err)
		}
		waitForJobStatus(db, jobID, "PAUSED")

		// Alter the changefeed to add new tables
		txn, err = startTransaction(db.client, "")
		for _, table := range tablesToAdd {
			_, err = txn.Exec(fmt.Sprintf("ALTER CHANGEFEED %s ADD %s", jobID, table))
			if err != nil {
				return fmt.Errorf("Error altering changefeed to add table %s: %w", table, err)
			}
		}

		// Alter the changefeed to drop removed tables
		for _, table := range tablesToRemove {
			_, err = txn.Exec(fmt.Sprintf("ALTER CHANGEFEED %s DROP %s", jobID, table))
			if err != nil {
				return fmt.Errorf("Error altering changefeed to drop table %s: %w", table, err)
			}
		}

		// Resume the job
		_, err = txn.Exec(fmt.Sprintf("RESUME JOB %s", jobID))
		if err != nil {
			return fmt.Errorf("Error resuming changefeed job: %w", err)
		}
	}

	if err = txn.Commit(); err != nil {
		return fmt.Errorf("could not commit transaction: %w", err)
	}

	return resourceCockroachDBChangefeedReadImpl(db, d)
}
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func findTableChanges(currentTableList []string, newTableList []string) ([]string, []string) {
	var tablesToAdd []string
	for _, newTable := range newTableList {
		if !contains(currentTableList, newTable) {
			tablesToAdd = append(tablesToAdd, newTable)
		}
	}

	var tablesToRemove []string
	for _, currentTable := range currentTableList {
		if !contains(newTableList, currentTable) {
			tablesToRemove = append(tablesToRemove, currentTable)
		}
	}

	return tablesToAdd, tablesToRemove
}

func waitForJobStatus(db *DBConnection, jobID string, requestedStatus string) error {
	for {
		txn, err := startTransaction(db.client, "")
		if err != nil {
			return fmt.Errorf("Error starting transaction: %w", err)
		}

		var status string
		query := fmt.Sprintf("SELECT status FROM [SHOW JOB %s]", jobID)
		err = txn.QueryRow(query).Scan(&status)
		if err != nil {
			txn.Rollback()
			return fmt.Errorf("Error querying job status: %w", err)
		}

		if strings.ToUpper(status) == strings.ToUpper(requestedStatus) {
			txn.Commit()
			return nil
		}

		txn.Commit()
		time.Sleep(1 * time.Second)
	}
}

func extractAvroDetails(sql string) (string, string) {
	//sql := `CREATE CHANGEFEED FOR TABLE riskx.public.dd, TABLE riskx.public.bb, TABLE cc, TABLE riskx.public.yy INTO 'external://my_kafka_prefix' WITH OPTIONS (avro_schema_prefix = 'my_avro_prefix_', confluent_schema_registry = 'external://confluence_prefix', diff, format = 'avro', on_error = 'pause', updated)`

	// Regular expression to extract the avro_schema_prefix
	avroSchemaPrefixRegex := regexp.MustCompile(`avro_schema_prefix\s*=\s*'([^']*)'`)
	avroSchemaPrefixMatch := avroSchemaPrefixRegex.FindStringSubmatch(sql)
	avroSchemaPrefix := ""
	if len(avroSchemaPrefixMatch) > 1 {
		avroSchemaPrefix = avroSchemaPrefixMatch[1]
	}

	// Regular expression to extract the confluent_schema_registry
	confluentSchemaRegistryRegex := regexp.MustCompile(`confluent_schema_registry\s*=\s*'external://([^']*)'`)
	confluentSchemaRegistryMatch := confluentSchemaRegistryRegex.FindStringSubmatch(sql)
	confluentSchemaRegistry := ""
	if len(confluentSchemaRegistryMatch) > 1 {
		confluentSchemaRegistry = confluentSchemaRegistryMatch[1]
	}

	return avroSchemaPrefix, confluentSchemaRegistry
}

func Interface2StringList(interfaceList interface{}) []string {
	list := interfaceList.([]interface{})
	stringList := make([]string, len(list))
	for i, v := range list {
		stringList[i] = v.(string)
	}
	return stringList

}
