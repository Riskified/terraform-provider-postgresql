package postgresql

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

const (
	CDCtableList              = "table_list"
	CDCKafkaConnectionName    = "kafka_connection_name"
	CDCAvroSchemaPrefix       = "avro_schema_prefix"
	CDCRegistryConnectionName = "registry_connection_name"
	CDCStartFrom              = "start_from"
	CDCInitialScan            = "initial_scan"
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
				Description: "kafka external connection name",
				ForceNew:    true,
			},
			CDCAvroSchemaPrefix: {
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: validation.StringIsNotEmpty,
				Description:  "avro schema prefix",
				ForceNew:     true,
			},
			CDCRegistryConnectionName: {
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: validation.StringIsNotEmpty,
				Description:  "schema registry external connection name",
				ForceNew:     true,
			},
			CDCStartFrom: {
				Type:         schema.TypeString,
				Optional:     true,
				Description:  "cdc start from cursor",
				ForceNew:     true,
				ValidateFunc: validateDateTime,
			},
			CDCInitialScan: {
				Type:         schema.TypeString,
				Optional:     true,
				Description:  "cdc initial scan",
				ValidateFunc: validation.StringInSlice([]string{"yes", "no"}, false),
				ForceNew:     true,
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

	var cursorClause string
	if startFrom != "" {
		cursorClause = fmt.Sprintf("cursor='%s',", startFrom)
	}

	var initialScanClause string
	if d.Get(CDCInitialScan).(string) == "yes" {
		initialScanClause = "initial_scan = 'yes',"
	} else {
		initialScanClause = "initial_scan = 'no',"
	}
	tableList := Interface2StringList(tableListInterface)
	tableListStr := strings.Join(tableList, ", ")
	sqlChangefeed := fmt.Sprintf(
		`CREATE CHANGEFEED FOR TABLE %v INTO "external://%s" WITH %s updated, %s diff, on_error='pause', format = avro, avro_schema_prefix='%s_', confluent_schema_registry = 'external://%s'`,
		tableListStr, kafkaConnectionName, initialScanClause, cursorClause, avroSchemaPrefix, registryConnectionName,
	)
	txn, err := startTransaction(db.client, database)
	if err != nil {
		return fmt.Errorf("error starting transaction: %w", err)
	}
	var jobID string
	if err = txn.QueryRow(sqlChangefeed).Scan(&jobID); err != nil {
		return fmt.Errorf("error creating changefeed: %w", err)
	}
	if db.dbType != dbTypeCockroachdb {
		if err = txn.Commit(); err != nil {
			return fmt.Errorf("could not commit transaction: %w", err)
		}
	}

	d.SetId(jobID)
	d.Set(CDCAvroSchemaPrefix, avroSchemaPrefix)
	d.Set(CDCRegistryConnectionName, registryConnectionName)
	d.Set(CDCKafkaConnectionName, kafkaConnectionName)
	d.Set(CDCtableList, tableList)

	return nil
}

func resourceCockroachDBChangefeedRead(db *DBConnection, d *schema.ResourceData) error {
	exists, err := resourceCockroachDBChangefeedExists(db, d)
	if err != nil {
		return err
	}
	if !exists {
		return resourceCockroachDBChangefeedCreate(db, d)
	}
	return resourceCockroachDBChangefeedReadImpl(db, d)
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
		// in case we're in import mode
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
	avroSchemaPrefix, confluentSchemaRegistry, initialScanValue, cursorValue := extractDetails(description)
	d.Set(CDCAvroSchemaPrefix, strings.TrimSuffix(avroSchemaPrefix, "_"))
	d.Set(CDCRegistryConnectionName, confluentSchemaRegistry)
	if initialScanValue == "yes" {
		d.Set(CDCInitialScan, "yes")
	} else {
		d.Set(CDCInitialScan, "no")
	}
	if cursorValue != "" {
		d.Set(CDCStartFrom, cursorValue)
	}

	return nil
}

func resourceCockroachDBChangefeedDelete(db *DBConnection, d *schema.ResourceData) error {
	txn, err := startTransaction(db.client, "")
	if err != nil {
		return err
	}
	defer deferredRollback(txn)
	if _, err = txn.Exec(fmt.Sprintf("CANCEL JOB %s", d.Id())); err != nil {
		return fmt.Errorf("could not cancel job: %w", err)
	}
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
		if err = waitForJobStatus(db, jobID, "PAUSED"); err != nil {
			return fmt.Errorf("error waiting for job status to be paused: %w", err)
		}

		// Alter the changefeed to add new tables
		txn, err = startTransaction(db.client, "")
		if err != nil {
			return fmt.Errorf("error starting transaction: %w", err)
		}
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

// helper functions
func jobExists(db QueryAble, jobID string) (bool, error) {
	var jobIDExists string
	err := db.QueryRow(fmt.Sprintf("SELECT job_id FROM [SHOW changefeed JOB %s]  where status='running';", jobID)).Scan(&jobIDExists)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return jobIDExists == jobID, nil
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

func waitForJobStatus(db *DBConnection, jobID string, requestedStatus string, timeoutMinutes ...int) error {
	timeout := 10
	if len(timeoutMinutes) > 0 {
		timeout = timeoutMinutes[0]
	}

	timeoutChan := time.After(time.Duration(timeout) * time.Minute)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-timeoutChan:
			return fmt.Errorf("timeout reached while waiting for job status to be %s", requestedStatus)
		case <-ticker.C:
			txn, err := startTransaction(db.client, "")
			if err != nil {
				return fmt.Errorf("error starting transaction: %w", err)
			}

			var status string
			query := fmt.Sprintf("SELECT status FROM [SHOW JOB %s]", jobID)
			err = txn.QueryRow(query).Scan(&status)
			if err != nil {
				err = txn.Rollback()
				if err != nil {
					return err
				}
				return fmt.Errorf("error querying job status: %w", err)
			}

			if strings.EqualFold(status, requestedStatus) {
				err = txn.Commit()
				if err != nil {
					return err
				}
				return nil
			}

			err = txn.Commit()
			if err != nil {
				return err
			}
		}
	}
}

func extractDetails(sql string) (string, string, string, string) {
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

	// Regular expression to extract the initial_scan
	initialScanRegex := regexp.MustCompile(`initial_scan\s*=\s*'([^']*)'`)
	initialScanMatch := initialScanRegex.FindStringSubmatch(sql)
	initialScan := ""
	if len(initialScanMatch) > 1 {
		initialScan = initialScanMatch[1]
	}

	// Regular expression to extract the cursor
	cursorRegex := regexp.MustCompile(`cursor\s*=\s*'([^']*)'`)
	cursorMatch := cursorRegex.FindStringSubmatch(sql)
	cursor := ""
	if len(cursorMatch) > 1 {
		cursor = cursorMatch[1]
	}

	return avroSchemaPrefix, confluentSchemaRegistry, initialScan, cursor
}

func Interface2StringList(interfaceList interface{}) []string {
	list := interfaceList.([]interface{})
	stringList := make([]string, len(list))
	for i, v := range list {
		stringList[i] = v.(string)
	}
	return stringList

}

func validateDateTime(val interface{}, key string) (warns []string, errs []error) {
	v := val.(string)

	_, err := time.Parse("2006-01-02 15:04:05", v)
	if err != nil {
		errs = append(errs, fmt.Errorf("%q must be a valid RFC3339 datetime, got: %s", key, v))
	}
	return
}
