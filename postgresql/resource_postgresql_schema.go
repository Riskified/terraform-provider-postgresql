package postgresql

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/lib/pq"
)

const (
	schemaNameAttr     = "name"
	schemaDatabaseAttr = "database"
	schemaOwnerAttr    = "owner"
	schemaIfNotExists  = "if_not_exists"
	schemaDropCascade  = "drop_cascade"
)

func resourcePostgreSQLSchema() *schema.Resource {
	return &schema.Resource{
		Create: PGResourceFunc(resourcePostgreSQLSchemaCreate),
		Read:   PGResourceFunc(resourcePostgreSQLSchemaRead),
		Update: PGResourceFunc(resourcePostgreSQLSchemaUpdate),
		Delete: PGResourceFunc(resourcePostgreSQLSchemaDelete),
		Exists: PGResourceExistsFunc(resourcePostgreSQLSchemaExists),
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},

		Schema: map[string]*schema.Schema{
			schemaNameAttr: {
				Type:        schema.TypeString,
				Required:    true,
				Description: "The name of the schema",
			},
			schemaDatabaseAttr: {
				Type:        schema.TypeString,
				Optional:    true,
				Computed:    true,
				ForceNew:    true,
				Description: "The database name to alter schema",
			},
			schemaOwnerAttr: {
				Type:        schema.TypeString,
				Optional:    true,
				Computed:    true,
				Description: "The ROLE name who owns the schema",
			},
			schemaIfNotExists: {
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     true,
				Description: "When true, use the existing schema if it exists",
			},
			schemaDropCascade: {
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     false,
				Description: "When true, will also drop all the objects that are contained in the schema",
			},
		},
	}
}

func resourcePostgreSQLSchemaCreate(db *DBConnection, d *schema.ResourceData) error {
	database := getDatabase(d, db.client.databaseName)

	dbConn, err := connectToDatabase(db, database)
	if err != nil {
		return err
	}
	if err := createSchemaWithDB(dbConn, d); err != nil {
		return err
	}
	d.SetId(generateSchemaID(d, database))
	return resourcePostgreSQLSchemaReadImpl(dbConn, d)
}

// createSchemaWithDB creates a schema without using transactions
func createSchemaWithDB(db *DBConnection, d *schema.ResourceData) error {
	schemaName := d.Get(schemaNameAttr).(string)

	// Check if previous tasks haven't already created schema
	var foundSchema bool
	err := db.QueryRow(`SELECT TRUE FROM pg_catalog.pg_namespace WHERE nspname = $1`, schemaName).Scan(&foundSchema)

	queries := []string{}
	switch {
	case err == sql.ErrNoRows:
		b := bytes.NewBufferString("CREATE SCHEMA ")
		if db.featureSupported(featureSchemaCreateIfNotExist) {
			if v := d.Get(schemaIfNotExists); v.(bool) {
				fmt.Fprint(b, "IF NOT EXISTS ")
			}
		}
		fmt.Fprint(b, pq.QuoteIdentifier(schemaName))

		switch v, ok := d.GetOk(schemaOwnerAttr); {
		case ok:
			fmt.Fprint(b, " AUTHORIZATION ", pq.QuoteIdentifier(v.(string)))
		}
		queries = append(queries, b.String())

	case err != nil:
		return fmt.Errorf("Error looking for schema: %w", err)

	default:
		// The schema already exists, we just set the owner.
		if err := setSchemaOwnerWithDB(db, d); err != nil {
			return err
		}
	}

	for _, query := range queries {
		if _, err = db.Exec(query); err != nil {
			return fmt.Errorf("Error creating schema %s: %w", schemaName, err)
		}
	}

	return nil
}

// setSchemaOwnerWithDB sets the schema owner outside of a transaction for CockroachDB
func setSchemaOwnerWithDB(db *DBConnection, d *schema.ResourceData) error {
	schemaName := d.Get(schemaNameAttr).(string)
	owner := d.Get(schemaOwnerAttr).(string)
	if owner == "" {
		return nil
	}
	sql := fmt.Sprintf("ALTER SCHEMA %s OWNER TO %s", pq.QuoteIdentifier(schemaName), pq.QuoteIdentifier(owner))
	if _, err := db.Exec(sql); err != nil {
		return fmt.Errorf("Error setting schema owner: %w", err)
	}
	return nil
}

func resourcePostgreSQLSchemaDelete(db *DBConnection, d *schema.ResourceData) error {
	database := getDatabase(d, db.client.databaseName)
	schemaName := d.Get(schemaNameAttr).(string)

	if schemaName != "public" {
		dbConn, err := connectToDatabase(db, database)
		if err != nil {
			return err
		}
		exists, err := schemaExistsWithDB(dbConn, schemaName)
		if err != nil {
			return err
		}
		if !exists {
			d.SetId("")
			return nil
		}

		dropMode := "RESTRICT"
		if d.Get(schemaDropCascade).(bool) {
			dropMode = "CASCADE"
		}

		sql := fmt.Sprintf("DROP SCHEMA %s %s", pq.QuoteIdentifier(schemaName), dropMode)
		if _, err = dbConn.Exec(sql); err != nil {
			return fmt.Errorf("Error deleting schema: %w", err)
		}
		d.SetId("")
	} else {
		log.Printf("cannot delete schema %s", schemaName)
	}

	return nil
}

func resourcePostgreSQLSchemaExists(db *DBConnection, d *schema.ResourceData) (bool, error) {
	database, schemaName, err := getDBSchemaName(d, db.client.databaseName)
	if err != nil {
		return false, err
	}

	// Check if the database exists
	exists, err := dbExists(db, database)
	if err != nil || !exists {
		return false, err
	}

	dbConn, err := connectToDatabase(db, database)
	if err != nil {
		return false, err
	}

	err = dbConn.QueryRow("SELECT n.nspname FROM pg_catalog.pg_namespace n WHERE n.nspname=$1", schemaName).Scan(&schemaName)
	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, fmt.Errorf("Error reading schema: %w", err)
	}

	return true, nil
}

func resourcePostgreSQLSchemaRead(db *DBConnection, d *schema.ResourceData) error {
	return resourcePostgreSQLSchemaReadImpl(db, d)
}

func resourcePostgreSQLSchemaReadImpl(db *DBConnection, d *schema.ResourceData) error {
	database, schemaName, err := getDBSchemaName(d, db.client.databaseName)
	if err != nil {
		return err
	}

	dbConn, err := connectToDatabase(db, database)
	if err != nil {
		return err
	}

	var schemaOwner string
	err = dbConn.QueryRow("SELECT pg_catalog.pg_get_userbyid(n.nspowner) FROM pg_catalog.pg_namespace n WHERE n.nspname=$1", schemaName).Scan(&schemaOwner)
	switch {
	case err == sql.ErrNoRows:
		log.Printf("[WARN] PostgreSQL schema (%s) not found in database %s", schemaName, database)
		d.SetId("")
		return nil
	case err != nil:
		return fmt.Errorf("Error reading schema: %w", err)
	default:
		d.Set(schemaNameAttr, schemaName)
		d.Set(schemaOwnerAttr, schemaOwner)
		d.Set(schemaDatabaseAttr, database)
		d.SetId(generateSchemaID(d, database))

		return nil
	}
}

func resourcePostgreSQLSchemaUpdate(db *DBConnection, d *schema.ResourceData) error {
	databaseName := getDatabase(d, db.client.databaseName)

	dbConn, err := connectToDatabase(db, databaseName)
	if err != nil {
		return err
	}

	if err := setSchemaName(dbConn, d, databaseName); err != nil {
		return err
	}

	if err := setSchemaOwner(dbConn, d); err != nil {
		return err
	}

	return resourcePostgreSQLSchemaReadImpl(db, d)
}

func setSchemaName(db QueryAble, d *schema.ResourceData, databaseName string) error {
	if !d.HasChange(schemaNameAttr) {
		return nil
	}

	oraw, nraw := d.GetChange(schemaNameAttr)
	o := oraw.(string)
	n := nraw.(string)
	if n == "" {
		return errors.New("Error setting schema name to an empty string")
	}

	sql := fmt.Sprintf("ALTER SCHEMA %s RENAME TO %s", pq.QuoteIdentifier(o), pq.QuoteIdentifier(n))
	if _, err := db.Exec(sql); err != nil {
		return fmt.Errorf("Error updating schema NAME: %w", err)
	}
	d.SetId(generateSchemaID(d, databaseName))

	return nil
}

func setSchemaOwner(db QueryAble, d *schema.ResourceData) error {
	if !d.HasChange(schemaOwnerAttr) {
		return nil
	}

	schemaName := d.Get(schemaNameAttr).(string)
	schemaOwner := d.Get(schemaOwnerAttr).(string)

	if schemaOwner == "" {
		return errors.New("Error setting schema owner to an empty string")
	}

	sql := fmt.Sprintf("ALTER SCHEMA %s OWNER TO %s", pq.QuoteIdentifier(schemaName), pq.QuoteIdentifier(schemaOwner))
	if _, err := db.Exec(sql); err != nil {
		return fmt.Errorf("Error updating schema OWNER: %w", err)
	}

	return nil
}

func generateSchemaID(d *schema.ResourceData, databaseName string) string {
	SchemaID := strings.Join([]string{
		getDatabase(d, databaseName),
		d.Get(schemaNameAttr).(string),
	}, ".")

	return SchemaID
}

func getDBSchemaName(d *schema.ResourceData, databaseName string) (string, string, error) {
	database := getDatabase(d, databaseName)
	schemaName := d.Get(schemaNameAttr).(string)

	// When importing, we have to parse the ID to find schema and database names.
	if schemaName == "" {
		parsed := strings.Split(d.Id(), ".")
		if len(parsed) != 2 {
			return "", "", fmt.Errorf("schema ID %s has not the expected format 'database.schema': %v", d.Id(), parsed)
		}
		database = parsed[0]
		schemaName = parsed[1]
	}
	return database, schemaName, nil
}
