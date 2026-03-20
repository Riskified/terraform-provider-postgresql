package postgresql

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"github.com/lib/pq"
)

const (
	dbCTypeAttr              = "lc_ctype"
	dbCollationAttr          = "lc_collate"
	dbConnLimitAttr          = "connection_limit"
	dbEncodingAttr           = "encoding"
	dbNameAttr               = "name"
	dbOwnerAttr              = "owner"
	dbDeletionProtectionAttr = "deletion_protection"
)

func resourcePostgreSQLDatabase() *schema.Resource {
	return &schema.Resource{
		Create: PGResourceFunc(resourcePostgreSQLDatabaseCreate),
		Read:   PGResourceFunc(resourcePostgreSQLDatabaseRead),
		Update: PGResourceFunc(resourcePostgreSQLDatabaseUpdate),
		Delete: PGResourceFunc(resourcePostgreSQLDatabaseDelete),
		Exists: PGResourceExistsFunc(resourcePostgreSQLDatabaseExists),
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},

		Schema: map[string]*schema.Schema{
			dbNameAttr: {
				Type:        schema.TypeString,
				Required:    true,
				Description: "The database name to connect to",
			},
			dbOwnerAttr: {
				Type:        schema.TypeString,
				Optional:    true,
				Computed:    true,
				Description: "The ROLE which owns the database",
			},
			dbEncodingAttr: {
				Type:        schema.TypeString,
				Optional:    true,
				Computed:    true,
				ForceNew:    true,
				Description: "Character set encoding to use in the new database",
			},
			dbCollationAttr: {
				Type:        schema.TypeString,
				Optional:    true,
				Computed:    true,
				ForceNew:    true,
				Description: "Collation order (LC_COLLATE) to use in the new database",
			},
			dbCTypeAttr: {
				Type:        schema.TypeString,
				Optional:    true,
				Computed:    true,
				ForceNew:    true,
				Description: "Character classification (LC_CTYPE) to use in the new database",
			},
			dbConnLimitAttr: {
				Type:         schema.TypeInt,
				Optional:     true,
				Default:      -1,
				Description:  "How many concurrent connections can be made to this database",
				ValidateFunc: validation.IntAtLeast(-1),
			},
			dbDeletionProtectionAttr: {
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     true,
				Description: "If true, Terraform will refuse to destroy this database. Set to false to allow deletion.",
			},
		},
	}
}

func resourcePostgreSQLDatabaseCreate(db *DBConnection, d *schema.ResourceData) error {
	if err := createDatabase(db, d); err != nil {
		return err
	}

	d.SetId(d.Get(dbNameAttr).(string))

	return resourcePostgreSQLDatabaseReadImpl(db, d)
}

func createDatabase(db *DBConnection, d *schema.ResourceData) error {
	currentUser := db.client.config.getDatabaseUsername()
	owner := d.Get(dbOwnerAttr).(string)

	var err error
	if owner != "" {
		// Needed in order to set the owner of the db if the connection user is not a
		// superuser
		ownerGranted, err := grantRoleMembership(db, owner, currentUser)
		if err != nil {
			return err
		}
		if ownerGranted {
			defer func() {
				_, err = revokeRoleMembership(db, owner, currentUser)
			}()
		}
	}

	dbName := d.Get(dbNameAttr).(string)
	b := bytes.NewBufferString("CREATE DATABASE ")
	fmt.Fprint(b, pq.QuoteIdentifier(dbName))

	// Handle each option individually and stream results into the query
	// buffer.

	switch v, ok := d.GetOk(dbEncodingAttr); {
	case ok:
		fmt.Fprintf(b, " ENCODING = '%s' ", pqQuoteLiteral(v.(string)))
	case v.(string) == "":
		fmt.Fprint(b, ` ENCODING = 'UTF-8'`)
	}

	// Don't specify LC_COLLATE if user didn't specify it
	// This will use the default one (usually the one defined in the template database)
	switch v, ok := d.GetOk(dbCollationAttr); {
	case ok && strings.ToUpper(v.(string)) == "DEFAULT":
		fmt.Fprintf(b, " LC_COLLATE DEFAULT")
	case ok:
		fmt.Fprintf(b, " LC_COLLATE '%s' ", pqQuoteLiteral(v.(string)))
	}

	// Don't specify LC_CTYPE if user didn't specify it
	// This will use the default one (usually the one defined in the template database)
	switch v, ok := d.GetOk(dbCTypeAttr); {
	case ok && strings.ToUpper(v.(string)) == "DEFAULT":
		fmt.Fprintf(b, " LC_CTYPE DEFAULT")
	case ok:
		fmt.Fprintf(b, " LC_CTYPE '%s' ", pqQuoteLiteral(v.(string)))
	}

	// OWNER needs to be at the end of the command
	switch v, ok := d.GetOk(dbOwnerAttr); {
	case ok:
		fmt.Fprint(b, " OWNER ", pq.QuoteIdentifier(v.(string)))
	default:
		// No owner specified in the config, default to using
		// the connecting username.
		fmt.Fprint(b, " OWNER ", pq.QuoteIdentifier(currentUser))
	}

	sql := b.String()
	if _, err := db.Exec(sql); err != nil {
		return fmt.Errorf("Error creating database %q: %w", dbName, err)
	}

	// Set err outside of the return so that the deferred revoke can override err
	// if necessary.
	return err
}

func resourcePostgreSQLDatabaseDelete(db *DBConnection, d *schema.ResourceData) error {
	if d.Get(dbDeletionProtectionAttr).(bool) {
		return fmt.Errorf(
			"cannot destroy database %q: deletion_protection is set to true. "+
				"Set deletion_protection = false in your configuration before destroying this resource.",
			d.Get(dbNameAttr).(string),
		)
	}

	currentUser := db.client.config.getDatabaseUsername()
	owner := d.Get(dbOwnerAttr).(string)

	var err error
	if owner != "" {
		// Needed in order to set the owner of the db if the connection user is not a
		// superuser
		ownerGranted, err := grantRoleMembership(db, owner, currentUser)
		if err != nil {
			return err
		}
		if ownerGranted {
			defer func() {
				_, err = revokeRoleMembership(db, owner, currentUser)
			}()
		}
	}

	dbName := d.Get(dbNameAttr).(string)

	sql := fmt.Sprintf("DROP DATABASE %s", pq.QuoteIdentifier(dbName))
	if _, err := db.Exec(sql); err != nil {
		return fmt.Errorf("Error dropping database: %w", err)
	}

	d.SetId("")

	// Returning err even if it's nil so defer func can modify it.
	return err
}

func resourcePostgreSQLDatabaseExists(db *DBConnection, d *schema.ResourceData) (bool, error) {
	txn, err := startTransaction(db.client, "")
	if err != nil {
		return false, err
	}
	defer deferredRollback(txn)

	return dbExists(txn, d.Id())
}

func resourcePostgreSQLDatabaseRead(db *DBConnection, d *schema.ResourceData) error {
	return resourcePostgreSQLDatabaseReadImpl(db, d)
}

func resourcePostgreSQLDatabaseReadImpl(db *DBConnection, d *schema.ResourceData) error {
	dbId := d.Id()
	var dbName, ownerName string
	err := db.QueryRow("SELECT d.datname, pg_catalog.pg_get_userbyid(d.datdba) from pg_database d WHERE datname=$1", dbId).Scan(&dbName, &ownerName)
	switch {
	case err == sql.ErrNoRows:
		log.Printf("[WARN] PostgreSQL database (%q) not found", dbId)
		d.SetId("")
		return nil
	case err != nil:
		return fmt.Errorf("Error reading database: %w", err)
	}

	var dbEncoding, dbCollation, dbCType string
	var dbConnLimit int

	columns := []string{
		"pg_catalog.pg_encoding_to_char(d.encoding)",
		"d.datcollate",
		"d.datctype",
		"d.datconnlimit",
	}

	dbSQL := fmt.Sprintf(`SELECT %s FROM pg_catalog.pg_database AS d WHERE d.datname = $1`,
		strings.Join(columns, ", "))
	err = db.QueryRow(dbSQL, dbId).
		Scan(
			&dbEncoding,
			&dbCollation,
			&dbCType,
			&dbConnLimit,
		)
	switch {
	case err == sql.ErrNoRows:
		log.Printf("[WARN] PostgreSQL database (%q) not found", dbId)
		d.SetId("")
		return nil
	case err != nil:
		return fmt.Errorf("Error reading database: %w", err)
	}

	d.Set(dbNameAttr, dbName)
	d.Set(dbOwnerAttr, ownerName)
	d.Set(dbEncodingAttr, dbEncoding)
	d.Set(dbCollationAttr, dbCollation)
	d.Set(dbCTypeAttr, dbCType)
	d.Set(dbConnLimitAttr, dbConnLimit)

	return nil
}

func resourcePostgreSQLDatabaseUpdate(db *DBConnection, d *schema.ResourceData) error {
	if err := setDBName(db, d); err != nil {
		return err
	}

	if err := setDBOwner(db, d); err != nil {
		return err
	}

	if err := setDBConnLimit(db, d); err != nil {
		return err
	}

	// Empty values: ALTER DATABASE name RESET configuration_parameter;

	return resourcePostgreSQLDatabaseReadImpl(db, d)
}

func setDBName(db QueryAble, d *schema.ResourceData) error {
	if !d.HasChange(dbNameAttr) {
		return nil
	}

	oraw, nraw := d.GetChange(dbNameAttr)
	o := oraw.(string)
	n := nraw.(string)
	if n == "" {
		return errors.New("Error setting database name to an empty string")
	}

	sql := fmt.Sprintf("ALTER DATABASE %s RENAME TO %s", pq.QuoteIdentifier(o), pq.QuoteIdentifier(n))
	if _, err := db.Exec(sql); err != nil {
		return fmt.Errorf("Error updating database name: %w", err)
	}
	d.SetId(n)

	return nil
}

func setDBOwner(db *DBConnection, d *schema.ResourceData) error {
	if !d.HasChange(dbOwnerAttr) {
		return nil
	}

	owner := d.Get(dbOwnerAttr).(string)
	if owner == "" {
		return nil
	}
	currentUser := db.client.config.getDatabaseUsername()

	//needed in order to set the owner of the db if the connection user is not a superuser
	ownerGranted, err := grantRoleMembership(db, owner, currentUser)
	if err != nil {
		return err
	}
	if ownerGranted {
		defer func() {
			_, err = revokeRoleMembership(db, owner, currentUser)
		}()
	}

	dbName := d.Get(dbNameAttr).(string)
	sql := fmt.Sprintf("ALTER DATABASE %s OWNER TO %s", pq.QuoteIdentifier(dbName), pq.QuoteIdentifier(owner))
	if _, err := db.Exec(sql); err != nil {
		return fmt.Errorf("Error updating database OWNER: %w", err)
	}

	return err
}

func setDBConnLimit(db QueryAble, d *schema.ResourceData) error {
	if !d.HasChange(dbConnLimitAttr) {
		return nil
	}

	connLimit := d.Get(dbConnLimitAttr).(int)
	dbName := d.Get(dbNameAttr).(string)
	sql := fmt.Sprintf("ALTER DATABASE %s CONNECTION LIMIT = %d", pq.QuoteIdentifier(dbName), connLimit)
	if _, err := db.Exec(sql); err != nil {
		return fmt.Errorf("Error updating database CONNECTION LIMIT: %w", err)
	}

	return nil
}
