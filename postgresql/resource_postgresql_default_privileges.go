package postgresql

import (
	"fmt"
	"log"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"

	// Use Postgres as SQL driver
	"github.com/lib/pq"
)

func resourcePostgreSQLDefaultPrivileges() *schema.Resource {
	return &schema.Resource{
		Create: PGResourceFunc(resourcePostgreSQLDefaultPrivilegesCreate),
		Update: PGResourceFunc(resourcePostgreSQLDefaultPrivilegesCreate),
		Read:   PGResourceFunc(resourcePostgreSQLDefaultPrivilegesRead),
		Delete: PGResourceFunc(resourcePostgreSQLDefaultPrivilegesDelete),

		Schema: map[string]*schema.Schema{
			"role": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The name of the role to which grant default privileges on",
			},
			"database": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The database to grant default privileges for this role",
			},
			"owner": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "Target role for which to alter default privileges.",
			},
			"schema": {
				Type:        schema.TypeString,
				Optional:    true,
				ForceNew:    true,
				Description: "The database schema to set default privileges for this role",
			},
			"object_type": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
				ValidateFunc: validation.StringInSlice([]string{
					"table",
					"sequence",
					"function",
					"type",
					"schema",
				}, false),
				Description: "The PostgreSQL object type to set the default privileges on (one of: table, sequence, function, type, schema)",
			},
			"privileges": {
				Type:        schema.TypeSet,
				Required:    true,
				Elem:        &schema.Schema{Type: schema.TypeString},
				Set:         schema.HashString,
				Description: "The list of privileges to apply as default privileges",
			},
			"with_grant_option": {
				Type:        schema.TypeBool,
				Optional:    true,
				ForceNew:    true,
				Default:     false,
				Description: "Permit the grant recipient to grant it to others",
			},
		},
	}
}

func resourcePostgreSQLDefaultPrivilegesRead(db *DBConnection, d *schema.ResourceData) error {
	pgSchema := d.Get("schema").(string)
	objectType := d.Get("object_type").(string)

	if pgSchema != "" && objectType == "schema" && !db.featureSupported(featurePrivilegesOnSchemas) {
		return fmt.Errorf(
			"changing default privileges for schemas is not supported for this Postgres version (%s)",
			db.version,
		)
	}

	exists, err := checkRoleDBSchemaExists(db.client, d)
	if err != nil {
		return err
	}
	if !exists {
		d.SetId("")
		return nil
	}

	return readRoleDefaultPrivilegesWithDB(db, d)
}

func resourcePostgreSQLDefaultPrivilegesCreate(db *DBConnection, d *schema.ResourceData) error {
	pgSchema := d.Get("schema").(string)
	objectType := d.Get("object_type").(string)

	if pgSchema != "" && objectType == "schema" {
		if !db.featureSupported(featurePrivilegesOnSchemas) {
			return fmt.Errorf(
				"changing default privileges for schemas is not supported for this Postgres version (%s)",
				db.version,
			)
		}
		return fmt.Errorf("cannot specify `schema` when `object_type` is `schema`")
	}

	if d.Get("with_grant_option").(bool) && strings.ToLower(d.Get("role").(string)) == "public" {
		return fmt.Errorf("with_grant_option cannot be true for role 'public'")
	}

	if err := validatePrivileges(d); err != nil {
		return err
	}

	if err := revokeRoleDefaultPrivilegesWithDB(db, d); err != nil {
		return err
	}
	if err := grantRoleDefaultPrivilegesWithDB(db, d); err != nil {
		return err
	}
	d.SetId(generateDefaultPrivilegesID(d))
	return readRoleDefaultPrivilegesWithDB(db, d)
}

func resourcePostgreSQLDefaultPrivilegesDelete(db *DBConnection, d *schema.ResourceData) error {
	pgSchema := d.Get("schema").(string)
	objectType := d.Get("object_type").(string)

	if pgSchema != "" && objectType == "schema" && !db.featureSupported(featurePrivilegesOnSchemas) {
		return fmt.Errorf(
			"changing default privileges for schemas is not supported for this Postgres version (%s)",
			db.version,
		)
	}

	return revokeRoleDefaultPrivilegesWithDB(db, d)
}

func generateDefaultPrivilegesID(d *schema.ResourceData) string {
	pgSchema := d.Get("schema").(string)
	if pgSchema == "" {
		pgSchema = "noschema"
	}

	return strings.Join([]string{
		d.Get("role").(string), d.Get("database").(string), pgSchema,
		d.Get("owner").(string), d.Get("object_type").(string),
	}, "_")

}

// grantRoleDefaultPrivilegesWithDB grants default privileges outside of a transaction for CockroachDB
func grantRoleDefaultPrivilegesWithDB(db *DBConnection, d *schema.ResourceData) error {
	database := d.Get("database").(string)
	db, err := connectToDatabase(db, database)
	if err != nil {
		return fmt.Errorf("could not connect to database %s: %w", database, err)
	}

	role := d.Get("role").(string)
	pgSchema := d.Get("schema").(string)

	privileges := []string{}
	for _, priv := range d.Get("privileges").(*schema.Set).List() {
		privileges = append(privileges, priv.(string))
	}

	if len(privileges) == 0 {
		log.Printf("[DEBUG] no default privileges to grant for role %s, owner %s in database: %s,", d.Get("role").(string), d.Get("owner").(string), d.Get("database").(string))
		return nil
	}

	var inSchema string

	// If a schema is specified we need to build the part of the query string to action this
	if pgSchema != "" {
		inSchema = fmt.Sprintf("IN SCHEMA %s", pq.QuoteIdentifier(pgSchema))
	}

	query := fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR ROLE %s %s GRANT %s ON %sS TO %s",
		pq.QuoteIdentifier(d.Get("owner").(string)),
		inSchema,
		strings.Join(privileges, ","),
		strings.ToUpper(d.Get("object_type").(string)),
		pq.QuoteIdentifier(role),
	)

	if d.Get("with_grant_option").(bool) {
		query = query + " WITH GRANT OPTION"
	}
	if _, err = db.Exec(query); err != nil {
		return fmt.Errorf("could not alter default privileges: %w", err)
	}

	return nil
}

// revokeRoleDefaultPrivilegesWithDB revokes default privileges outside of a transaction for CockroachDB
func revokeRoleDefaultPrivilegesWithDB(db *DBConnection, d *schema.ResourceData) error {
	database := d.Get("database").(string)
	db, err := connectToDatabase(db, database)
	if err != nil {
		return fmt.Errorf("could not connect to database %s: %w", database, err)
	}

	pgSchema := d.Get("schema").(string)

	var inSchema string

	// If a schema is specified we need to build the part of the query string to action this
	if pgSchema != "" {
		inSchema = fmt.Sprintf("IN SCHEMA %s", pq.QuoteIdentifier(pgSchema))
	}
	query := fmt.Sprintf(
		"ALTER DEFAULT PRIVILEGES FOR ROLE %s %s REVOKE ALL ON %sS FROM %s",
		pq.QuoteIdentifier(d.Get("owner").(string)),
		inSchema,
		strings.ToUpper(d.Get("object_type").(string)),
		pq.QuoteIdentifier(d.Get("role").(string)),
	)
	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("could not revoke default privileges: %w", err)
	}
	return nil
}

// readRoleDefaultPrivilegesWithDB reads default privileges outside of a transaction for CockroachDB
func readRoleDefaultPrivilegesWithDB(db *DBConnection, d *schema.ResourceData) error {
	database := d.Get("database").(string)
	db, err := connectToDatabase(db, database)
	if err != nil {
		return fmt.Errorf("could not connect to database %s: %w", database, err)
	}

	role := d.Get("role").(string)
	owner := d.Get("owner").(string)
	pgSchema := d.Get("schema").(string)
	objectType := d.Get("object_type").(string)
	privilegesInput := d.Get("privileges").(*schema.Set).List()

	var query string
	var inSchema string
	// If a schema is specified we need to build the part of the query string to action this
	if pgSchema != "" {
		inSchema = fmt.Sprintf("IN SCHEMA %s", pq.QuoteIdentifier(pgSchema))
	}
	// CockroachDB uses SHOW DEFAULT PRIVILEGES instead of aclexplode
	query = fmt.Sprintf("with a as (show DEFAULT PRIVILEGES for role %s %s) select array_agg(privilege_type) from a where grantee = '%s' and object_type = '%ss';", owner, inSchema, role, objectType)

	var privileges pq.ByteaArray
	if err := db.QueryRow(query).Scan(&privileges); err != nil {
		return fmt.Errorf("could not read default privileges: %w", err)
	}

	// We consider no privileges as "not exists" unless no privileges were provided as input
	if len(privileges) == 0 {
		log.Printf("[DEBUG] no default privileges for role %s in schema %s", role, pgSchema)
		if len(privilegesInput) != 0 {
			d.SetId("")
			return nil
		}
	}

	privilegesSet := pgArrayToSet(privileges)
	d.Set("privileges", privilegesSet)
	d.SetId(generateDefaultPrivilegesID(d))

	return nil
}
