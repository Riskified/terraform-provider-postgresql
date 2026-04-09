package postgresql

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"

	// Use Postgres as SQL driver
	"github.com/lib/pq"
)

var allowedObjectTypes = []string{
	"system",
	"database",
	"function",
	"procedure",
	"routine",
	"schema",
	"sequence",
	"table",
	"type",
}

func resourcePostgreSQLGrant() *schema.Resource {
	return &schema.Resource{
		Create: PGResourceFunc(resourcePostgreSQLGrantCreate),
		// Since all of this resource's arguments force a recreation
		// there's no need for an Update function
		// Update:
		Read:   PGResourceFunc(resourcePostgreSQLGrantRead),
		Delete: PGResourceFunc(resourcePostgreSQLGrantDelete),

		Schema: map[string]*schema.Schema{
			"role": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The name of the role to grant privileges on",
			},
			"database": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The database to grant privileges on for this role",
			},
			"schema": {
				Type:        schema.TypeString,
				Optional:    true,
				ForceNew:    true,
				Description: "The database schema to grant privileges on for this role",
			},
			"object_type": {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validation.StringInSlice(allowedObjectTypes, false),
				Description:  "The PostgreSQL object type to grant the privileges on (one of: " + strings.Join(allowedObjectTypes, ", ") + ")",
			},
			"objects": {
				Type:        schema.TypeSet,
				Optional:    true,
				ForceNew:    true,
				Elem:        &schema.Schema{Type: schema.TypeString},
				Set:         schema.HashString,
				Description: "The specific objects to grant privileges on for this role (empty means all objects of the requested type)",
			},
			"privileges": {
				Type:        schema.TypeSet,
				Required:    true,
				ForceNew:    true,
				Elem:        &schema.Schema{Type: schema.TypeString},
				Set:         schema.HashString,
				Description: "The list of privileges to grant",
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

func resourcePostgreSQLGrantRead(db *DBConnection, d *schema.ResourceData) error {
	if err := validateFeatureSupport(db, d); err != nil {
		return fmt.Errorf("feature is not supported: %v", err)
	}

	exists, err := checkRoleDBSchemaExists(db, d)
	if err != nil {
		return err
	}
	if !exists {
		d.SetId("")
		return nil
	}
	d.SetId(generateGrantID(d))

	database := d.Get("database").(string)

	dbConn, err := connectToDatabase(db, database)
	if err != nil {
		return err
	}

	return readRolePrivileges(dbConn, d)
}

func resourcePostgreSQLGrantCreate(db *DBConnection, d *schema.ResourceData) error {
	// Validate parameters first (DB-agnostic), so validation errors are reported
	// regardless of DB type/version.
	objectType := d.Get("object_type").(string)
	if d.Get("schema").(string) == "" && objectType != "database" {
		return fmt.Errorf("parameter 'schema' is mandatory for postgresql_grant resource")
	}
	if d.Get("objects").(*schema.Set).Len() > 0 && (objectType == "database" || objectType == "schema") {
		return fmt.Errorf("cannot specify `objects` when `object_type` is `database` or `schema`")
	}
	if err := validatePrivileges(d); err != nil {
		return err
	}

	if err := validateFeatureSupport(db, d); err != nil {
		return fmt.Errorf("feature is not supported: %v", err)
	}

	database := d.Get("database").(string)

	dbConn, err := connectToDatabase(db, database)
	if err != nil {
		return err
	}

	// Revoke all privileges before granting
	if err := revokeRolePrivilegesWithDB(dbConn, d); err != nil {
		return err
	}
	if err := grantRolePrivilegesWithDB(dbConn, d); err != nil {
		return err
	}

	d.SetId(generateGrantID(d))

	return readRolePrivileges(dbConn, d)
}

func resourcePostgreSQLGrantDelete(db *DBConnection, d *schema.ResourceData) error {
	if err := validateFeatureSupport(db, d); err != nil {
		return fmt.Errorf("feature is not supported: %v", err)
	}

	database := d.Get("database").(string)

	dbConn, err := connectToDatabase(db, database)
	if err != nil {
		return err
	}

	if err := revokeRolePrivilegesWithDB(dbConn, d); err != nil {
		return err
	}
	return nil
}

func readSystemRolePriviges(db QueryAble, role string) error {
	var query string
	var privileges pq.ByteaArray
	query = fmt.Sprintf(`with a as (show system grants for %s) select array_agg(privilege_type) from a`, role)
	if err := db.QueryRow(query).Scan(&privileges); err != nil {
		return fmt.Errorf("could not read system privileges: %w", err)
	}
	return nil
}

func readDatabaseRolePriviges(db QueryAble, d *schema.ResourceData, role string) error {
	dbName := d.Get("database").(string)
	var privileges pq.ByteaArray
	query := fmt.Sprintf(`with a as (show grants on database %s for %s) select array_agg(privilege_type) from a where grantee=%s`, pq.QuoteIdentifier(dbName), pq.QuoteIdentifier(role), pq.QuoteLiteral(role))
	if err := db.QueryRow(query).Scan(&privileges); err != nil {
		return fmt.Errorf("could not read privileges for database %s: %w", dbName, err)
	}

	d.Set("privileges", pgArrayToSet(privileges))
	return nil
}

func readSchemaRolePriviges(db QueryAble, d *schema.ResourceData, role string) error {
	schemaName := d.Get("schema").(string)
	var privileges pq.ByteaArray
	query := fmt.Sprintf(`with a as ( show grants on schema %s for %s) select array_agg(privilege_type) from a where grantee=%s;`, pq.QuoteIdentifier(schemaName), pq.QuoteIdentifier(role), pq.QuoteLiteral(role))
	if err := db.QueryRow(query).Scan(&privileges); err != nil {
		return fmt.Errorf("could not read privileges for schema %s: %w", schemaName, err)
	}

	d.Set("privileges", pgArrayToSet(privileges))
	return nil
}

func readRolePrivileges(db QueryAble, d *schema.ResourceData) error {
	role := d.Get("role").(string)
	objectType := d.Get("object_type").(string)
	objects := d.Get("objects").(*schema.Set)

	var query string
	var rows *sql.Rows
	var err error

	switch objectType {
	case "system":
		return readSystemRolePriviges(db, role)
	case "database":
		return readDatabaseRolePriviges(db, d, role)

	case "schema":
		return readSchemaRolePriviges(db, d, role)

	case "function", "procedure", "routine":
		// CockroachDB: pg_proc.proacl is always NULL; use information_schema instead
		query = fmt.Sprintf(
			`SELECT routine_name, array_agg(privilege_type)
FROM information_schema.role_routine_grants
WHERE routine_schema = %s
AND grantee = %s
GROUP BY routine_name`,
			pq.QuoteLiteral(d.Get("schema").(string)), pq.QuoteLiteral(role))
		rows, err = db.Query(query)

	default:
		query = fmt.Sprintf("with a as (show tables from %s) , b as (show grants on table * for %s) select a.table_name,  array_agg(privilege_type) from a inner join b on a.table_name=b.table_name and a.schema_name = b.schema_name  where a.type='%s'  and grantee= %s group by a.table_name;", pq.QuoteIdentifier(d.Get("schema").(string)), pq.QuoteIdentifier(role), objectType, pq.QuoteLiteral(role))
		rows, err = db.Query(query)
	}

	// This returns, for the specified role (rolname),
	// the list of all object of the specified type in the specified schema
	// with the list of the currently applied privileges (aggregation of privilege_type)
	//
	// Our goal is to check that every object has the same privileges as saved in the state.
	if err != nil {
		return err
	}

	for rows.Next() {
		var objName string
		var privileges pq.ByteaArray

		if err := rows.Scan(&objName, &privileges); err != nil {
			return err
		}

		if objects.Len() > 0 && !objects.Contains(objName) {
			continue
		}

		privilegesSet := pgArrayToSet(privileges)

		if !privilegesSet.Equal(d.Get("privileges").(*schema.Set)) {
			// If any object doesn't have the same privileges as saved in the state,
			// we return its privileges to force an update.
			log.Printf(
				"[DEBUG] %s %s has not the expected privileges %v for role %s",
				strings.ToTitle(objectType), objName, privileges, d.Get("role"),
			)
			d.Set("privileges", privilegesSet)
			break
		}
	}

	return nil
}

func createGrantQuery(d *schema.ResourceData, privileges []string) string {
	var query string

	switch strings.ToUpper(d.Get("object_type").(string)) {
	case "SYSTEM":
		query = fmt.Sprintf(
			"GRANT SYSTEM %s TO %s",
			strings.Join(privileges, ","),
			pq.QuoteIdentifier(d.Get("role").(string)),
		)
	case "DATABASE":
		query = fmt.Sprintf(
			"GRANT %s ON DATABASE %s TO %s",
			strings.Join(privileges, ","),
			pq.QuoteIdentifier(d.Get("database").(string)),
			pq.QuoteIdentifier(d.Get("role").(string)),
		)
	case "SCHEMA":
		query = fmt.Sprintf(
			"GRANT %s ON SCHEMA %s TO %s",
			strings.Join(privileges, ","),
			pq.QuoteIdentifier(d.Get("schema").(string)),
			pq.QuoteIdentifier(d.Get("role").(string)),
		)
	case "TABLE", "SEQUENCE", "FUNCTION", "PROCEDURE", "ROUTINE":
		objects := d.Get("objects").(*schema.Set)
		if objects.Len() > 0 {
			query = fmt.Sprintf(
				"GRANT %s ON %s %s TO %s",
				strings.Join(privileges, ","),
				strings.ToUpper(d.Get("object_type").(string)),
				setToPgIdentList(d.Get("schema").(string), objects),
				pq.QuoteIdentifier(d.Get("role").(string)),
			)
		} else {
			query = fmt.Sprintf(
				"GRANT %s ON ALL %sS IN SCHEMA %s TO %s",
				strings.Join(privileges, ","),
				strings.ToUpper(d.Get("object_type").(string)),
				pq.QuoteIdentifier(d.Get("schema").(string)),
				pq.QuoteIdentifier(d.Get("role").(string)),
			)
		}
	}

	if d.Get("with_grant_option").(bool) {
		query = query + " WITH GRANT OPTION"
	}

	return query
}

func createRevokeQuery(d *schema.ResourceData) string {
	var query string

	switch strings.ToUpper(d.Get("object_type").(string)) {
	case "SYSTEM":
		query = fmt.Sprintf(
			"REVOKE SYSTEM ALL FROM %s",
			pq.QuoteIdentifier(d.Get("role").(string)),
		)
	case "DATABASE":
		query = fmt.Sprintf(
			"REVOKE ALL PRIVILEGES ON DATABASE %s FROM %s",
			pq.QuoteIdentifier(d.Get("database").(string)),
			pq.QuoteIdentifier(d.Get("role").(string)),
		)
	case "SCHEMA":
		query = fmt.Sprintf(
			"REVOKE ALL PRIVILEGES ON SCHEMA %s FROM %s",
			pq.QuoteIdentifier(d.Get("schema").(string)),
			pq.QuoteIdentifier(d.Get("role").(string)),
		)
	case "TABLE", "SEQUENCE", "FUNCTION", "PROCEDURE", "ROUTINE":
		objects := d.Get("objects").(*schema.Set)
		privileges := d.Get("privileges").(*schema.Set)
		if objects.Len() > 0 {
			if privileges.Len() > 0 {
				// Revoking specific privileges instead of all privileges
				// to avoid messing with column level grants
				query = fmt.Sprintf(
					"REVOKE %s ON %s %s FROM %s",
					setToPgIdentSimpleList(privileges),
					strings.ToUpper(d.Get("object_type").(string)),
					setToPgIdentList(d.Get("schema").(string), objects),
					pq.QuoteIdentifier(d.Get("role").(string)),
				)
			} else {
				query = fmt.Sprintf(
					"REVOKE ALL PRIVILEGES ON %s %s FROM %s",
					strings.ToUpper(d.Get("object_type").(string)),
					setToPgIdentList(d.Get("schema").(string), objects),
					pq.QuoteIdentifier(d.Get("role").(string)),
				)
			}
		} else {
			query = fmt.Sprintf(
				"REVOKE ALL PRIVILEGES ON ALL %sS IN SCHEMA %s FROM %s",
				strings.ToUpper(d.Get("object_type").(string)),
				pq.QuoteIdentifier(d.Get("schema").(string)),
				pq.QuoteIdentifier(d.Get("role").(string)),
			)
		}
	}

	return query
}

// grantRolePrivilegesWithDB grants privileges using the DB connection directly
func grantRolePrivilegesWithDB(db *DBConnection, d *schema.ResourceData) error {
	privileges := []string{}
	for _, priv := range d.Get("privileges").(*schema.Set).List() {
		privileges = append(privileges, priv.(string))
	}

	if len(privileges) == 0 {
		log.Printf("[DEBUG] no privileges to grant for role %s in database: %s,", d.Get("role").(string), d.Get("database"))
		return nil
	}

	query := createGrantQuery(d, privileges)

	_, err := db.Exec(query)
	return err
}

// revokeRolePrivilegesWithDB revokes privileges using the DB connection directly
func revokeRolePrivilegesWithDB(db *DBConnection, d *schema.ResourceData) error {
	query := createRevokeQuery(d)
	if len(query) == 0 {
		// Query is empty, don't run anything
		return nil
	}
	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("could not execute revoke query: %w", err)
	}
	return nil
}

func checkRoleDBSchemaExists(db *DBConnection, d *schema.ResourceData) (bool, error) {
	// Check the role exists
	role := d.Get("role").(string)
	if role != publicRole {
		exists, err := roleExists(db, role)
		if err != nil {
			return false, err
		}
		if !exists {
			log.Printf("[DEBUG] role %s does not exists", role)
			return false, nil
		}
	}

	// Check the database exists
	database := d.Get("database").(string)
	exists, err := dbExists(db, database)
	if err != nil {
		return false, err
	}
	if !exists {
		log.Printf("[DEBUG] database %s does not exists", database)
		return false, nil
	}

	pgSchema := d.Get("schema").(string)

	if d.Get("object_type").(string) != "database" && pgSchema != "" {
		// Connect on this database to check if schema exists
		dbConn, err := connectToDatabase(db, database)
		if err != nil {
			return false, err
		}

		// Check the schema exists (the SQL connection needs to be on the right database)
		exists, err = schemaExists(dbConn, pgSchema)
		if err != nil {
			return false, err
		}
		if !exists {
			log.Printf("[DEBUG] schema %s does not exists", pgSchema)
			return false, nil
		}
	}

	return true, nil
}

func generateGrantID(d *schema.ResourceData) string {
	parts := []string{d.Get("role").(string), d.Get("database").(string)}

	objectType := d.Get("object_type").(string)
	if objectType != "database" {
		parts = append(parts, d.Get("schema").(string))
	}
	parts = append(parts, objectType)

	for _, object := range d.Get("objects").(*schema.Set).List() {
		parts = append(parts, object.(string))
	}

	return strings.Join(parts, "_")
}

func validateFeatureSupport(db *DBConnection, d *schema.ResourceData) error {
	if !db.featureSupported(featurePrivileges) {
		return fmt.Errorf(
			"postgresql_grant resource is not supported for this version (%s)",
			db.version,
		)
	}
	if d.Get("object_type") == "procedure" && !db.featureSupported(featureProcedure) {
		return fmt.Errorf(
			"object type PROCEDURE is not supported for this version (%s)",
			db.version,
		)
	}
	if d.Get("object_type") == "routine" && !db.featureSupported(featureRoutine) {
		return fmt.Errorf(
			"object type ROUTINE is not supported for this version (%s)",
			db.version,
		)
	}
	if d.Get("object_type") == "system" && !db.featureSupported(featureSysPrivileges) {
		return fmt.Errorf(
			"privelege type System is not supported for this version (%s)",
			db.version,
		)
	}
	return nil
}
