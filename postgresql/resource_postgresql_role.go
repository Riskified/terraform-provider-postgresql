package postgresql

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"github.com/lib/pq"
)

const (
	roleBypassRLSAttr                       = "bypass_row_level_security"
	roleCreateDBAttr                        = "create_database"
	roleCreateRoleAttr                      = "create_role"
	roleIdleInTransactionSessionTimeoutAttr = "idle_in_transaction_session_timeout"
	roleLoginAttr                           = "login"
	roleNameAttr                            = "name"
	rolePasswordAttr                        = "password"
	roleSkipDropRoleAttr                    = "skip_drop_role"
	roleSkipReassignOwnedAttr               = "skip_reassign_owned"
	roleValidUntilAttr                      = "valid_until"
	roleRolesAttr                           = "roles"
	roleSearchPathAttr                      = "search_path"
	roleStatementTimeoutAttr                = "statement_timeout"
	defaultTransactionIsolationAttr         = "default_transaction_isolation"
	defaultTransactionFollowerReadsAttr     = "default_transaction_use_follower_reads"
)

func resourcePostgreSQLRole() *schema.Resource {
	return &schema.Resource{
		Create: PGResourceFunc(resourcePostgreSQLRoleCreate),
		Read:   PGResourceFunc(resourcePostgreSQLRoleRead),
		Update: PGResourceFunc(resourcePostgreSQLRoleUpdate),
		Delete: PGResourceFunc(resourcePostgreSQLRoleDelete),
		Exists: PGResourceExistsFunc(resourcePostgreSQLRoleExists),
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},

		Schema: map[string]*schema.Schema{
			roleNameAttr: {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The name of the role. Renaming a role forces it to be destroyed and recreated, which drops all associated grants.",
			},
			rolePasswordAttr: {
				Type:        schema.TypeString,
				Optional:    true,
				Sensitive:   true,
				Description: "Sets the role's password",
			},
			roleRolesAttr: {
				Type:        schema.TypeSet,
				Optional:    true,
				Elem:        &schema.Schema{Type: schema.TypeString},
				Set:         schema.HashString,
				MinItems:    0,
				Description: "Role(s) to grant to this new role",
			},
			roleSearchPathAttr: {
				Type:        schema.TypeList,
				Optional:    true,
				Elem:        &schema.Schema{Type: schema.TypeString},
				MinItems:    0,
				Description: "Sets the role's search path",
			},
			roleValidUntilAttr: {
				Type:        schema.TypeString,
				Optional:    true,
				Default:     "infinity",
				Description: "Sets a date and time after which the role's password is no longer valid",
			},
			roleCreateDBAttr: {
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     false,
				Description: "Define a role's ability to create databases",
			},
			roleCreateRoleAttr: {
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     false,
				Description: "Determine whether this role will be permitted to create new roles",
			},
			roleIdleInTransactionSessionTimeoutAttr: {
				Type:         schema.TypeInt,
				Optional:     true,
				Description:  "Terminate any session with an open transaction that has been idle for longer than the specified duration in milliseconds",
				ValidateFunc: validation.IntAtLeast(0),
			},
			roleLoginAttr: {
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     false,
				Description: "Determine whether a role is allowed to log in",
			},
			roleBypassRLSAttr: {
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     false,
				Description: "Determine whether a role bypasses every row-level security (RLS) policy",
			},
			roleSkipDropRoleAttr: {
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     false,
				Description: "Skip actually running the DROP ROLE command when removing a ROLE from PostgreSQL",
			},
			roleSkipReassignOwnedAttr: {
				Type:        schema.TypeBool,
				Optional:    true,
				Default:     false,
				Description: "Skip actually running the REASSIGN OWNED command when removing a role from PostgreSQL",
			},
			roleStatementTimeoutAttr: {
				Type:         schema.TypeInt,
				Optional:     true,
				Description:  "Abort any statement that takes more than the specified number of milliseconds",
				ValidateFunc: validation.IntAtLeast(0),
			},
			defaultTransactionIsolationAttr: {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "Role default_transaction_isolation",
			},
			defaultTransactionFollowerReadsAttr: {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "Role default_transaction_use_follower_reads",
			},
		},
	}
}

func resourcePostgreSQLRoleCreate(db *DBConnection, d *schema.ResourceData) error {
	stringOpts := []struct {
		hclKey string
		sqlKey string
	}{
		{rolePasswordAttr, "PASSWORD"},
		{roleValidUntilAttr, "VALID UNTIL"},
	}
	intOpts := []struct {
		hclKey string
		sqlKey string
	}{}
	type boolOptType struct {
		hclKey        string
		sqlKeyEnable  string
		sqlKeyDisable string
	}
	boolOpts := []boolOptType{
		{roleCreateDBAttr, "CREATEDB", "NOCREATEDB"},
		{roleCreateRoleAttr, "CREATEROLE", "NOCREATEROLE"},
		{roleLoginAttr, "LOGIN", "NOLOGIN"},
	}

	if db.featureSupported(featureRLS) {
		boolOpts = append(boolOpts, boolOptType{roleBypassRLSAttr, "BYPASSRLS", "NOBYPASSRLS"})
	}

	createOpts := make([]string, 0, len(stringOpts)+len(intOpts)+len(boolOpts))

	for _, opt := range stringOpts {
		v, ok := d.GetOk(opt.hclKey)
		if !ok {
			continue
		}

		val := v.(string)
		if val != "" {
			switch {
			case opt.hclKey == rolePasswordAttr:
				if strings.ToUpper(v.(string)) == "NULL" {
					createOpts = append(createOpts, "PASSWORD NULL")
				} else {
					createOpts = append(createOpts, fmt.Sprintf("%s '%s'", opt.sqlKey, pqQuoteLiteral(val)))
				}

			case opt.hclKey == roleValidUntilAttr:
				switch {
				case v.(string) == "", strings.ToLower(v.(string)) == "infinity":
					createOpts = append(createOpts, fmt.Sprintf("%s '%s'", opt.sqlKey, "294276-12-31 23:59:59"))
				default:
					createOpts = append(createOpts, fmt.Sprintf("%s '%s'", opt.sqlKey, pqQuoteLiteral(val)))
				}
			default:
				createOpts = append(createOpts, fmt.Sprintf("%s %s", opt.sqlKey, pq.QuoteIdentifier(val)))
			}
		}
	}

	for _, opt := range intOpts {
		val := d.Get(opt.hclKey).(int)
		createOpts = append(createOpts, fmt.Sprintf("%s %d", opt.sqlKey, val))
	}

	for _, opt := range boolOpts {
		val := d.Get(opt.hclKey).(bool)
		valStr := opt.sqlKeyDisable
		if val {
			valStr = opt.sqlKeyEnable
		}
		createOpts = append(createOpts, valStr)
	}

	roleName := d.Get(roleNameAttr).(string)
	createStr := strings.Join(createOpts, " ")
	if len(createOpts) > 0 {
		createStr = " WITH " + createStr
	}

	sqlStr := fmt.Sprintf("CREATE ROLE %s%s", pq.QuoteIdentifier(roleName), createStr)
	if _, err := db.Exec(sqlStr); err != nil {
		return fmt.Errorf("error creating role %s: %w", roleName, err)
	}

	if err := grantRoles(db, d); err != nil {
		return err
	}

	if err := alterSearchPath(db, d); err != nil {
		return err
	}

	if err := setStatementTimeout(db, d); err != nil {
		return err
	}

	if err := setIdleInTransactionSessionTimeout(db, d); err != nil {
		return err
	}

	if db.featureSupported(featureTransactionIsolation) {
		if err := setDefaultTransactionIsolation(db, d); err != nil {
			return err
		}
	}

	if db.featureSupported(featureFollowerReads) {
		if err := setDefaultFollowerReads(db, d); err != nil {
			return err
		}
	}

	d.SetId(roleName)

	return resourcePostgreSQLRoleReadImpl(db, d)
}

func resourcePostgreSQLRoleDelete(db *DBConnection, d *schema.ResourceData) error {
	roleName := d.Get(roleNameAttr).(string)

	if !d.Get(roleSkipReassignOwnedAttr).(bool) {
		currentUser := db.client.config.getDatabaseUsername()
		if _, err := db.Exec(fmt.Sprintf("REASSIGN OWNED BY %s TO %s", pq.QuoteIdentifier(roleName), pq.QuoteIdentifier(currentUser))); err != nil {
			return fmt.Errorf("could not reassign owned by role %s to %s: %w", roleName, currentUser, err)
		}
		if _, err := db.Exec(fmt.Sprintf("DROP OWNED BY %s", pq.QuoteIdentifier(roleName))); err != nil {
			return fmt.Errorf("could not drop owned by role %s: %w", roleName, err)
		}
	}
	if !d.Get(roleSkipDropRoleAttr).(bool) {
		if _, err := db.Exec(fmt.Sprintf("DROP ROLE %s", pq.QuoteIdentifier(roleName))); err != nil {
			return fmt.Errorf("could not delete role %s: %w", roleName, err)
		}
	}

	d.SetId("")

	return nil
}

func resourcePostgreSQLRoleExists(db *DBConnection, d *schema.ResourceData) (bool, error) {
	var roleName string
	err := db.QueryRow("SELECT rolname FROM pg_catalog.pg_roles WHERE rolname=$1", d.Id()).Scan(&roleName)
	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, err
	}

	return true, nil
}

func resourcePostgreSQLRoleRead(db *DBConnection, d *schema.ResourceData) error {
	return resourcePostgreSQLRoleReadImpl(db, d)
}

func resourcePostgreSQLRoleReadImpl(db *DBConnection, d *schema.ResourceData) error {
	var roleCreateRole, roleCreateDB, roleCanLogin, roleBypassRLS bool
	var roleName, roleValidUntil string
	var roleRoles pq.ByteaArray

	roleID := d.Id()

	columns := []string{
		"rolname",
		"rolcreaterole",
		"rolcreatedb",
		"rolcanlogin",
		`COALESCE(rolvaliduntil::TEXT, 'infinity')`,
	}

	// values must match the SELECT column order: ARRAY(...) subquery first, then each entry in columns.
	values := []interface{}{
		&roleRoles,
		&roleName,
		&roleCreateRole,
		&roleCreateDB,
		&roleCanLogin,
		&roleValidUntil,
	}

	if db.featureSupported(featureRLS) {
		columns = append(columns, "rolbypassrls")
		values = append(values, &roleBypassRLS)
	}

	roleSQL := fmt.Sprintf(`SELECT ARRAY(
			SELECT pg_get_userbyid(roleid) FROM pg_catalog.pg_auth_members members WHERE member = pg_roles.oid
		), %s
		FROM pg_catalog.pg_roles WHERE rolname=$1`,
		// select columns
		strings.Join(columns, ", "),
	)
	err := db.QueryRow(roleSQL, roleID).Scan(values...)

	switch {
	case err == sql.ErrNoRows:
		log.Printf("[WARN] PostgreSQL ROLE (%s) not found", roleID)
		d.SetId("")
		return nil
	case err != nil:
		return fmt.Errorf("Error reading ROLE: %w", err)
	}

	// pg_db_role_setting is the authoritative source for role settings on all CRDB versions.
	// pg_roles.rolconfig can retain stale values after a RESET, so it is not used.
	var roleConfig pq.ByteaArray
	settingSQL := `SELECT setconfig FROM pg_catalog.pg_db_role_setting
		WHERE setrole = (SELECT oid FROM pg_catalog.pg_roles WHERE rolname=$1) AND setdatabase = 0`
	if settingErr := db.QueryRow(settingSQL, roleID).Scan(&roleConfig); settingErr != nil && settingErr != sql.ErrNoRows {
		return fmt.Errorf("Error reading role settings from pg_db_role_setting: %w", settingErr)
	}
	// If settingErr == sql.ErrNoRows, roleConfig remains nil — no settings configured.

	d.Set(roleNameAttr, roleName)
	d.Set(roleCreateDBAttr, roleCreateDB)
	d.Set(roleCreateRoleAttr, roleCreateRole)
	d.Set(roleLoginAttr, roleCanLogin)
	d.Set(roleSkipDropRoleAttr, d.Get(roleSkipDropRoleAttr).(bool))
	d.Set(roleSkipReassignOwnedAttr, d.Get(roleSkipReassignOwnedAttr).(bool))
	// CockroachDB stores VALID UNTIL 'infinity' as '294276-12-31 23:59:59' due to a bug.
	// Normalize it back to "infinity" so the state matches the config default.
	// https://github.com/cockroachdb/cockroach/issues/116714
	if strings.HasPrefix(roleValidUntil, "294276-12-31 23:59:59") {
		roleValidUntil = "infinity"
	}
	d.Set(roleValidUntilAttr, roleValidUntil)
	d.Set(roleBypassRLSAttr, roleBypassRLS)
	d.Set(roleRolesAttr, pgArrayToSet(roleRoles))
	d.Set(roleSearchPathAttr, readSearchPath(roleConfig))

	statementTimeout, err := readStatementTimeout(roleConfig)
	if err != nil {
		return err
	}

	d.Set(roleStatementTimeoutAttr, statementTimeout)

	idleInTransactionSessionTimeout, err := readIdleInTransactionSessionTimeout(roleConfig)
	if err != nil {
		return err
	}

	d.Set(roleIdleInTransactionSessionTimeoutAttr, idleInTransactionSessionTimeout)

	d.SetId(roleName)

	d.Set(defaultTransactionIsolationAttr, readDefaultTransactionIsolation(roleConfig))
	d.Set(defaultTransactionFollowerReadsAttr, readFollowerReads(roleConfig))

	password, err := readRolePassword(db, d, roleCanLogin)
	if err != nil {
		return err
	}

	d.Set(rolePasswordAttr, password)

	return nil
}

// readSearchPath searches for a search_path entry in the rolconfig array.
// In case no such value is present, it returns nil.
func readSearchPath(roleConfig pq.ByteaArray) []string {
	for _, v := range roleConfig {
		config := string(v)
		if strings.HasPrefix(config, roleSearchPathAttr) {
			var result = strings.Split(strings.TrimPrefix(config, roleSearchPathAttr+"="), ", ")
			for i := range result {
				result[i] = strings.Trim(result[i], `"`)
			}
			return result
		}
	}
	return nil
}

// readIdleInTransactionSessionTimeout searches for a idle_in_transaction_session_timeout entry in the rolconfig array.
// In case no such value is present, it returns nil.
func readIdleInTransactionSessionTimeout(roleConfig pq.ByteaArray) (int, error) {
	for _, v := range roleConfig {
		config := string(v)
		if strings.HasPrefix(config, roleIdleInTransactionSessionTimeoutAttr) {
			val := strings.TrimPrefix(config, roleIdleInTransactionSessionTimeoutAttr+"=")
			return parseDurationToMillis(val)
		}
	}
	return 0, nil
}

// readStatementTimeout searches for a statement_timeout entry in the rolconfig array.
// In case no such value is present, it returns nil.
func readStatementTimeout(roleConfig pq.ByteaArray) (int, error) {
	for _, v := range roleConfig {
		config := string(v)
		if strings.HasPrefix(config, roleStatementTimeoutAttr) {
			val := strings.TrimPrefix(config, roleStatementTimeoutAttr+"=")
			return parseDurationToMillis(val)
		}
	}
	return 0, nil
}

// parseDurationToMillis parses a duration value stored by CockroachDB (e.g. "30s", "500ms", "1m0s")
// or a plain integer millisecond string, returning the value in milliseconds.
func parseDurationToMillis(val string) (int, error) {
	// Try plain integer first (legacy / PostgreSQL format)
	if ms, err := strconv.Atoi(val); err == nil {
		return ms, nil
	}
	// Fall back to Go duration format used by CockroachDB
	d, err := time.ParseDuration(val)
	if err != nil {
		return -1, fmt.Errorf("cannot parse duration %q: %w", val, err)
	}
	return int(d.Milliseconds()), nil
}

func readDefaultTransactionIsolation(roleConfig pq.ByteaArray) string {
	for _, v := range roleConfig {
		config := string(v)
		if strings.HasPrefix(config, "default_transaction_isolation=") {
			return strings.TrimPrefix(config, "default_transaction_isolation=")
		}
	}
	return ""
}

func readFollowerReads(roleConfig pq.ByteaArray) string {
	for _, v := range roleConfig {
		config := string(v)
		if strings.HasPrefix(config, "default_transaction_use_follower_reads=") {
			return strings.TrimPrefix(config, "default_transaction_use_follower_reads=")
		}
	}
	return ""
}

// readRolePassword reads password from Terraform state.
func readRolePassword(db *DBConnection, d *schema.ResourceData, roleCanLogin bool) (string, error) {
	return d.Get(rolePasswordAttr).(string), nil
}

func resourcePostgreSQLRoleUpdate(db *DBConnection, d *schema.ResourceData) error {
	if err := setRolePassword(db, d); err != nil {
		return err
	}

	if db.featureSupported(featureRLS) {
		if err := setRoleBypassRLS(db, d); err != nil {
			return err
		}
	}

	if err := setRoleCreateDB(db, d); err != nil {
		return err
	}

	if err := setRoleCreateRole(db, d); err != nil {
		return err
	}

	if err := setRoleLogin(db, d); err != nil {
		return err
	}

	if err := setRoleValidUntil(db, d); err != nil {
		return err
	}

	// applying roles: let's revoke all / grant the right ones
	if err := revokeRoles(db, d); err != nil {
		return err
	}

	if err := grantRoles(db, d); err != nil {
		return err
	}

	if err := alterSearchPath(db, d); err != nil {
		return err
	}

	if err := setStatementTimeout(db, d); err != nil {
		return err
	}

	if err := setIdleInTransactionSessionTimeout(db, d); err != nil {
		return err
	}

	if db.featureSupported(featureTransactionIsolation) {
		if err := setDefaultTransactionIsolation(db, d); err != nil {
			return err
		}
	}

	if db.featureSupported(featureFollowerReads) {
		if err := setDefaultFollowerReads(db, d); err != nil {
			return err
		}
	}

	return resourcePostgreSQLRoleReadImpl(db, d)
}

func setRolePassword(db QueryAble, d *schema.ResourceData) error {
	if !d.HasChange(rolePasswordAttr) {
		return nil
	}

	roleName := d.Get(roleNameAttr).(string)
	password := d.Get(rolePasswordAttr).(string)

	sqlStr := fmt.Sprintf("ALTER ROLE %s PASSWORD '%s'", pq.QuoteIdentifier(roleName), pqQuoteLiteral(password))
	if _, err := db.Exec(sqlStr); err != nil {
		return fmt.Errorf("Error updating role password: %w", err)
	}
	return nil
}

func setRoleBypassRLS(db *DBConnection, d *schema.ResourceData) error {
	if !d.HasChange(roleBypassRLSAttr) {
		return nil
	}

	if !db.featureSupported(featureRLS) {
		return fmt.Errorf("PostgreSQL client is talking with a server (%q) that does not support PostgreSQL Row-Level Security", db.version.String())
	}

	bypassRLS := d.Get(roleBypassRLSAttr).(bool)
	tok := "NOBYPASSRLS"
	if bypassRLS {
		tok = "BYPASSRLS"
	}
	roleName := d.Get(roleNameAttr).(string)
	sqlStr := fmt.Sprintf("ALTER ROLE %s WITH %s", pq.QuoteIdentifier(roleName), tok)
	if _, err := db.Exec(sqlStr); err != nil {
		return fmt.Errorf("Error updating role BYPASSRLS: %w", err)
	}

	return nil
}

func setRoleCreateDB(db QueryAble, d *schema.ResourceData) error {
	if !d.HasChange(roleCreateDBAttr) {
		return nil
	}

	createDB := d.Get(roleCreateDBAttr).(bool)
	tok := "NOCREATEDB"
	if createDB {
		tok = "CREATEDB"
	}
	roleName := d.Get(roleNameAttr).(string)
	sqlStr := fmt.Sprintf("ALTER ROLE %s WITH %s", pq.QuoteIdentifier(roleName), tok)
	if _, err := db.Exec(sqlStr); err != nil {
		return fmt.Errorf("Error updating role CREATEDB: %w", err)
	}

	return nil
}

func setRoleCreateRole(db QueryAble, d *schema.ResourceData) error {
	if !d.HasChange(roleCreateRoleAttr) {
		return nil
	}

	createRole := d.Get(roleCreateRoleAttr).(bool)
	tok := "NOCREATEROLE"
	if createRole {
		tok = "CREATEROLE"
	}
	roleName := d.Get(roleNameAttr).(string)
	sqlStr := fmt.Sprintf("ALTER ROLE %s WITH %s", pq.QuoteIdentifier(roleName), tok)
	if _, err := db.Exec(sqlStr); err != nil {
		return fmt.Errorf("Error updating role CREATEROLE: %w", err)
	}

	return nil
}

func setRoleLogin(db QueryAble, d *schema.ResourceData) error {
	if !d.HasChange(roleLoginAttr) {
		return nil
	}

	login := d.Get(roleLoginAttr).(bool)
	tok := "NOLOGIN"
	if login {
		tok = "LOGIN"
	}
	roleName := d.Get(roleNameAttr).(string)
	sqlStr := fmt.Sprintf("ALTER ROLE %s WITH %s", pq.QuoteIdentifier(roleName), tok)
	if _, err := db.Exec(sqlStr); err != nil {
		return fmt.Errorf("Error updating role LOGIN: %w", err)
	}

	return nil
}

func setRoleValidUntil(db QueryAble, d *schema.ResourceData) error {
	if !d.HasChange(roleValidUntilAttr) {
		return nil
	}

	validUntil := d.Get(roleValidUntilAttr).(string)
	if validUntil == "" {
		return nil
	} else if strings.ToLower(validUntil) == "infinity" {
		validUntil = "294276-12-31 23:59:59"
	}

	roleName := d.Get(roleNameAttr).(string)
	sqlStr := fmt.Sprintf("ALTER ROLE %s VALID UNTIL '%s'", pq.QuoteIdentifier(roleName), pqQuoteLiteral(validUntil))
	if _, err := db.Exec(sqlStr); err != nil {
		return fmt.Errorf("Error updating role VALID UNTIL: %w", err)
	}

	return nil
}

func revokeRoles(db QueryAble, d *schema.ResourceData) error {
	role := d.Get(roleNameAttr).(string)

	query := `SELECT pg_get_userbyid(roleid)
		FROM pg_catalog.pg_auth_members members
		JOIN pg_catalog.pg_roles ON members.member = pg_roles.oid
		WHERE rolname = $1`

	rows, err := db.Query(query, role)
	if err != nil {
		return fmt.Errorf("could not get roles list for role %s: %w", role, err)
	}
	defer rows.Close()

	grantedRoles := []string{}
	for rows.Next() {
		var grantedRole string

		if err = rows.Scan(&grantedRole); err != nil {
			return fmt.Errorf("could not scan role name for role %s: %w", role, err)
		}
		// We cannot revoke directly here as it shares the same cursor (with Tx)
		// and rows.Next seems to retrieve result row by row.
		// see: https://github.com/lib/pq/issues/81
		grantedRoles = append(grantedRoles, grantedRole)
	}

	for _, grantedRole := range grantedRoles {
		query = fmt.Sprintf("REVOKE %s FROM %s", pq.QuoteIdentifier(grantedRole), pq.QuoteIdentifier(role))

		log.Printf("[DEBUG] revoking role %s from %s", grantedRole, role)
		if _, err := db.Exec(query); err != nil {
			return fmt.Errorf("could not revoke role %s from %s: %w", string(grantedRole), role, err)
		}
	}

	return nil
}

func grantRoles(db QueryAble, d *schema.ResourceData) error {
	role := d.Get(roleNameAttr).(string)

	for _, grantingRole := range d.Get("roles").(*schema.Set).List() {
		query := fmt.Sprintf(
			"GRANT %s TO %s", pq.QuoteIdentifier(grantingRole.(string)), pq.QuoteIdentifier(role),
		)
		if _, err := db.Exec(query); err != nil {
			return fmt.Errorf("could not grant role %s to %s: %w", grantingRole, role, err)
		}
	}
	return nil
}

func alterSearchPath(db QueryAble, d *schema.ResourceData) error {
	role := d.Get(roleNameAttr).(string)
	searchPathInterface := d.Get(roleSearchPathAttr).([]interface{})

	var searchPathString []string
	if len(searchPathInterface) > 0 {
		searchPathString = make([]string, len(searchPathInterface))
		for i, searchPathPart := range searchPathInterface {
			if strings.Contains(searchPathPart.(string), ", ") {
				return fmt.Errorf("search_path cannot contain `, `: %v", searchPathPart)
			}
			searchPathString[i] = pq.QuoteIdentifier(searchPathPart.(string))
		}
	} else {
		searchPathString = []string{"DEFAULT"}
	}
	searchPath := strings.Join(searchPathString[:], ", ")

	query := fmt.Sprintf(
		"ALTER ROLE %s SET search_path TO %s", pq.QuoteIdentifier(role), searchPath,
	)
	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("could not set search_path %s for %s: %w", searchPath, role, err)
	}
	return nil
}

func setStatementTimeout(db QueryAble, d *schema.ResourceData) error {
	if !d.HasChange(roleStatementTimeoutAttr) {
		return nil
	}

	roleName := d.Get(roleNameAttr).(string)
	statementTimeout := d.Get(roleStatementTimeoutAttr).(int)
	if statementTimeout != 0 {
		sqlStr := fmt.Sprintf(
			"ALTER ROLE %s SET statement_timeout TO %d", pq.QuoteIdentifier(roleName), statementTimeout,
		)
		if _, err := db.Exec(sqlStr); err != nil {
			return fmt.Errorf("could not set statement_timeout %d for %s: %w", statementTimeout, roleName, err)
		}
	} else {
		sqlStr := fmt.Sprintf(
			"ALTER ROLE %s RESET statement_timeout", pq.QuoteIdentifier(roleName),
		)
		if _, err := db.Exec(sqlStr); err != nil {
			return fmt.Errorf("could not reset statement_timeout for %s: %w", roleName, err)
		}
	}
	return nil
}

func setIdleInTransactionSessionTimeout(db QueryAble, d *schema.ResourceData) error {
	if !d.HasChange(roleIdleInTransactionSessionTimeoutAttr) {
		return nil
	}

	roleName := d.Get(roleNameAttr).(string)
	idleInTransactionSessionTimeout := d.Get(roleIdleInTransactionSessionTimeoutAttr).(int)
	if idleInTransactionSessionTimeout != 0 {
		sqlStr := fmt.Sprintf(
			"ALTER ROLE %s SET idle_in_transaction_session_timeout TO %d", pq.QuoteIdentifier(roleName), idleInTransactionSessionTimeout,
		)
		if _, err := db.Exec(sqlStr); err != nil {
			return fmt.Errorf("could not set idle_in_transaction_session_timeout %d for %s: %w", idleInTransactionSessionTimeout, roleName, err)
		}
	} else {
		sqlStr := fmt.Sprintf(
			"ALTER ROLE %s RESET idle_in_transaction_session_timeout", pq.QuoteIdentifier(roleName),
		)
		if _, err := db.Exec(sqlStr); err != nil {
			return fmt.Errorf("could not reset idle_in_transaction_session_timeout for %s: %w", roleName, err)
		}
	}
	return nil
}

func setDefaultTransactionIsolation(db QueryAble, d *schema.ResourceData) error {
	if !d.HasChange(defaultTransactionIsolationAttr) {
		return nil
	}

	roleName := d.Get(roleNameAttr).(string)
	defaultTransactionIsolation := d.Get(defaultTransactionIsolationAttr).(string)
	if defaultTransactionIsolation != "" {
		sqlStr := fmt.Sprintf(
			"ALTER ROLE %s SET default_transaction_isolation = %s", pq.QuoteIdentifier(roleName), pq.QuoteIdentifier(defaultTransactionIsolation),
		)
		if _, err := db.Exec(sqlStr); err != nil {
			return fmt.Errorf("could not set default_transaction_isolation %s for %s: %w", defaultTransactionIsolation, roleName, err)
		}
	} else {
		// Only RESET when clearing a previously set value; skip no-op RESET on Create.
		oldValue, _ := d.GetChange(defaultTransactionIsolationAttr)
		if oldValue.(string) == "" {
			return nil
		}
		sqlStr := fmt.Sprintf(
			"ALTER ROLE %s RESET default_transaction_isolation", pq.QuoteIdentifier(roleName),
		)
		if _, err := db.Exec(sqlStr); err != nil {
			return fmt.Errorf("could not reset default_transaction_isolation for %s: %w", roleName, err)
		}
	}
	return nil
}

func setDefaultFollowerReads(db QueryAble, d *schema.ResourceData) error {
	if !d.HasChange(defaultTransactionFollowerReadsAttr) {
		return nil
	}

	roleName := d.Get(roleNameAttr).(string)
	defaultFollowerReads := d.Get(defaultTransactionFollowerReadsAttr).(string)
	if defaultFollowerReads != "" {
		sqlStr := fmt.Sprintf(
			"ALTER ROLE %s SET default_transaction_use_follower_reads = %s", pq.QuoteIdentifier(roleName), pq.QuoteIdentifier(defaultFollowerReads),
		)
		if _, err := db.Exec(sqlStr); err != nil {
			return fmt.Errorf("could not set default_transaction_use_follower_reads %s for %s: %w", defaultFollowerReads, roleName, err)
		}
	} else {
		// Only RESET when clearing a previously set value; skip no-op RESET on Create.
		oldValue, _ := d.GetChange(defaultTransactionFollowerReadsAttr)
		if oldValue.(string) == "" {
			return nil
		}
		sqlStr := fmt.Sprintf(
			"ALTER ROLE %s RESET default_transaction_use_follower_reads", pq.QuoteIdentifier(roleName),
		)
		if _, err := db.Exec(sqlStr); err != nil {
			return fmt.Errorf("could not reset default_transaction_use_follower_reads for %s: %w", roleName, err)
		}
	}
	return nil
}
