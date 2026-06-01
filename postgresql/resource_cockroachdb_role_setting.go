package postgresql

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"github.com/lib/pq"
)

const (
	RoleName     = "role_name"
	SettingName  = "setting_name"
	SettingValue = "setting_value"
)

func resourceCockroachDBRoleSetting() *schema.Resource {
	return &schema.Resource{
		Create: PGResourceFunc(resourceCockroachDBRoleSettingCreate),
		Read:   PGResourceFunc(resourceCockroachDBRoleSettingRead),
		Update: PGResourceFunc(resourceCockroachDBRoleSettingUpdate),
		Delete: PGResourceFunc(resourceCockroachDBRoleSettingDelete),
		Exists: PGResourceExistsFunc(resourceCockroachDBRoleSettingExists),
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},
		Schema: map[string]*schema.Schema{
			RoleName: {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validation.StringIsNotEmpty,
				Description:  "Role to apply the setting to. Use 'ALL' to apply to all roles.",
			},
			SettingName: {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validation.StringIsNotEmpty,
				Description:  "Role setting name (e.g. 'statement_timeout').",
			},
			SettingValue: {
				Type:         schema.TypeString,
				Required:     true,
				ValidateFunc: validation.StringIsNotEmpty,
				Description:  "Role setting value (e.g. '1m').",
			},
		},
	}
}

func resourceCockroachDBRoleSettingCreate(db *DBConnection, d *schema.ResourceData) error {
	roleName := d.Get(RoleName).(string)
	settingName := d.Get(SettingName).(string)
	settingValue := d.Get(SettingValue).(string)

	if err := execAlterRoleSet(db, roleName, settingName, settingValue); err != nil {
		return err
	}

	d.SetId(fmt.Sprintf("%s:%s", roleName, settingName))
	return nil
}

func resourceCockroachDBRoleSettingRead(db *DBConnection, d *schema.ResourceData) error {
	return resourceCockroachDBRoleSettingReadImpl(db, d)
}

func resourceCockroachDBRoleSettingReadImpl(db *DBConnection, d *schema.ResourceData) error {
	roleName, settingName, err := parseRoleSettingID(d)
	if err != nil {
		return err
	}

	value, found, err := readRoleSetting(db, roleName, settingName)
	if err != nil {
		return err
	}
	if !found {
		d.SetId("")
		return nil
	}

	d.Set(RoleName, roleName)
	d.Set(SettingName, settingName)
	d.Set(SettingValue, value)
	return nil
}

func resourceCockroachDBRoleSettingUpdate(db *DBConnection, d *schema.ResourceData) error {
	if d.HasChange(SettingValue) {
		roleName := d.Get(RoleName).(string)
		settingName := d.Get(SettingName).(string)
		settingValue := d.Get(SettingValue).(string)

		if err := execAlterRoleSet(db, roleName, settingName, settingValue); err != nil {
			return err
		}
	}
	return resourceCockroachDBRoleSettingReadImpl(db, d)
}

func resourceCockroachDBRoleSettingDelete(db *DBConnection, d *schema.ResourceData) error {
	roleName := d.Get(RoleName).(string)
	settingName := d.Get(SettingName).(string)

	roleRef := roleSettingRoleRef(roleName)
	query := fmt.Sprintf("ALTER ROLE %s RESET %s", roleRef, pq.QuoteIdentifier(settingName))
	if _, err := db.ExecRetry(query); err != nil {
		return fmt.Errorf("error resetting session setting %s for role %s: %w", settingName, roleName, err)
	}

	d.SetId("")
	return nil
}

func resourceCockroachDBRoleSettingExists(db *DBConnection, d *schema.ResourceData) (bool, error) {
	roleName, settingName, err := parseRoleSettingID(d)
	if err != nil {
		return false, err
	}

	_, found, err := readRoleSetting(db, roleName, settingName)
	return found, err
}

// execAlterRoleSet runs ALTER ROLE <role> SET <setting> = '<value>'.
func execAlterRoleSet(db *DBConnection, roleName, settingName, settingValue string) error {
	roleRef := roleSettingRoleRef(roleName)
	query := fmt.Sprintf(
		"ALTER ROLE %s SET %s = '%s'",
		roleRef,
		pq.QuoteIdentifier(settingName),
		pqQuoteLiteral(settingValue),
	)
	if _, err := db.ExecRetry(query); err != nil {
		return fmt.Errorf("error setting %s for role %s: %w", settingName, roleName, err)
	}
	return nil
}

// roleSettingRoleRef formats the role reference for ALTER ROLE statements.
// "ALL" is a SQL keyword and must not be quoted as an identifier.
func roleSettingRoleRef(roleName string) string {
	if strings.EqualFold(roleName, "ALL") {
		return "ALL"
	}
	return pq.QuoteIdentifier(roleName)
}

func parseRoleSettingID(d *schema.ResourceData) (roleName, settingName string, err error) {
	roleName = d.Get(RoleName).(string)
	settingName = d.Get(SettingName).(string)

	// During import the fields are not yet populated; derive them from the ID.
	if roleName == "" || settingName == "" {
		parts := strings.SplitN(d.Id(), ":", 2)
		if len(parts) != 2 {
			return "", "", fmt.Errorf("invalid resource ID %q: expected <role_name>:<setting_name>", d.Id())
		}
		roleName = parts[0]
		settingName = parts[1]
	}
	return roleName, settingName, nil
}

// readRoleSetting queries system.database_role_settings and returns the
// stored value for the given role and setting name. found is false when the
// setting has not been configured for that role.
//
// CockroachDB stores ALTER ROLE ALL settings with role_id = 0 and database_id = 0
// (global, not scoped to a specific database).
func readRoleSetting(db *DBConnection, roleName, settingName string) (value string, found bool, err error) {
	prefix := strings.ToLower(settingName) + "="

	rows, err := roleSettingRows(db, roleName)
	if err != nil {
		return "", false, fmt.Errorf("error reading session settings for role %s: %w", roleName, err)
	}
	defer rows.Close()

	for rows.Next() {
		var entry string
		if scanErr := rows.Scan(&entry); scanErr != nil {
			return "", false, fmt.Errorf("error scanning session setting: %w", scanErr)
		}
		// Each entry is "key=value"; split on the first '=' only.
		if strings.HasPrefix(strings.ToLower(entry), prefix) {
			return entry[len(prefix):], true, nil
		}
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return "", false, fmt.Errorf("error iterating session settings: %w", rowsErr)
	}

	return "", false, nil
}

// roleSettingRows returns the individual setting entries for a role from
// system.database_role_settings. The settings column is a STRING[] where each
// element is "key=value"; unnest expands it to one row per entry.
func roleSettingRows(db *DBConnection, roleName string) (*sql.Rows, error) {
	if strings.EqualFold(roleName, "ALL") {
		// ALTER ROLE ALL is stored with role_id = 0 (no named role).
		return db.QueryRetry(
			"SELECT unnest(settings) FROM system.database_role_settings WHERE database_id = 0 AND role_id = 0",
		)
	}
	return db.QueryRetry(
		"SELECT unnest(settings) FROM system.database_role_settings WHERE database_id = 0 AND role_name = $1",
		roleName,
	)
}
