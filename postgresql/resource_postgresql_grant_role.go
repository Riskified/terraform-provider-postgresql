package postgresql

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/lib/pq"
)

func resourcePostgreSQLGrantRole() *schema.Resource {
	return &schema.Resource{
		Create: PGResourceFunc(resourcePostgreSQLGrantRoleCreate),
		Read:   PGResourceFunc(resourcePostgreSQLGrantRoleRead),
		Delete: PGResourceFunc(resourcePostgreSQLGrantRoleDelete),

		Schema: map[string]*schema.Schema{
			"role": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The name of the role to grant grant_role",
			},
			"grant_role": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    true,
				Description: "The name of the role that is granted to role",
			},
			"with_admin_option": {
				Type:        schema.TypeBool,
				Optional:    true,
				ForceNew:    true,
				Default:     false,
				Description: "Permit the grant recipient to grant it to others",
			},
		},
	}
}

func resourcePostgreSQLGrantRoleRead(db *DBConnection, d *schema.ResourceData) error {
	if !db.featureSupported(featurePrivileges) {
		return fmt.Errorf(
			"postgresql_grant_role resource is not supported for this Postgres version (%s)",
			db.version,
		)
	}

	return readGrantRole(db, d)
}

func resourcePostgreSQLGrantRoleCreate(db *DBConnection, d *schema.ResourceData) error {
	if !db.featureSupported(featurePrivileges) {
		return fmt.Errorf(
			"postgresql_grant_role resource is not supported for this Postgres version (%s)",
			db.version,
		)
	}

	// Revoke the granted roles before granting them again.
	if err := revokeRoleWithDB(db, d); err != nil {
		return err
	}

	if err := grantRoleWithDB(db, d); err != nil {
		return err
	}

	d.SetId(generateGrantRoleID(d))

	return readGrantRole(db, d)
}

func resourcePostgreSQLGrantRoleDelete(db *DBConnection, d *schema.ResourceData) error {
	if !db.featureSupported(featurePrivileges) {
		return fmt.Errorf(
			"postgresql_grant_role resource is not supported for this Postgres version (%s)",
			db.version,
		)
	}

	if err := revokeRoleWithDB(db, d); err != nil {
		return err
	}

	return nil
}

func readGrantRole(db *DBConnection, d *schema.ResourceData) error {
	var roleName, grantRoleName string
	var withAdminOption bool

	values := []interface{}{
		&roleName,
		&grantRoleName,
		&withAdminOption,
	}

	query := fmt.Sprintf(` with a as (show grants on role %s for %s) select member as role , role_name as grant_role, is_admin as with_admin_option from a;
`, pq.QuoteIdentifier(d.Get("grant_role").(string)), pq.QuoteIdentifier(d.Get("role").(string)))
	err := db.QueryRow(query).Scan(values...)
	switch {
	case err == sql.ErrNoRows:
		log.Printf("[WARN] PostgreSQL grant role %s for %s not found, removing from state", d.Get("grant_role"), d.Get("role"))
		d.SetId("")
		return nil
	case err != nil:
		return fmt.Errorf("Error to show grants on role %s for %s :%w ", d.Get("grant_role"), d.Get("role"), err)
	}

	return nil
}

func createGrantRoleQuery(d *schema.ResourceData) string {
	grantRole, _ := d.Get("grant_role").(string)
	role, _ := d.Get("role").(string)

	query := fmt.Sprintf(
		"GRANT %s TO %s",
		pq.QuoteIdentifier(grantRole),
		pq.QuoteIdentifier(role),
	)
	if wao, _ := d.Get("with_admin_option").(bool); wao {
		query = query + " WITH ADMIN OPTION"
	}

	return query
}

func createRevokeRoleQuery(d *schema.ResourceData) string {
	grantRole, _ := d.Get("grant_role").(string)
	role, _ := d.Get("role").(string)

	return fmt.Sprintf(
		"REVOKE %s FROM %s",
		pq.QuoteIdentifier(grantRole),
		pq.QuoteIdentifier(role),
	)
}

// grantRoleWithDB grants a role using the DB connection directly
func grantRoleWithDB(db *DBConnection, d *schema.ResourceData) error {
	query := createGrantRoleQuery(d)
	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("could not execute grant query: %w", err)
	}
	return nil
}

// revokeRoleWithDB revokes a role using the DB connection directly
func revokeRoleWithDB(db *DBConnection, d *schema.ResourceData) error {
	query := createRevokeRoleQuery(d)
	if _, err := db.Exec(query); err != nil {
		// Ignore error if the role is not a member (for CockroachDB compatibility)
		if !strings.Contains(err.Error(), "is not a member") {
			return fmt.Errorf("could not execute revoke query: %w", err)
		}
	}
	return nil
}

func generateGrantRoleID(d *schema.ResourceData) string {
	return strings.Join([]string{d.Get("role").(string), d.Get("grant_role").(string), strconv.FormatBool(d.Get("with_admin_option").(bool))}, "_")
}
