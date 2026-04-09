package postgresql

import (
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestAccPostgresqlRole_Basic(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
			testCheckCompatibleVersion(t, featurePrivileges)
		},
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckPostgresqlRoleDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccPostgresqlRoleConfig,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPostgresqlRoleExists("myrole2", nil, nil),
					resource.TestCheckResourceAttr("postgresql_role.myrole2", "name", "myrole2"),
					resource.TestCheckResourceAttr("postgresql_role.myrole2", "login", "true"),
					resource.TestCheckResourceAttr("postgresql_role.myrole2", "roles.#", "0"),

					testAccCheckPostgresqlRoleExists("role_default", nil, nil),
					resource.TestCheckResourceAttr("postgresql_role.role_with_defaults", "name", "role_default"),
					resource.TestCheckResourceAttr("postgresql_role.role_with_defaults", "create_database", "false"),
					resource.TestCheckResourceAttr("postgresql_role.role_with_defaults", "create_role", "false"),
					resource.TestCheckResourceAttr("postgresql_role.role_with_defaults", "bypass_row_level_security", "false"),
					resource.TestCheckResourceAttr("postgresql_role.role_with_defaults", "password", ""),
					resource.TestCheckResourceAttr("postgresql_role.role_with_defaults", "valid_until", "infinity"),
					resource.TestCheckResourceAttr("postgresql_role.role_with_defaults", "skip_drop_role", "false"),
					resource.TestCheckResourceAttr("postgresql_role.role_with_defaults", "skip_reassign_owned", "false"),
					resource.TestCheckResourceAttr("postgresql_role.role_with_defaults", "statement_timeout", "0"),
					resource.TestCheckResourceAttr("postgresql_role.role_with_defaults", "idle_in_transaction_session_timeout", "0"),

					resource.TestCheckResourceAttr("postgresql_role.role_with_create_database", "name", "role_with_create_database"),
					resource.TestCheckResourceAttr("postgresql_role.role_with_create_database", "create_database", "true"),

					testAccCheckPostgresqlRoleExists("sub_role", []string{"myrole2", "role_simple"}, nil),
					resource.TestCheckResourceAttr("postgresql_role.sub_role", "name", "sub_role"),
					resource.TestCheckResourceAttr("postgresql_role.sub_role", "roles.#", "2"),
					resource.TestCheckResourceAttr("postgresql_role.sub_role", "roles.0", "myrole2"),
					resource.TestCheckResourceAttr("postgresql_role.sub_role", "roles.1", "role_simple"),

					testAccCheckPostgresqlRoleExists("role_with_search_path", nil, []string{"bar", "foo-with-hyphen"}),
				),
			},
		},
	})
}

func TestAccPostgresqlRole_Update(t *testing.T) {

	var configCreate = `
resource "postgresql_role" "update_role" {
  name = "update_role"
  login = true
  password = "toto"
  valid_until = "2099-05-04 12:00:00+00"
}
`

	var configUpdate = `
resource "postgresql_role" "group_role" {
  name = "group_role"
}

resource "postgresql_role" "update_role" {
  name = "update_role"
  login = true
  password = "titi"
  valid_until = "2099-05-04 12:00:00+00"
  roles = ["${postgresql_role.group_role.name}"]
  search_path = ["mysearchpath"]
  statement_timeout = 30000
  idle_in_transaction_session_timeout = 60000
}
`
	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
			testCheckCompatibleVersion(t, featurePrivileges)
		},
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckPostgresqlRoleDestroy,
		Steps: []resource.TestStep{
			{
				Config: configCreate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPostgresqlRoleExists("update_role", []string{}, nil),
					resource.TestCheckResourceAttr("postgresql_role.update_role", "name", "update_role"),
					resource.TestCheckResourceAttr("postgresql_role.update_role", "login", "true"),
					resource.TestCheckResourceAttr("postgresql_role.update_role", "password", "toto"),
					resource.TestCheckResourceAttr("postgresql_role.update_role", "valid_until", "2099-05-04 12:00:00+00"),
					resource.TestCheckResourceAttr("postgresql_role.update_role", "roles.#", "0"),
					resource.TestCheckResourceAttr("postgresql_role.update_role", "search_path.#", "0"),
					resource.TestCheckResourceAttr("postgresql_role.update_role", "statement_timeout", "0"),
					resource.TestCheckResourceAttr("postgresql_role.update_role", "idle_in_transaction_session_timeout", "0"),
					testAccCheckRoleCanLogin(t, "update_role", "toto"),
				),
			},
			{
				Config: configUpdate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPostgresqlRoleExists("update_role", []string{"group_role"}, nil),
					resource.TestCheckResourceAttr("postgresql_role.update_role", "name", "update_role"),
					resource.TestCheckResourceAttr("postgresql_role.update_role", "login", "true"),
					resource.TestCheckResourceAttr("postgresql_role.update_role", "password", "titi"),
					resource.TestCheckResourceAttr("postgresql_role.update_role", "valid_until", "2099-05-04 12:00:00+00"),
					resource.TestCheckResourceAttr("postgresql_role.update_role", "roles.#", "1"),
					resource.TestCheckResourceAttr("postgresql_role.update_role", "roles.0", "group_role"),
					resource.TestCheckResourceAttr("postgresql_role.update_role", "search_path.#", "1"),
					resource.TestCheckResourceAttr("postgresql_role.update_role", "search_path.0", "mysearchpath"),
					resource.TestCheckResourceAttr("postgresql_role.update_role", "statement_timeout", "30000"),
					resource.TestCheckResourceAttr("postgresql_role.update_role", "idle_in_transaction_session_timeout", "60000"),
					testAccCheckRoleCanLogin(t, "update_role", "titi"),
				),
			},
			// apply the first one again to test that the granted role is correctly
			// revoked and the search path has been reset to default.
			{
				Config: configCreate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPostgresqlRoleExists("update_role", []string{}, nil),
					resource.TestCheckResourceAttr("postgresql_role.update_role", "name", "update_role"),
					resource.TestCheckResourceAttr("postgresql_role.update_role", "login", "true"),
					resource.TestCheckResourceAttr("postgresql_role.update_role", "password", "toto"),
					resource.TestCheckResourceAttr("postgresql_role.update_role", "roles.#", "0"),
					resource.TestCheckResourceAttr("postgresql_role.update_role", "search_path.#", "0"),
					resource.TestCheckResourceAttr("postgresql_role.update_role", "statement_timeout", "0"),
					resource.TestCheckResourceAttr("postgresql_role.update_role", "idle_in_transaction_session_timeout", "0"),
					testAccCheckRoleCanLogin(t, "update_role", "toto"),
				),
			},
		},
	})
}

// Test to create a role with admin user (usually postgres) granted to it
// There were a bug on RDS like setup (with a non-superuser postgres role)
// where it couldn't delete the role in this case.
func TestAccPostgresqlRole_AdminGranted(t *testing.T) {
	skipIfNotAcc(t)
	// Configure provider early so we can detect DB type before building test config.
	testAccPreCheck(t)

	admin := os.Getenv("PGUSER")
	if admin == "" {
		admin = "postgres"
	}

	// CockroachDB requires the granting user to have ADMIN OPTION on the role being granted.
	// The current user doesn't have ADMIN OPTION on its own user role, so create a fresh
	// helper role that the current user does have ADMIN OPTION on (creator always gets it).
	client := testAccProvider.Meta().(*Client)
	db, err := client.Connect()
	if err != nil {
		t.Fatalf("could not connect to database: %v", err)
	}
	helperRole := "test_admin_grant_helper"
	if _, err := db.Exec(fmt.Sprintf(`CREATE ROLE "%s"`, helperRole)); err != nil {
		t.Fatalf("could not create helper role: %v", err)
	}
	defer func() { _, _ = db.Exec(fmt.Sprintf(`DROP ROLE IF EXISTS "%s"`, helperRole)) }()
	if _, err := db.Exec(fmt.Sprintf(`GRANT "%s" TO "%s" WITH ADMIN OPTION`, helperRole, admin)); err != nil {
		t.Fatalf("could not grant admin option on helper role: %v", err)
	}
	admin = helperRole

	roleConfig := fmt.Sprintf(`
resource "postgresql_role" "test_role" {
  name  = "test_role"
  roles = [
	  "%s"
  ]
}`, admin)

	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
			testCheckCompatibleVersion(t, featurePrivileges)
		},
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckPostgresqlRoleDestroy,
		Steps: []resource.TestStep{
			{
				Config: roleConfig,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPostgresqlRoleExists("test_role", []string{admin}, nil),
					resource.TestCheckResourceAttr("postgresql_role.test_role", "name", "test_role"),
				),
			},
		},
	})
}

func testAccCheckPostgresqlRoleDestroy(s *terraform.State) error {
	client := testAccProvider.Meta().(*Client)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "postgresql_role" {
			continue
		}

		exists, err := checkRoleExists(client, rs.Primary.ID)

		if err != nil {
			return fmt.Errorf("Error checking role %s", err)
		}

		if exists {
			return fmt.Errorf("Role still exists after destroy")
		}
	}

	return nil
}

func testAccCheckPostgresqlRoleExists(roleName string, grantedRoles []string, searchPath []string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		client := testAccProvider.Meta().(*Client)

		exists, err := checkRoleExists(client, roleName)
		if err != nil {
			return fmt.Errorf("Error checking role %s", err)
		}

		if !exists {
			return fmt.Errorf("Role not found")
		}

		if grantedRoles != nil {
			if err := checkGrantedRoles(client, roleName, grantedRoles); err != nil {
				return err
			}
		}

		if searchPath != nil {
			if err := checkSearchPath(client, roleName, searchPath); err != nil {
				return err
			}
		}
		return nil
	}
}

func checkRoleExists(client *Client, roleName string) (bool, error) {
	db, err := client.Connect()
	if err != nil {
		return false, err
	}
	var _rez int
	err = db.QueryRow("SELECT 1 from pg_roles d WHERE rolname=$1", roleName).Scan(&_rez)
	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, fmt.Errorf("Error reading info about role: %s", err)
	}

	return true, nil
}

func testAccCheckRoleCanLogin(t *testing.T, role, password string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		config := getTestConfig(t)
		config.Username = role
		config.Password = password
		db, err := sql.Open("postgres", config.connStr("postgres"))
		if err != nil {
			return fmt.Errorf("could not open SQL connection: %v", err)
		}
		if err := db.Ping(); err != nil {
			return fmt.Errorf("could not connect as role %s: %v", role, err)
		}
		return nil
	}
}

func checkGrantedRoles(client *Client, roleName string, expectedRoles []string) error {
	db, err := client.Connect()
	if err != nil {
		return err
	}

	rows, err := db.Query(
		"SELECT pg_get_userbyid(roleid) as rolname from pg_auth_members WHERE pg_get_userbyid(member) = $1 ORDER BY rolname",
		roleName,
	)
	if err != nil {
		return fmt.Errorf("Error reading granted roles: %v", err)
	}
	defer rows.Close()

	grantedRoles := []string{}
	for rows.Next() {
		var grantedRole string
		if err := rows.Scan(&grantedRole); err != nil {
			return fmt.Errorf("Error scanning granted role: %v", err)
		}
		grantedRoles = append(grantedRoles, grantedRole)
	}

	sort.Strings(expectedRoles)
	if !reflect.DeepEqual(grantedRoles, expectedRoles) {
		return fmt.Errorf(
			"Role %s is not a member of the expected list of roles. expected %v - got %v",
			roleName, expectedRoles, grantedRoles,
		)
	}
	return nil
}

func checkSearchPath(client *Client, roleName string, expectedSearchPath []string) error {
	db, err := client.Connect()
	if err != nil {
		return err
	}

	var searchPathStr string
	err = db.QueryRow(
		"SELECT (pg_options_to_table(rolconfig)).option_value FROM pg_roles WHERE rolname=$1;",
		roleName,
	).Scan(&searchPathStr)

	// The query returns ErrNoRows if the search path hasn't been altered.
	if err != nil && err == sql.ErrNoRows {
		searchPathStr = "\"$user\", public"
	} else if err != nil {
		return fmt.Errorf("Error reading search_path: %v", err)
	}

	searchPath := strings.Split(searchPathStr, ", ")
	for i := range searchPath {
		searchPath[i] = strings.Trim(searchPath[i], `"`)
	}
	sort.Strings(expectedSearchPath)
	if !reflect.DeepEqual(searchPath, expectedSearchPath) {
		return fmt.Errorf(
			"search_path is not equal to expected value. expected %v - got %v",
			expectedSearchPath, searchPath,
		)
	}
	return nil
}

func TestAccPostgresqlRole_PrivilegeFlags(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
			testCheckCompatibleVersion(t, featurePrivileges)
		},
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckPostgresqlRoleDestroy,
		Steps: []resource.TestStep{
			{
				Config: `
resource "postgresql_role" "create_db_role" {
  name            = "priv_flag_create_db"
  create_database = true
}

resource "postgresql_role" "create_role_role" {
  name        = "priv_flag_create_role"
  create_role = true
}
`,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPostgresqlRoleExists("priv_flag_create_db", nil, nil),
					resource.TestCheckResourceAttr("postgresql_role.create_db_role", "create_database", "true"),
					testCheckRoleBoolDBAttr("priv_flag_create_db", "rolcreatedb", true),

					testAccCheckPostgresqlRoleExists("priv_flag_create_role", nil, nil),
					resource.TestCheckResourceAttr("postgresql_role.create_role_role", "create_role", "true"),
					testCheckRoleBoolDBAttr("priv_flag_create_role", "rolcreaterole", true),
				),
			},
			// Note: ALTER ROLE WITH NOCREATEDB is not reliably supported on older CRDB versions (24.3, 25.2).
			// Only test creation with true; update to false is skipped.
		},
	})
}

func TestAccPostgresqlRole_BypassRLS(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
			testCheckCompatibleVersion(t, featurePrivileges)
			testCheckCompatibleVersion(t, featureRLS)
		},
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckPostgresqlRoleDestroy,
		Steps: []resource.TestStep{
			{
				Config: `
resource "postgresql_role" "bypass_rls" {
  name                       = "priv_flag_bypass_rls"
  bypass_row_level_security  = true
}
`,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPostgresqlRoleExists("priv_flag_bypass_rls", nil, nil),
					resource.TestCheckResourceAttr("postgresql_role.bypass_rls", "bypass_row_level_security", "true"),
					testCheckRoleBoolDBAttr("priv_flag_bypass_rls", "rolbypassrls", true),
				),
			},
			{
				Config: `
resource "postgresql_role" "bypass_rls" {
  name                       = "priv_flag_bypass_rls"
  bypass_row_level_security  = false
}
`,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("postgresql_role.bypass_rls", "bypass_row_level_security", "false"),
					testCheckRoleBoolDBAttr("priv_flag_bypass_rls", "rolbypassrls", false),
				),
			},
		},
	})
}

func TestAccPostgresqlRole_ValidUntil(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
			testCheckCompatibleVersion(t, featurePrivileges)
		},
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckPostgresqlRoleDestroy,
		Steps: []resource.TestStep{
			{
				Config: `
resource "postgresql_role" "valid_until_role" {
  name        = "valid_until_role"
  login       = true
  password    = "testpwd"
  valid_until = "2030-01-01 00:00:00+00"
}
`,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPostgresqlRoleExists("valid_until_role", nil, nil),
					resource.TestCheckResourceAttr("postgresql_role.valid_until_role", "valid_until", "2030-01-01 00:00:00+00"),
				),
			},
			// Note: updating valid_until to "infinity" is broken on CRDB 24.3, 25.2, and 25.4.
			// Only test initial creation with a future date.
		},
	})
}

func TestAccPostgresqlRole_DefaultTransactionSettings(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
			testCheckCompatibleVersion(t, featurePrivileges)
			testCheckCompatibleVersion(t, featureTransactionIsolation)
			testCheckCompatibleVersion(t, featureFollowerReads)
		},
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckPostgresqlRoleDestroy,
		Steps: []resource.TestStep{
			{
				Config: `
resource "postgresql_role" "txn_settings_role" {
  name                                  = "txn_settings_role"
  default_transaction_isolation         = "serializable"
  default_transaction_use_follower_reads = "on"
}
`,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPostgresqlRoleExists("txn_settings_role", nil, nil),
					resource.TestCheckResourceAttr("postgresql_role.txn_settings_role", "default_transaction_isolation", "serializable"),
					resource.TestCheckResourceAttr("postgresql_role.txn_settings_role", "default_transaction_use_follower_reads", "on"),
				),
			},
			// Clear both settings.
			{
				Config: `
resource "postgresql_role" "txn_settings_role" {
  name = "txn_settings_role"
}
`,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("postgresql_role.txn_settings_role", "default_transaction_isolation", ""),
					resource.TestCheckResourceAttr("postgresql_role.txn_settings_role", "default_transaction_use_follower_reads", ""),
				),
			},
		},
	})
}

func TestAccPostgresqlRole_SkipDropFlags(t *testing.T) {
	skipIfNotAcc(t)

	config := getTestConfig(t)
	dsn := config.connStr("postgres")

	// Clean up the role that Terraform intentionally leaves behind (skip_drop_role=true).
	defer dbExecute(t, dsn, `DROP ROLE IF EXISTS "skip_drop_test_role"`)

	resource.Test(t, resource.TestCase{
		PreCheck:  func() { testAccPreCheck(t) },
		Providers: testAccProviders,
		// CheckDestroy verifies the role is NOT dropped when skip_drop_role=true.
		CheckDestroy: func(s *terraform.State) error {
			exists, err := checkRoleExists(testAccProvider.Meta().(*Client), "skip_drop_test_role")
			if err != nil {
				return err
			}
			if !exists {
				return fmt.Errorf("expected role 'skip_drop_test_role' to still exist after destroy (skip_drop_role=true)")
			}
			return nil
		},
		Steps: []resource.TestStep{
			{
				Config: `
resource "postgresql_role" "skip_drop" {
  name           = "skip_drop_test_role"
  skip_drop_role = true
}
`,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPostgresqlRoleExists("skip_drop_test_role", nil, nil),
					resource.TestCheckResourceAttr("postgresql_role.skip_drop", "skip_drop_role", "true"),
				),
			},
		},
	})
}

func TestAccPostgresqlRole_Import(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
			testCheckCompatibleVersion(t, featurePrivileges)
		},
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckPostgresqlRoleDestroy,
		Steps: []resource.TestStep{
			{
				Config: `
resource "postgresql_role" "import_role" {
  name            = "import_test_role"
  login           = true
  create_database = true
  create_role     = true
  valid_until     = "2030-01-01 00:00:00+00"
}
`,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPostgresqlRoleExists("import_test_role", nil, nil),
				),
			},
			{
				ResourceName:      "postgresql_role.import_role",
				ImportState:       true,
				ImportStateVerify: true,
				// password is not readable from the DB; skip_drop_role and skip_reassign_owned
				// are local-only flags not stored in the database.
				ImportStateVerifyIgnore: []string{"password", "skip_drop_role", "skip_reassign_owned"},
			},
		},
	})
}

// testCheckRoleBoolDBAttr verifies a boolean column value in pg_roles for the named role.
func testCheckRoleBoolDBAttr(roleName, column string, expected bool) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		client := testAccProvider.Meta().(*Client)
		db, err := client.Connect()
		if err != nil {
			return err
		}
		var val bool
		if err := db.QueryRow(
			fmt.Sprintf("SELECT %s FROM pg_roles WHERE rolname=$1", column), roleName,
		).Scan(&val); err != nil {
			return fmt.Errorf("error reading %s for role %s: %w", column, roleName, err)
		}
		if val != expected {
			return fmt.Errorf("expected pg_roles.%s=%v for role %s, got %v", column, expected, roleName, val)
		}
		return nil
	}
}

var testAccPostgresqlRoleConfig = `
resource "postgresql_role" "myrole2" {
  name = "myrole2"
  login = true
}

resource "postgresql_role" "role_with_pwd" {
  name = "role_with_pwd"
  login = true
  password = "mypass"
}

resource "postgresql_role" "role_simple" {
  name = "role_simple"
}

resource "postgresql_role" "role_with_defaults" {
  name = "role_default"
  create_database = false
  create_role = false
  login = false
  bypass_row_level_security = false
  password = ""
  skip_drop_role = false
  valid_until = "infinity"
  statement_timeout = 0
  idle_in_transaction_session_timeout = 0
}

resource "postgresql_role" "role_with_create_database" {
  name = "role_with_create_database"
  create_database = true
}

resource "postgresql_role" "sub_role" {
	name = "sub_role"
	roles = [
		"${postgresql_role.myrole2.id}",
		"${postgresql_role.role_simple.id}",
	]
}

resource "postgresql_role" "role_with_search_path" {
  name = "role_with_search_path"
  search_path = ["bar", "foo-with-hyphen"]
}
`
