package postgresql

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestAccPostgresqlDefaultPrivileges(t *testing.T) {
	skipIfNotAcc(t)

	// We have to create the database outside of resource.Test
	// because we need to create a table to assert that grant are correctly applied
	// and we don't have this resource yet
	dbSuffix, teardown := setupTestDatabase(t, true, true)
	defer teardown()

	config := getTestConfig(t)
	dbName, roleName := getTestDBNames(dbSuffix)

	// Set default privileges to the test role then to public (i.e.: everyone)
	for _, role := range []string{roleName, "public"} {
		t.Run(role, func(t *testing.T) {
			withGrant := true
			if role == "public" {
				withGrant = false
			}

			// We set PGUSER as owner as he will create the test table
			var tfConfig = fmt.Sprintf(`
resource "postgresql_default_privileges" "test_ro" {
	database    = "%s"
	owner       = "%s"
	role        = "%s"
	schema      = "test_schema"
	object_type = "table"
	with_grant_option = %t
	privileges   = %%s
}
	`, dbName, config.Username, role, withGrant)

			resource.Test(t, resource.TestCase{
				PreCheck: func() {
					testAccPreCheck(t)
					testCheckCompatibleVersion(t, featurePrivileges)
				},
				Providers: testAccProviders,
				Steps: []resource.TestStep{
					{
						Config: fmt.Sprintf(tfConfig, `[]`),
						Check: resource.ComposeTestCheckFunc(
							func(*terraform.State) error {
								tables := []string{"test_schema.test_table"}
								// To test default privileges, we need to create a table
								// after having apply the state.
								dropFunc := createTestTables(t, dbSuffix, tables, "")
								defer dropFunc()

								return testCheckTablesPrivileges(t, dbName, roleName, tables, []string{})
							},
							resource.TestCheckResourceAttr("postgresql_default_privileges.test_ro", "object_type", "table"),
							resource.TestCheckResourceAttr("postgresql_default_privileges.test_ro", "with_grant_option", fmt.Sprintf("%t", withGrant)),
							resource.TestCheckResourceAttr("postgresql_default_privileges.test_ro", "privileges.#", "0"),
						),
					},
					{
						Config: fmt.Sprintf(tfConfig, `["SELECT"]`),
						Check: resource.ComposeTestCheckFunc(
							func(*terraform.State) error {
								tables := []string{"test_schema.test_table"}
								// To test default privileges, we need to create a table
								// after having apply the state.
								dropFunc := createTestTables(t, dbSuffix, tables, "")
								defer dropFunc()

								return testCheckTablesPrivileges(t, dbName, roleName, tables, []string{"SELECT"})
							},
							resource.TestCheckResourceAttr("postgresql_default_privileges.test_ro", "object_type", "table"),
							resource.TestCheckResourceAttr("postgresql_default_privileges.test_ro", "with_grant_option", fmt.Sprintf("%t", withGrant)),
							resource.TestCheckResourceAttr("postgresql_default_privileges.test_ro", "privileges.#", "1"),
							resource.TestCheckResourceAttr("postgresql_default_privileges.test_ro", "privileges.0", "SELECT"),
						),
					},
					{
						Config: fmt.Sprintf(tfConfig, `["SELECT", "UPDATE"]`),
						Check: resource.ComposeTestCheckFunc(
							func(*terraform.State) error {
								tables := []string{"test_schema.test_table"}
								// To test default privileges, we need to create a table
								// after having apply the state.
								dropFunc := createTestTables(t, dbSuffix, tables, "")
								defer dropFunc()

								return testCheckTablesPrivileges(t, dbName, roleName, tables, []string{"SELECT", "UPDATE"})
							},
							resource.TestCheckResourceAttr("postgresql_default_privileges.test_ro", "privileges.#", "2"),
							resource.TestCheckResourceAttr("postgresql_default_privileges.test_ro", "privileges.0", "SELECT"),
							resource.TestCheckResourceAttr("postgresql_default_privileges.test_ro", "privileges.1", "UPDATE"),
						),
					},
					{
						Config: fmt.Sprintf(tfConfig, `[]`),
						Check: resource.ComposeTestCheckFunc(
							func(*terraform.State) error {
								tables := []string{"test_schema.test_table"}
								// To test default privileges, we need to create a table
								// after having apply the state.
								dropFunc := createTestTables(t, dbSuffix, tables, "")
								defer dropFunc()

								return testCheckTablesPrivileges(t, dbName, roleName, tables, []string{})
							},
							resource.TestCheckResourceAttr("postgresql_default_privileges.test_ro", "object_type", "table"),
							resource.TestCheckResourceAttr("postgresql_default_privileges.test_ro", "with_grant_option", fmt.Sprintf("%t", withGrant)),
							resource.TestCheckResourceAttr("postgresql_default_privileges.test_ro", "privileges.#", "0"),
						),
					},
				},
			})
		})
	}
}

// Test the case where we need to grant the owner to the connected user.
// The owner should be revoked
func TestAccPostgresqlDefaultPrivileges_GrantOwner(t *testing.T) {
	skipIfNotAcc(t)

	// We have to create the database outside of resource.Test
	// because we need to create a table to assert that grant are correctly applied
	// and we don't have this resource yet
	dbSuffix, teardown := setupTestDatabase(t, true, true)
	defer teardown()

	config := getTestConfig(t)
	dsn := config.connStr("postgres")
	dbName, roleName := getTestDBNames(dbSuffix)

	// We set PGUSER as owner as he will create the test table
	var stateConfig = fmt.Sprintf(`

resource postgresql_role "test_owner" {
    name = "test_owner"
}

// From PostgreSQL 15, schema public is not wild open anymore
resource "postgresql_grant" "public_usage" {
	database          = "%s"
	schema            = "public"
	role              = postgresql_role.test_owner.name
	object_type       = "schema"
	privileges        = ["CREATE", "USAGE"]
}

resource "postgresql_default_privileges" "test_ro" {
	database    = "%s"
	owner       = postgresql_role.test_owner.name
	role        = "%s"
	schema      = "public"
	object_type = "table"
	privileges  = ["SELECT"]
}
	`, dbName, dbName, roleName)

	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
			testCheckCompatibleVersion(t, featurePrivileges)
		},
		Providers: testAccProviders,
		Steps: []resource.TestStep{
			{
				Config: stateConfig,
				Check: resource.ComposeTestCheckFunc(
					func(*terraform.State) error {
						tables := []string{"public.test_table"}
						// To test default privileges, we need to create a table
						// after having apply the state.
						dropFunc := createTestTables(t, dbSuffix, tables, "test_owner")
						defer dropFunc()

						return testCheckTablesPrivileges(t, dbName, roleName, tables, []string{"SELECT"})
					},
					resource.TestCheckResourceAttr("postgresql_default_privileges.test_ro", "object_type", "table"),
					resource.TestCheckResourceAttr("postgresql_default_privileges.test_ro", "privileges.#", "1"),
					resource.TestCheckResourceAttr("postgresql_default_privileges.test_ro", "privileges.0", "SELECT"),

					// check if connected user does not have test_owner granted anymore.
					checkUserMembership(t, dsn, config.Username, "test_owner", false),
				),
			},
		},
	})
}

// Test the case where we define default priviliges without specifying a schema. These
// priviliges should apply to newly created resources for the named role in all schema.
func TestAccPostgresqlDefaultPrivileges_NoSchema(t *testing.T) {
	skipIfNotAcc(t)

	// We have to create the database outside of resource.Test
	// because we need to create a table to assert that grant are correctly applied
	// and we don't have this resource yet
	dbSuffix, teardown := setupTestDatabase(t, true, true)
	defer teardown()

	config := getTestConfig(t)
	dbName, roleName := getTestDBNames(dbSuffix)

	// Set default privileges to the test role then to public (i.e.: everyone)
	for _, role := range []string{roleName, "public"} {
		t.Run(role, func(t *testing.T) {

			hclText := `
resource "postgresql_default_privileges" "test_ro" {
	database    = "%s"
	owner       = "%s"
	role        = "%s"
	object_type = "table"
	privileges  = %%s
}
`
			// We set PGUSER as owner as he will create the test table
			var tfConfig = fmt.Sprintf(hclText, dbName, config.Username, role)

			resource.Test(t, resource.TestCase{
				PreCheck: func() {
					testAccPreCheck(t)
					testCheckCompatibleVersion(t, featurePrivileges)
				},
				Providers: testAccProviders,
				Steps: []resource.TestStep{
					{
						Config: fmt.Sprintf(tfConfig, `["SELECT"]`),
						Check: resource.ComposeTestCheckFunc(
							func(*terraform.State) error {
								tables := []string{"test_schema.test_table", "dev_schema.test_table"}
								// To test default privileges, we need to create tables
								// in both dev and test schema after having applied the state.
								dropFunc := createTestTables(t, dbSuffix, tables, "")
								defer dropFunc()

								return testCheckTablesPrivileges(t, dbName, roleName, tables, []string{"SELECT"})
							},
							resource.TestCheckResourceAttr("postgresql_default_privileges.test_ro", "object_type", "table"),
							resource.TestCheckResourceAttr("postgresql_default_privileges.test_ro", "privileges.#", "1"),
							resource.TestCheckResourceAttr("postgresql_default_privileges.test_ro", "privileges.0", "SELECT"),
						),
					},
					{
						Config: fmt.Sprintf(tfConfig, `["SELECT", "UPDATE"]`),
						Check: resource.ComposeTestCheckFunc(
							func(*terraform.State) error {
								tables := []string{"test_schema.test_table", "dev_schema.test_table"}
								// To test default privileges, we need to create tables
								// in both dev and test schema after having applied the state.
								dropFunc := createTestTables(t, dbSuffix, tables, "")
								defer dropFunc()

								return testCheckTablesPrivileges(t, dbName, roleName, tables, []string{"SELECT", "UPDATE"})
							},
							resource.TestCheckResourceAttr("postgresql_default_privileges.test_ro", "privileges.#", "2"),
							resource.TestCheckResourceAttr("postgresql_default_privileges.test_ro", "privileges.0", "SELECT"),
							resource.TestCheckResourceAttr("postgresql_default_privileges.test_ro", "privileges.1", "UPDATE"),
						),
					},
				},
			})
		})
	}
}

func TestAccPostgresqlDefaultPrivileges_Sequence(t *testing.T) {
	skipIfNotAcc(t)

	dbSuffix, teardown := setupTestDatabase(t, true, true)
	defer teardown()

	config := getTestConfig(t)
	dbName, roleName := getTestDBNames(dbSuffix)

	tfConfig := fmt.Sprintf(`
resource "postgresql_default_privileges" "seq_priv" {
  database    = "%s"
  owner       = "%s"
  role        = "%s"
  schema      = "test_schema"
  object_type = "sequence"
  privileges  = ["USAGE"]
}
`, dbName, config.Username, roleName)

	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
			testCheckCompatibleVersion(t, featurePrivileges)
		},
		Providers: testAccProviders,
		Steps: []resource.TestStep{
			{
				Config: tfConfig,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("postgresql_default_privileges.seq_priv", "object_type", "sequence"),
					resource.TestCheckResourceAttr("postgresql_default_privileges.seq_priv", "privileges.#", "1"),
					resource.TestCheckResourceAttr("postgresql_default_privileges.seq_priv", "privileges.0", "USAGE"),
					func(*terraform.State) error {
						seqs := []string{"test_schema.test_seq"}
						dropFunc := createTestSequences(t, dbSuffix, seqs, "")
						defer dropFunc()
						return testCheckSequenceUsable(t, dbName, roleName, "test_schema.test_seq", true)
					},
				),
			},
		},
	})
}

func TestAccPostgresqlDefaultPrivileges_Function(t *testing.T) {
	skipIfNotAcc(t)

	dbSuffix, teardown := setupTestDatabase(t, true, true)
	defer teardown()

	config := getTestConfig(t)
	dbName, roleName := getTestDBNames(dbSuffix)

	tfConfig := fmt.Sprintf(`
resource "postgresql_default_privileges" "func_priv" {
  database    = "%s"
  owner       = "%s"
  role        = "%s"
  schema      = "test_schema"
  object_type = "function"
  privileges  = ["EXECUTE"]
}
`, dbName, config.Username, roleName)

	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
			testCheckCompatibleVersion(t, featurePrivileges)
			testCheckCompatibleVersion(t, featureFunction)
		},
		Providers: testAccProviders,
		Steps: []resource.TestStep{
			{
				Config: tfConfig,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("postgresql_default_privileges.func_priv", "object_type", "function"),
					resource.TestCheckResourceAttr("postgresql_default_privileges.func_priv", "privileges.#", "1"),
					resource.TestCheckResourceAttr("postgresql_default_privileges.func_priv", "privileges.0", "EXECUTE"),
					func(*terraform.State) error {
						cfg := getTestConfig(t)
						db, err := sql.Open("postgres", cfg.connStr(dbName))
						if err != nil {
							return fmt.Errorf("could not open connection: %w", err)
						}
						defer db.Close()
						if _, err := db.Exec(`
CREATE FUNCTION test_schema.dp_test_fn() RETURNS integer
  LANGUAGE sql AS $$ SELECT 1 $$;
`); err != nil {
							return fmt.Errorf("could not create test function: %w", err)
						}
						defer func() { _, _ = db.Exec("DROP FUNCTION IF EXISTS test_schema.dp_test_fn()") }()

						roleDB := connectAsTestRole(t, roleName, dbName)
						defer roleDB.Close()
						return testHasGrantForQuery(roleDB, "SELECT test_schema.dp_test_fn()", true)
					},
				),
			},
		},
	})
}

func TestAccPostgresqlDefaultPrivileges_Type(t *testing.T) {
	skipIfNotAcc(t)

	dbSuffix, teardown := setupTestDatabase(t, true, true)
	defer teardown()

	config := getTestConfig(t)
	dbName, roleName := getTestDBNames(dbSuffix)

	tfConfig := fmt.Sprintf(`
resource "postgresql_default_privileges" "type_priv" {
  database    = "%s"
  owner       = "%s"
  role        = "%s"
  schema      = "test_schema"
  object_type = "type"
  privileges  = ["USAGE"]
}
`, dbName, config.Username, roleName)

	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
			testCheckCompatibleVersion(t, featurePrivileges)
		},
		Providers: testAccProviders,
		Steps: []resource.TestStep{
			{
				Config: tfConfig,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("postgresql_default_privileges.type_priv", "object_type", "type"),
					resource.TestCheckResourceAttr("postgresql_default_privileges.type_priv", "privileges.#", "1"),
					resource.TestCheckResourceAttr("postgresql_default_privileges.type_priv", "privileges.0", "USAGE"),
					func(*terraform.State) error {
						cfg := getTestConfig(t)
						db, err := sql.Open("postgres", cfg.connStr(dbName))
						if err != nil {
							return fmt.Errorf("could not open connection: %w", err)
						}
						defer db.Close()
						if _, err := db.Exec(`CREATE TYPE test_schema.dp_status AS ENUM ('active', 'inactive')`); err != nil {
							return fmt.Errorf("could not create test type: %w", err)
						}
						defer func() { _, _ = db.Exec("DROP TYPE IF EXISTS test_schema.dp_status") }()

						// Verify the grantee can use the type via a simple cast (no CREATE privilege needed).
						roleDB := connectAsTestRole(t, roleName, dbName)
						defer roleDB.Close()
						err = testHasGrantForQuery(roleDB,
							"SELECT 'active'::test_schema.dp_status", true)
						if err != nil {
							return err
						}
						return nil
					},
				),
			},
		},
	})
}

// testCheckSequenceUsable verifies whether a role can call nextval on a sequence (USAGE privilege).
func testCheckSequenceUsable(t *testing.T, dbName, roleName, seqName string, expected bool) error {
	db := connectAsTestRole(t, roleName, dbName)
	defer db.Close()
	return testHasGrantForQuery(db, fmt.Sprintf("SELECT nextval('%s')", seqName), expected)
}

// Test defaults privileges on schemas
func TestAccPostgresqlDefaultPrivilegesOnSchemas(t *testing.T) {
	skipIfNotAcc(t)

	// We have to create the database outside of resource.Test
	// because we need to create schemas to assert that grant are correctly applied
	// and we don't have this resource yet
	dbSuffix, teardown := setupTestDatabase(t, true, true)
	defer teardown()

	config := getTestConfig(t)
	dbName, roleName := getTestDBNames(dbSuffix)

	// Set default privileges to the test role then to public (i.e.: everyone)
	for _, role := range []string{roleName, "public"} {
		t.Run(role, func(t *testing.T) {

			hclText := `
resource "postgresql_default_privileges" "test_ro" {
	database    = "%s"
	owner       = "%s"
	role        = "%s"
	object_type = "schema"
	privileges  = %%s
}
`
			// We set PGUSER as owner as he will create the test schemas
			var tfConfig = fmt.Sprintf(hclText, dbName, config.Username, role)

			resource.Test(t, resource.TestCase{
				PreCheck: func() {
					testAccPreCheck(t)
					testCheckCompatibleVersion(t, featurePrivileges)
					testCheckCompatibleVersion(t, featurePrivilegesOnSchemas)
				},
				Providers: testAccProviders,
				Steps: []resource.TestStep{
					{
						Config: fmt.Sprintf(tfConfig, `[]`),
						Check: resource.ComposeTestCheckFunc(
							func(*terraform.State) error {
								schemas := []string{"test_schema2", "dev_schema2"}
								// To test default privileges, we need to create a schema
								// after having apply the state.
								dropFunc := createTestSchemas(t, dbSuffix, schemas, "")
								defer dropFunc()

								return testCheckSchemasPrivileges(t, dbName, roleName, schemas, []string{})
							},
							resource.TestCheckResourceAttr("postgresql_default_privileges.test_ro", "object_type", "schema"),
							resource.TestCheckResourceAttr("postgresql_default_privileges.test_ro", "privileges.#", "0"),
						),
					},
					{
						Config: fmt.Sprintf(tfConfig, `["CREATE", "USAGE"]`),
						Check: resource.ComposeTestCheckFunc(
							func(*terraform.State) error {
								schemas := []string{"test_schema2", "dev_schema2"}
								// To test default privileges, we need to create a schema
								// after having apply the state.
								dropFunc := createTestSchemas(t, dbSuffix, schemas, "")
								defer dropFunc()

								return testCheckSchemasPrivileges(t, dbName, roleName, schemas, []string{"CREATE", "USAGE"})
							},
							resource.TestCheckResourceAttr(
								"postgresql_default_privileges.test_ro", "id", fmt.Sprintf("%s_%s_noschema_%s_schema", role, dbName, config.Username),
							),
							resource.TestCheckResourceAttr("postgresql_default_privileges.test_ro", "privileges.#", "2"),
							resource.TestCheckResourceAttr("postgresql_default_privileges.test_ro", "privileges.0", "CREATE"),
							resource.TestCheckResourceAttr("postgresql_default_privileges.test_ro", "privileges.1", "USAGE"),
						),
					},
				},
			})
		})
	}
}
