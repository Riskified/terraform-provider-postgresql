package postgresql

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func TestAccPostgresqlFunction_Basic(t *testing.T) {
	config := `
resource "postgresql_function" "basic_function" {
    name = "basic_function"
    returns = "integer"
    language = "plpgsql"
    body = <<-EOF
        BEGIN
            RETURN 1;
        END;
    EOF
}
`

	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
			testCheckCompatibleVersion(t, featureFunction)
		},
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckPostgresqlFunctionDestroy,
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPostgresqlFunctionExists("postgresql_function.basic_function", ""),
					resource.TestCheckResourceAttr(
						"postgresql_function.basic_function", "name", "basic_function"),
					resource.TestCheckResourceAttr(
						"postgresql_function.basic_function", "schema", "public"),
					resource.TestCheckResourceAttr(
						"postgresql_function.basic_function", "language", "plpgsql"),
				),
			},
		},
	})
}

func TestAccPostgresqlFunction_SpecificDatabase(t *testing.T) {
	skipIfNotAcc(t)

	dbSuffix, teardown := setupTestDatabase(t, true, true)
	defer teardown()

	dbName, _ := getTestDBNames(dbSuffix)

	config := `
resource "postgresql_function" "basic_function" {
    name = "basic_function"
    database = "%s"
    returns = "integer"
    language = "plpgsql"
    body = <<-EOF
        BEGIN
            RETURN 1;
        END;
    EOF
}
`

	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
			testCheckCompatibleVersion(t, featureFunction)
		},
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckPostgresqlFunctionDestroy,
		Steps: []resource.TestStep{
			{
				Config: fmt.Sprintf(config, dbName),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPostgresqlFunctionExists("postgresql_function.basic_function", dbName),
					resource.TestCheckResourceAttr(
						"postgresql_function.basic_function", "name", "basic_function"),
					resource.TestCheckResourceAttr(
						"postgresql_function.basic_function", "database", dbName),
					resource.TestCheckResourceAttr(
						"postgresql_function.basic_function", "schema", "public"),
					resource.TestCheckResourceAttr(
						"postgresql_function.basic_function", "language", "plpgsql"),
				),
			},
		},
	})
}

func TestAccPostgresqlFunction_Update(t *testing.T) {
	configCreate := `
resource "postgresql_function" "func" {
    name = "func"
    returns = "integer"
    language = "plpgsql"
    body = <<-EOF
        BEGIN
            RETURN 1;
        END;
    EOF
}
`

	configUpdate := `
resource "postgresql_function" "func" {
    name = "func"
    returns = "integer"
    language = "plpgsql"
    volatility = "IMMUTABLE"
    body = <<-EOF
        BEGIN
            RETURN 2;
        END;
    EOF
}
`
	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
			testCheckCompatibleVersion(t, featureFunction)
		},
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckPostgresqlFunctionDestroy,
		Steps: []resource.TestStep{
			{
				Config: configCreate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPostgresqlFunctionExists("postgresql_function.func", ""),
					resource.TestCheckResourceAttr(
						"postgresql_function.func", "name", "func"),
					resource.TestCheckResourceAttr(
						"postgresql_function.func", "schema", "public"),
					resource.TestCheckResourceAttr(
						"postgresql_function.func", "volatility", "VOLATILE"),
				),
			},
			{
				Config: configUpdate,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPostgresqlFunctionExists("postgresql_function.func", ""),
					resource.TestCheckResourceAttr(
						"postgresql_function.func", "name", "func"),
					resource.TestCheckResourceAttr(
						"postgresql_function.func", "schema", "public"),
					resource.TestCheckResourceAttr(
						"postgresql_function.func", "volatility", "IMMUTABLE"),
				),
			},
		},
	})
}

func TestAccPostgresqlFunction_WithArgs(t *testing.T) {
	configCreate := `
resource "postgresql_function" "func_with_args" {
  name    = "func_with_args"
  returns = "STRING"
  language = "plpgsql"
  arg {
    type = "STRING"
    name = "in_text"
  }
  arg {
    type = "INT8"
    name = "in_int"
  }
  body = <<-EOF
    BEGIN
      RETURN in_text || ':' || in_int::text;
    END;
  EOF
}
`

	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
			testCheckCompatibleVersion(t, featureFunction)
		},
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckPostgresqlFunctionDestroy,
		Steps: []resource.TestStep{
			{
				Config: configCreate,
				// CRDB normalizes the function body (type casts, operator parentheses), causing plan drift.
				ExpectNonEmptyPlan: true,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPostgresqlFunctionExists("postgresql_function.func_with_args", ""),
					resource.TestCheckResourceAttr("postgresql_function.func_with_args", "name", "func_with_args"),
					resource.TestCheckResourceAttr("postgresql_function.func_with_args", "arg.#", "2"),
					resource.TestCheckResourceAttr("postgresql_function.func_with_args", "arg.0.type", "STRING"),
					resource.TestCheckResourceAttr("postgresql_function.func_with_args", "arg.0.name", "in_text"),
					resource.TestCheckResourceAttr("postgresql_function.func_with_args", "arg.1.type", "INT8"),
					resource.TestCheckResourceAttr("postgresql_function.func_with_args", "arg.1.name", "in_int"),
				),
			},
		},
	})
}

func TestAccPostgresqlFunction_SecurityDefiner(t *testing.T) {
	config := `
resource "postgresql_function" "sec_def_func" {
  name             = "sec_def_func"
  returns          = "integer"
  language         = "plpgsql"
  security_definer = true
  body = <<-EOF
    BEGIN
      RETURN 42;
    END;
  EOF
}
`

	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
			testCheckCompatibleVersion(t, featureFunction)
		},
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckPostgresqlFunctionDestroy,
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPostgresqlFunctionExists("postgresql_function.sec_def_func", ""),
					resource.TestCheckResourceAttr("postgresql_function.sec_def_func", "security_definer", "true"),
					// Note: pg_proc.prosecdef is not set by CRDB, so we only check the Terraform state attribute.
				),
			},
		},
	})
}

func TestAccPostgresqlFunction_Strict(t *testing.T) {
	config := `
resource "postgresql_function" "strict_func" {
  name     = "strict_func"
  returns  = "integer"
  language = "plpgsql"
  strict   = true
  body = <<-EOF
    BEGIN
      RETURN 42;
    END;
  EOF
}
`

	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
			testCheckCompatibleVersion(t, featureFunction)
		},
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckPostgresqlFunctionDestroy,
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPostgresqlFunctionExists("postgresql_function.strict_func", ""),
					resource.TestCheckResourceAttr("postgresql_function.strict_func", "strict", "true"),
					testCheckFuncProcBoolAttr("strict_func", "proisstrict", true),
				),
			},
		},
	})
}

func TestAccPostgresqlFunction_Import(t *testing.T) {
	config := `
resource "postgresql_function" "import_func" {
  name    = "import_func"
  returns = "integer"
  language = "plpgsql"
  body = <<-EOF
    BEGIN
      RETURN 1;
    END;
  EOF
}
`

	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
			testCheckCompatibleVersion(t, featureFunction)
		},
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckPostgresqlFunctionDestroy,
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPostgresqlFunctionExists("postgresql_function.import_func", ""),
				),
			},
			{
				ResourceName:      "postgresql_function.import_func",
				ImportState:       true,
				ImportStateVerify: true,
				// body may have whitespace differences; drop_cascade is local-only.
				ImportStateVerifyIgnore: []string{"body", "drop_cascade"},
			},
		},
	})
}

// testCheckFuncProcBoolAttr verifies a boolean column in pg_proc for the named function in public schema.
func testCheckFuncProcBoolAttr(funcName, column string, expected bool) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		db, err := testAccProvider.Meta().(*Client).Connect()
		if err != nil {
			return err
		}

		var val bool
		if err := db.QueryRow(
			fmt.Sprintf("SELECT %s FROM pg_proc WHERE proname=$1 AND pronamespace = 'public'::regnamespace", column),
			funcName,
		).Scan(&val); err != nil {
			return fmt.Errorf("error reading pg_proc.%s for function %s: %w", column, funcName, err)
		}
		if val != expected {
			return fmt.Errorf("expected pg_proc.%s=%v for function %s, got %v", column, expected, funcName, val)
		}
		return nil
	}
}

func testAccCheckPostgresqlFunctionExists(n string, database string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs, ok := s.RootModule().Resources[n]
		if !ok {
			return fmt.Errorf("Resource not found: %s", n)
		}

		if rs.Primary.ID == "" {
			return fmt.Errorf("No ID is set")
		}

		signature := rs.Primary.ID

		baseDB, err := testAccProvider.Meta().(*Client).Connect()
		if err != nil {
			return err
		}
		db, err := connectToDatabase(baseDB, database)
		if err != nil {
			return err
		}

		exists, err := checkFunctionExists(db, signature)

		if err != nil {
			return fmt.Errorf("Error checking function %s", err)
		}

		if !exists {
			return fmt.Errorf("Function not found")
		}

		return nil
	}
}

func testAccCheckPostgresqlFunctionDestroy(s *terraform.State) error {
	client := testAccProvider.Meta().(*Client)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "postgresql_function" {
			continue
		}

		db, err := client.Connect()
		if err != nil {
			return err
		}

		_, functionSignature, expandErr := expandFunctionID(rs.Primary.ID, nil, nil)

		if expandErr != nil {
			return fmt.Errorf("Incorrect resource Id %s", err)
		}

		exists, err := checkFunctionExists(db, functionSignature)

		if err != nil {
			return fmt.Errorf("Error checking function %s", err)
		}

		if exists {
			return fmt.Errorf("Function still exists after destroy")
		}
	}

	return nil
}

func checkFunctionExists(db QueryAble, signature string) (bool, error) {
	var _rez bool
	err := db.QueryRow(fmt.Sprintf("SELECT to_regprocedure('%s') IS NOT NULL", signature)).Scan(&_rez)
	switch {
	case err == sql.ErrNoRows:
		return false, nil
	case err != nil:
		return false, fmt.Errorf("Error reading info about function: %s", err)
	}

	return _rez, nil
}
