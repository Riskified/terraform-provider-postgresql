package postgresql

import (
	"fmt"
	"os"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

func testAccCheckCockroachDBExternalConnectionDestroy(s *terraform.State) error {
	client := testAccProvider.Meta().(*Client)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "postgresql_crdb_external_connection" {
			continue
		}

		txn, err := startTransaction(client, "")
		if err != nil {
			return err
		}
		defer deferredRollback(txn)

		exists, err := connExists(txn, rs.Primary.ID)
		if err != nil {
			return fmt.Errorf("error checking external connection %s: %w", rs.Primary.ID, err)
		}

		if exists {
			return fmt.Errorf("external connection %s still exists after destroy", rs.Primary.ID)
		}
	}

	return nil
}

func testAccCheckCockroachDBExternalConnectionExists(n string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs, ok := s.RootModule().Resources[n]
		if !ok {
			return fmt.Errorf("Resource not found: %s", n)
		}

		if rs.Primary.ID == "" {
			return fmt.Errorf("No ID is set")
		}

		client := testAccProvider.Meta().(*Client)
		txn, err := startTransaction(client, "")
		if err != nil {
			return err
		}
		defer deferredRollback(txn)

		exists, err := connExists(txn, rs.Primary.ID)
		if err != nil {
			return fmt.Errorf("error checking external connection: %w", err)
		}

		if !exists {
			return fmt.Errorf("external connection not found")
		}

		return nil
	}
}

func TestAccCockroachDBExternalConnection_Basic(t *testing.T) {
	skipIfNotAcc(t)

	connURL := os.Getenv("CRDB_TEST_EXTERNAL_CONN_URL")
	if connURL == "" {
		t.Skip("CRDB_TEST_EXTERNAL_CONN_URL must be set for external connection acceptance tests")
	}

	config := fmt.Sprintf(`
resource "postgresql_crdb_external_connection" "test" {
  connection_name = "test-external-conn"
  connection_url  = "%s"
}
`, connURL)

	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
			skipIfNotCockroachDB(t)
		},
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckCockroachDBExternalConnectionDestroy,
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckCockroachDBExternalConnectionExists("postgresql_crdb_external_connection.test"),
					resource.TestCheckResourceAttr(
						"postgresql_crdb_external_connection.test", "connection_name", "test-external-conn"),
					resource.TestCheckResourceAttr(
						"postgresql_crdb_external_connection.test", "connection_url", connURL),
				),
			},
		},
	})
}

func TestAccCockroachDBExternalConnection_MultipleConnections(t *testing.T) {
	skipIfNotAcc(t)

	connURL := os.Getenv("CRDB_TEST_EXTERNAL_CONN_URL")
	if connURL == "" {
		t.Skip("CRDB_TEST_EXTERNAL_CONN_URL must be set for external connection acceptance tests")
	}

	config := fmt.Sprintf(`
resource "postgresql_crdb_external_connection" "conn1" {
  connection_name = "test-multi-conn-1"
  connection_url  = "%s"
}

resource "postgresql_crdb_external_connection" "conn2" {
  connection_name = "test-multi-conn-2"
  connection_url  = "%s"
}
`, connURL, connURL)

	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
			skipIfNotCockroachDB(t)
		},
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckCockroachDBExternalConnectionDestroy,
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckCockroachDBExternalConnectionExists("postgresql_crdb_external_connection.conn1"),
					testAccCheckCockroachDBExternalConnectionExists("postgresql_crdb_external_connection.conn2"),
					resource.TestCheckResourceAttr(
						"postgresql_crdb_external_connection.conn1", "connection_name", "test-multi-conn-1"),
					resource.TestCheckResourceAttr(
						"postgresql_crdb_external_connection.conn2", "connection_name", "test-multi-conn-2"),
				),
			},
		},
	})
}
