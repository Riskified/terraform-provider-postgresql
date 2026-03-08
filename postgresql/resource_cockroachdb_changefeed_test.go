package postgresql

import (
	"fmt"
	"os"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
)

// Unit tests for pure helper functions (no database connection required)

func TestFindTableChanges(t *testing.T) {
	tests := []struct {
		name             string
		currentTables    []string
		newTables        []string
		expectedToAdd    []string
		expectedToRemove []string
	}{
		{
			name:             "no changes",
			currentTables:    []string{"table1", "table2"},
			newTables:        []string{"table1", "table2"},
			expectedToAdd:    nil,
			expectedToRemove: nil,
		},
		{
			name:             "add one table",
			currentTables:    []string{"table1"},
			newTables:        []string{"table1", "table2"},
			expectedToAdd:    []string{"table2"},
			expectedToRemove: nil,
		},
		{
			name:             "remove one table",
			currentTables:    []string{"table1", "table2"},
			newTables:        []string{"table1"},
			expectedToAdd:    nil,
			expectedToRemove: []string{"table2"},
		},
		{
			name:             "add and remove",
			currentTables:    []string{"table1", "table2"},
			newTables:        []string{"table1", "table3"},
			expectedToAdd:    []string{"table3"},
			expectedToRemove: []string{"table2"},
		},
		{
			name:             "empty current list",
			currentTables:    []string{},
			newTables:        []string{"table1"},
			expectedToAdd:    []string{"table1"},
			expectedToRemove: nil,
		},
		{
			name:             "empty new list",
			currentTables:    []string{"table1"},
			newTables:        []string{},
			expectedToAdd:    nil,
			expectedToRemove: []string{"table1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			toAdd, toRemove := findTableChanges(tt.currentTables, tt.newTables)
			if !testStringSlicesEqual(toAdd, tt.expectedToAdd) {
				t.Errorf("findTableChanges() toAdd = %v, want %v", toAdd, tt.expectedToAdd)
			}
			if !testStringSlicesEqual(toRemove, tt.expectedToRemove) {
				t.Errorf("findTableChanges() toRemove = %v, want %v", toRemove, tt.expectedToRemove)
			}
		})
	}
}

func TestContains(t *testing.T) {
	tests := []struct {
		slice    []string
		item     string
		expected bool
	}{
		{[]string{"a", "b", "c"}, "a", true},
		{[]string{"a", "b", "c"}, "d", false},
		{[]string{}, "a", false},
		{[]string{"a"}, "a", true},
		{[]string{"foo", "bar"}, "baz", false},
	}

	for _, tt := range tests {
		result := contains(tt.slice, tt.item)
		if result != tt.expected {
			t.Errorf("contains(%v, %q) = %v, want %v", tt.slice, tt.item, result, tt.expected)
		}
	}
}

func TestExtractDetails(t *testing.T) {
	tests := []struct {
		name                string
		sql                 string
		expectedAvroPrefix  string
		expectedRegistry    string
		expectedInitialScan string
		expectedCursor      string
	}{
		{
			name:                "full description with cursor",
			sql:                 `CREATE CHANGEFEED FOR TABLE mytable INTO "external://kafka-conn" WITH initial_scan = 'no', updated, cursor='2023-01-01 00:00:00', diff, on_error='pause', format = avro, avro_schema_prefix='myprefix_', confluent_schema_registry = 'external://registry-conn'`,
			expectedAvroPrefix:  "myprefix_",
			expectedRegistry:    "registry-conn",
			expectedInitialScan: "no",
			expectedCursor:      "2023-01-01 00:00:00",
		},
		{
			name:                "initial_scan yes, no cursor",
			sql:                 `CREATE CHANGEFEED FOR TABLE mytable INTO "external://kafka-conn" WITH initial_scan = 'yes', updated, format = avro, avro_schema_prefix='test_', confluent_schema_registry = 'external://reg-conn'`,
			expectedAvroPrefix:  "test_",
			expectedRegistry:    "reg-conn",
			expectedInitialScan: "yes",
			expectedCursor:      "",
		},
		{
			name:                "empty sql",
			sql:                 "",
			expectedAvroPrefix:  "",
			expectedRegistry:    "",
			expectedInitialScan: "",
			expectedCursor:      "",
		},
		{
			name:                "prefix with underscore suffix",
			sql:                 `avro_schema_prefix='my_service_', confluent_schema_registry = 'external://my-registry'`,
			expectedAvroPrefix:  "my_service_",
			expectedRegistry:    "my-registry",
			expectedInitialScan: "",
			expectedCursor:      "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prefix, registry, initialScan, cursor := extractDetails(tt.sql)
			if prefix != tt.expectedAvroPrefix {
				t.Errorf("extractDetails() avroPrefix = %q, want %q", prefix, tt.expectedAvroPrefix)
			}
			if registry != tt.expectedRegistry {
				t.Errorf("extractDetails() registry = %q, want %q", registry, tt.expectedRegistry)
			}
			if initialScan != tt.expectedInitialScan {
				t.Errorf("extractDetails() initialScan = %q, want %q", initialScan, tt.expectedInitialScan)
			}
			if cursor != tt.expectedCursor {
				t.Errorf("extractDetails() cursor = %q, want %q", cursor, tt.expectedCursor)
			}
		})
	}
}

func TestValidateDateTime(t *testing.T) {
	validDates := []string{
		"2023-01-01 00:00:00",
		"2023-12-31 23:59:59",
		"2000-06-15 12:30:45",
	}
	for _, v := range validDates {
		_, errs := validateDateTime(v, "start_from")
		if len(errs) != 0 {
			t.Errorf("validateDateTime(%q) got unexpected errors: %v", v, errs)
		}
	}

	invalidDates := []string{
		"not-a-date",
		"2023/01/01",
		"2023-01-01",
		"01:00:00",
	}
	for _, v := range invalidDates {
		_, errs := validateDateTime(v, "start_from")
		if len(errs) == 0 {
			t.Errorf("validateDateTime(%q) expected errors but got none", v)
		}
	}
}

func TestInterface2StringList(t *testing.T) {
	tests := []struct {
		name     string
		input    []interface{}
		expected []string
	}{
		{
			name:     "basic list",
			input:    []interface{}{"a", "b", "c"},
			expected: []string{"a", "b", "c"},
		},
		{
			name:     "empty list",
			input:    []interface{}{},
			expected: []string{},
		},
		{
			name:     "single item",
			input:    []interface{}{"only"},
			expected: []string{"only"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := Interface2StringList(tt.input)
			if len(result) != len(tt.expected) {
				t.Fatalf("Interface2StringList() length = %d, want %d", len(result), len(tt.expected))
			}
			for i, v := range result {
				if v != tt.expected[i] {
					t.Errorf("Interface2StringList()[%d] = %q, want %q", i, v, tt.expected[i])
				}
			}
		})
	}
}

// testStringSlicesEqual compares two string slices treating nil as distinct from empty.
func testStringSlicesEqual(a, b []string) bool {
	if a == nil && b == nil {
		return true
	}
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Acceptance tests

func testAccCheckCockroachDBChangefeedDestroy(s *terraform.State) error {
	client := testAccProvider.Meta().(*Client)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "postgresql_crdb_changefeed" {
			continue
		}

		txn, err := startTransaction(client, "")
		if err != nil {
			return err
		}
		defer deferredRollback(txn)

		exists, err := jobExists(txn, rs.Primary.ID)
		if err != nil {
			return fmt.Errorf("error checking changefeed job %s: %w", rs.Primary.ID, err)
		}

		if exists {
			return fmt.Errorf("changefeed job %s still running after destroy", rs.Primary.ID)
		}
	}

	return nil
}

func testAccCheckCockroachDBChangefeedExists(n string) resource.TestCheckFunc {
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

		exists, err := jobExists(txn, rs.Primary.ID)
		if err != nil {
			return fmt.Errorf("error checking changefeed job: %w", err)
		}

		if !exists {
			return fmt.Errorf("changefeed job not found")
		}

		return nil
	}
}

func TestAccCockroachDBChangefeed_Basic(t *testing.T) {
	skipIfNotAcc(t)

	kafkaURL := os.Getenv("CRDB_TEST_KAFKA_URL")
	if kafkaURL == "" {
		t.Skip("CRDB_TEST_KAFKA_URL must be set for changefeed acceptance tests")
	}
	registryURL := os.Getenv("CRDB_TEST_REGISTRY_URL")
	if registryURL == "" {
		t.Skip("CRDB_TEST_REGISTRY_URL must be set for changefeed acceptance tests")
	}
	testTable := os.Getenv("CRDB_TEST_TABLE")
	if testTable == "" {
		t.Skip("CRDB_TEST_TABLE must be set for changefeed acceptance tests")
	}

	config := fmt.Sprintf(`
resource "postgresql_crdb_external_connection" "kafka" {
  connection_name = "test-changefeed-kafka"
  connection_url  = "%s"
}

resource "postgresql_crdb_external_connection" "registry" {
  connection_name = "test-changefeed-registry"
  connection_url  = "%s"
}

resource "postgresql_crdb_changefeed" "test" {
  table_list               = ["%s"]
  kafka_connection_name    = postgresql_crdb_external_connection.kafka.connection_name
  avro_schema_prefix       = "testprefix"
  registry_connection_name = postgresql_crdb_external_connection.registry.connection_name
  initial_scan             = "no"
}
`, kafkaURL, registryURL, testTable)

	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
			skipIfNotCockroachDB(t)
		},
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckCockroachDBChangefeedDestroy,
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckCockroachDBChangefeedExists("postgresql_crdb_changefeed.test"),
					resource.TestCheckResourceAttr(
						"postgresql_crdb_changefeed.test", "kafka_connection_name", "test-changefeed-kafka"),
					resource.TestCheckResourceAttr(
						"postgresql_crdb_changefeed.test", "avro_schema_prefix", "testprefix"),
					resource.TestCheckResourceAttr(
						"postgresql_crdb_changefeed.test", "registry_connection_name", "test-changefeed-registry"),
					resource.TestCheckResourceAttr(
						"postgresql_crdb_changefeed.test", "initial_scan", "no"),
				),
			},
		},
	})
}

func TestAccCockroachDBChangefeed_WithInitialScan(t *testing.T) {
	skipIfNotAcc(t)

	kafkaURL := os.Getenv("CRDB_TEST_KAFKA_URL")
	if kafkaURL == "" {
		t.Skip("CRDB_TEST_KAFKA_URL must be set for changefeed acceptance tests")
	}
	registryURL := os.Getenv("CRDB_TEST_REGISTRY_URL")
	if registryURL == "" {
		t.Skip("CRDB_TEST_REGISTRY_URL must be set for changefeed acceptance tests")
	}
	testTable := os.Getenv("CRDB_TEST_TABLE")
	if testTable == "" {
		t.Skip("CRDB_TEST_TABLE must be set for changefeed acceptance tests")
	}

	config := fmt.Sprintf(`
resource "postgresql_crdb_external_connection" "kafka" {
  connection_name = "test-initialscan-kafka"
  connection_url  = "%s"
}

resource "postgresql_crdb_external_connection" "registry" {
  connection_name = "test-initialscan-registry"
  connection_url  = "%s"
}

resource "postgresql_crdb_changefeed" "test" {
  table_list               = ["%s"]
  kafka_connection_name    = postgresql_crdb_external_connection.kafka.connection_name
  avro_schema_prefix       = "scanprefix"
  registry_connection_name = postgresql_crdb_external_connection.registry.connection_name
  initial_scan             = "yes"
}
`, kafkaURL, registryURL, testTable)

	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
			skipIfNotCockroachDB(t)
		},
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckCockroachDBChangefeedDestroy,
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckCockroachDBChangefeedExists("postgresql_crdb_changefeed.test"),
					resource.TestCheckResourceAttr(
						"postgresql_crdb_changefeed.test", "initial_scan", "yes"),
					resource.TestCheckResourceAttr(
						"postgresql_crdb_changefeed.test", "avro_schema_prefix", "scanprefix"),
				),
			},
		},
	})
}

func TestAccCockroachDBChangefeed_WithStartFrom(t *testing.T) {
	skipIfNotAcc(t)

	kafkaURL := os.Getenv("CRDB_TEST_KAFKA_URL")
	if kafkaURL == "" {
		t.Skip("CRDB_TEST_KAFKA_URL must be set for changefeed acceptance tests")
	}
	registryURL := os.Getenv("CRDB_TEST_REGISTRY_URL")
	if registryURL == "" {
		t.Skip("CRDB_TEST_REGISTRY_URL must be set for changefeed acceptance tests")
	}
	testTable := os.Getenv("CRDB_TEST_TABLE")
	if testTable == "" {
		t.Skip("CRDB_TEST_TABLE must be set for changefeed acceptance tests")
	}

	config := fmt.Sprintf(`
resource "postgresql_crdb_external_connection" "kafka" {
  connection_name = "test-startfrom-kafka"
  connection_url  = "%s"
}

resource "postgresql_crdb_external_connection" "registry" {
  connection_name = "test-startfrom-registry"
  connection_url  = "%s"
}

resource "postgresql_crdb_changefeed" "test" {
  table_list               = ["%s"]
  kafka_connection_name    = postgresql_crdb_external_connection.kafka.connection_name
  avro_schema_prefix       = "startprefix"
  registry_connection_name = postgresql_crdb_external_connection.registry.connection_name
  initial_scan             = "no"
  start_from               = "2023-01-01 00:00:00"
}
`, kafkaURL, registryURL, testTable)

	resource.Test(t, resource.TestCase{
		PreCheck: func() {
			testAccPreCheck(t)
			skipIfNotCockroachDB(t)
		},
		Providers:    testAccProviders,
		CheckDestroy: testAccCheckCockroachDBChangefeedDestroy,
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeTestCheckFunc(
					testAccCheckCockroachDBChangefeedExists("postgresql_crdb_changefeed.test"),
					resource.TestCheckResourceAttr(
						"postgresql_crdb_changefeed.test", "start_from", "2023-01-01 00:00:00"),
					resource.TestCheckResourceAttr(
						"postgresql_crdb_changefeed.test", "initial_scan", "no"),
				),
			},
		},
	})
}
