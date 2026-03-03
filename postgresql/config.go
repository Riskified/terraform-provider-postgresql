package postgresql

import (
	"database/sql"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"unicode"

	"github.com/blang/semver"
	_ "github.com/lib/pq" // PostgreSQL db
)

type featureName uint

const (
	featureCreateRoleWith featureName = iota
	featureDatabaseOwnerRole
	featureDBAllowConnections
	featureDBIsTemplate
	featureDBTablespace
	featureUseDBTemplate
	featureFallbackApplicationName
	featureRLS
	featureSchemaCreateIfNotExist
	featureReplication
	featureExtension
	featurePrivileges
	featureProcedure
	featureRoutine
	featurePrivilegesOnSchemas
	featureForceDropDatabase
	featurePid
	featurePublishViaRoot
	featurePubTruncate
	featurePublication
	featurePubWithoutTruncate
	featureFunction
	featureServer
	fetureAclExplode
	fetureAclItem
	fetureTerminateBackendFunc
	fetureRoleConnectionLimit
	fetureRoleSuperuser
	featureRoleroleInherit
	fetureRoleEncryptedPass
	featureAdvisoryXactLock
	featureTransactionIsolation
	featureSysPrivileges
	featureFollowerReads
	featureColumnPrivileges
	featureForeignDataWrapper
	featureRoleRename
)

var (
	dbRegistryLock sync.Mutex
	dbRegistry     map[string]*DBConnection = make(map[string]*DBConnection, 1)

	// Mapping of feature flags to CockroachDB versions
	featureSupportedCockroachdb = map[featureName]semver.Range{
		featureCreateRoleWith:          semver.MustParseRange(">=1.0.0"),
		featureDBAllowConnections:      semver.MustParseRange("<1.0.0"),
		featureDBIsTemplate:            semver.MustParseRange("<1.0.0"),
		featureUseDBTemplate:           semver.MustParseRange("<1.0.0"),
		featureDBTablespace:            semver.MustParseRange("<1.0.0"),
		featureFallbackApplicationName: semver.MustParseRange("<1.0.0"),
		featureSchemaCreateIfNotExist:  semver.MustParseRange(">=1.0.0"),
		featureRLS:                     semver.MustParseRange("<1.0.0"),
		featureReplication:             semver.MustParseRange("<1.0.0"),
		featureExtension:               semver.MustParseRange("<1.0.0"),
		featurePrivileges:              semver.MustParseRange(">=21.2.17"),
		featureProcedure:               semver.MustParseRange("<1.0.0"),
		featureRoutine:                 semver.MustParseRange("<1.0.0"),
		featurePrivilegesOnSchemas:     semver.MustParseRange(">=21.2.17"),
		featureForceDropDatabase:       semver.MustParseRange("<1.0.0"),
		featurePid:                     semver.MustParseRange(">=20.2.19"),
		featurePublishViaRoot:          semver.MustParseRange("<1.0.0"),
		featurePubTruncate:             semver.MustParseRange("<1.0.0"),
		featurePubWithoutTruncate:      semver.MustParseRange("<1.0.0"),
		featurePublication:             semver.MustParseRange("<1.0.0"),
		featureFunction:                semver.MustParseRange(">=22.2.17"),
		featureServer:                  semver.MustParseRange("<1.0.0"),
		featureDatabaseOwnerRole:       semver.MustParseRange("<1.0.0"),
		fetureAclExplode:               semver.MustParseRange("<1.0.0"),
		fetureAclItem:                  semver.MustParseRange("<1.0.0"),
		fetureTerminateBackendFunc:     semver.MustParseRange("<1.0.0"),
		fetureRoleConnectionLimit:      semver.MustParseRange("<1.0.0"),
		fetureRoleSuperuser:            semver.MustParseRange("<1.0.0"),
		featureRoleroleInherit:         semver.MustParseRange("<1.0.0"),
		fetureRoleEncryptedPass:        semver.MustParseRange("<1.0.0"),
		featureAdvisoryXactLock:        semver.MustParseRange("<1.0.0"),
		featureTransactionIsolation:    semver.MustParseRange(">=23.2.0"),
		featureSysPrivileges:           semver.MustParseRange(">=22.2.0"),
		featureFollowerReads:           semver.MustParseRange(">=22.2.0"),
		featureColumnPrivileges:        semver.MustParseRange("<1.0.0"),
		featureForeignDataWrapper:      semver.MustParseRange("<1.0.0"),
		featureRoleRename:              semver.MustParseRange("<1.0.0"),
	}
)

type DBConnection struct {
	*sql.DB

	client *Client

	// version is the version number of the database as determined by parsing the
	// output of `SELECT VERSION()`.
	version semver.Version
}

// featureSupported returns true if a given feature is supported or not. This is
// slightly different from Config's featureSupported in that here we're
// evaluating against the fingerprinted version, not the expected version.
func (db *DBConnection) featureSupported(name featureName) bool {
	fn, found := featureSupportedCockroachdb[name]
	if !found {
		// panic'ing because this is a provider-only bug
		panic(fmt.Sprintf("unknown feature flag %v", name))
	}

	return fn(db.version)
}

type ClientCertificateConfig struct {
	CertificatePath string
	KeyPath         string
	SSLInline       bool
}

// Config - provider config
type Config struct {
	Scheme            string
	Host              string
	Port              int
	Username          string
	Password          string
	DatabaseUsername  string
	SSLMode           string
	ApplicationName   string
	Timeout           int
	ConnectTimeoutSec int
	MaxConns          int
	ExpectedVersion   semver.Version
	SSLClientCert     *ClientCertificateConfig
	SSLRootCertPath   string
}

// Client struct holding connection string
type Client struct {
	// Configuration for the client
	config Config

	databaseName string
}

// NewClient returns client config for the specified database.
func (c *Config) NewClient(database string) *Client {
	return &Client{
		config:       *c,
		databaseName: database,
	}
}

func (c *Config) connParams() []string {
	params := map[string]string{}

	params["sslmode"] = c.SSLMode
	params["connect_timeout"] = strconv.Itoa(c.ConnectTimeoutSec)

	if c.SSLClientCert != nil {
		params["sslcert"] = c.SSLClientCert.CertificatePath
		params["sslkey"] = c.SSLClientCert.KeyPath
		if c.SSLClientCert.SSLInline {
			params["sslinline"] = strconv.FormatBool(c.SSLClientCert.SSLInline)
		}
	}

	if c.SSLRootCertPath != "" {
		params["sslrootcert"] = c.SSLRootCertPath
	}

	paramsArray := []string{}
	for key, value := range params {
		paramsArray = append(paramsArray, fmt.Sprintf("%s=%s", key, url.QueryEscape(value)))
	}

	return paramsArray
}

func (c *Config) connStr(database string) string {
	connStr := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?%s",
		url.PathEscape(c.Username),
		url.PathEscape(c.Password),
		c.Host,
		c.Port,
		database,
		strings.Join(c.connParams(), "&"),
	)

	return connStr
}

func (c *Config) getDatabaseUsername() string {
	if c.DatabaseUsername != "" {
		return c.DatabaseUsername
	}
	return c.Username
}

// Connect returns a copy to an sql.Open()'ed database connection wrapped in a DBConnection struct.
// Callers must return their database resources. Use of QueryRow() or Exec() is encouraged.
// Query() must have their rows.Close()'ed.
func (c *Client) Connect() (*DBConnection, error) {
	dbRegistryLock.Lock()
	defer dbRegistryLock.Unlock()

	dsn := c.config.connStr(c.databaseName)
	conn, found := dbRegistry[dsn]
	if !found {
		db, err := sql.Open(proxyDriverName, dsn)

		if err == nil {
			err = db.Ping()
		}
		if err != nil {
			errString := strings.Replace(err.Error(), c.config.Password, "XXXX", 2)
			return nil, fmt.Errorf("Error connecting to CockroachDB server %s: %s", c.config.Host, errString)
		}

		// We don't want to retain connection
		// So when we connect on a specific database which might be managed by terraform,
		// we don't keep opened connection in case of the db has to be dropped in the plan.
		db.SetMaxIdleConns(0)
		db.SetMaxOpenConns(c.config.MaxConns)

		defaultVersion, _ := semver.Parse(defaultExpectedCockroachDBVersion)
		version := &c.config.ExpectedVersion
		if defaultVersion.Equals(c.config.ExpectedVersion) {
			// Version hint not set by user, need to fingerprint
			version, err = fingerprintCapabilities(db)
			if err != nil {
				_ = db.Close()
				return nil, fmt.Errorf("error detecting capabilities: %w", err)
			}
		}

		conn = &DBConnection{
			db,
			c,
			*version,
		}
		dbRegistry[dsn] = conn
	}

	return conn, nil
}

// fingerprintCapabilities queries CockroachDB to determine the version.
// This is only run once per Client.
func fingerprintCapabilities(db *sql.DB) (*semver.Version, error) {
	var pgVersion string
	err := db.QueryRow(`SELECT VERSION()`).Scan(&pgVersion)
	if err != nil {
		return nil, fmt.Errorf("error querying CockroachDB version: %w", err)
	}

	// CockroachDB CCL v24.3.0 (x86_64-pc-linux-gnu, built ...)
	fields := strings.FieldsFunc(pgVersion, func(c rune) bool {
		return unicode.IsSpace(c) || c == ','
	})
	if len(fields) < 3 {
		return nil, fmt.Errorf("error determining the server version: %q", pgVersion)
	}

	if fields[0] != "CockroachDB" {
		return nil, fmt.Errorf("expected CockroachDB, got: %q", pgVersion)
	}

	version, err := semver.ParseTolerant(fields[2])
	if err != nil {
		return nil, fmt.Errorf("error parsing version: %w", err)
	}
	version = semver.MustParse(strings.TrimPrefix(version.String(), "v"))

	return &version, nil
}
