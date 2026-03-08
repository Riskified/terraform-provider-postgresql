package postgresql

import (
	"fmt"

	"github.com/blang/semver"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

const (
	defaultProviderMaxOpenConnections = 20
	defaultExpectedCockroachDBVersion = "22.2.0"
)

// Provider returns a terraform.ResourceProvider.
func Provider() *schema.Provider {
	return &schema.Provider{
		Schema: map[string]*schema.Schema{
			"host": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("PGHOST", nil),
				Description: "Name of CockroachDB server address to connect to",
			},
			"port": {
				Type:        schema.TypeInt,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("PGPORT", 26257),
				Description: "The CockroachDB port number to connect to at the server host",
			},
			"database": {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "The name of the database to connect to (defaults to `postgres`).",
				DefaultFunc: schema.EnvDefaultFunc("PGDATABASE", "postgres"),
			},
			"username": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("PGUSER", "postgres"),
				Description: "CockroachDB user name to connect as",
			},
			"password": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("PGPASSWORD", nil),
				Description: "Password for authentication",
				Sensitive:   true,
			},

			"database_username": {
				Type:        schema.TypeString,
				Optional:    true,
				Description: "Database username associated to the connected user (for user name maps)",
			},

			"sslmode": {
				Type:        schema.TypeString,
				Optional:    true,
				DefaultFunc: schema.EnvDefaultFunc("PGSSLMODE", nil),
				Description: "This option determines whether or with what priority a secure SSL TCP/IP connection will be negotiated with the server",
			},
			"ssl_mode": {
				Type:       schema.TypeString,
				Optional:   true,
				Deprecated: "Rename provider `ssl_mode` attribute to `sslmode`",
			},
			"clientcert": {
				Type:        schema.TypeList,
				Optional:    true,
				Description: "SSL client certificate if required by the database.",
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"cert": {
							Type:        schema.TypeString,
							Description: "The SSL client certificate file path. The file must contain PEM encoded data.",
							Required:    true,
						},
						"key": {
							Type:        schema.TypeString,
							Description: "The SSL client certificate private key file path. The file must contain PEM encoded data.",
							Required:    true,
						},
						"sslinline": {
							Type:        schema.TypeBool,
							Description: "Must be set to true if you are inlining the cert/key instead of using a file path.",
							Optional:    true,
						},
					},
				},
				MaxItems: 1,
			},
			"sslrootcert": {
				Type:        schema.TypeString,
				Description: "The SSL server root certificate file path. The file must contain PEM encoded data.",
				Optional:    true,
			},

			"connect_timeout": {
				Type:         schema.TypeInt,
				Optional:     true,
				DefaultFunc:  schema.EnvDefaultFunc("PGCONNECT_TIMEOUT", 180),
				Description:  "Maximum wait for connection, in seconds. Zero or not specified means wait indefinitely.",
				ValidateFunc: validation.IntAtLeast(-1),
			},
			"max_connections": {
				Type:         schema.TypeInt,
				Optional:     true,
				Default:      defaultProviderMaxOpenConnections,
				Description:  "Maximum number of connections to establish to the database. Zero means unlimited.",
				ValidateFunc: validation.IntAtLeast(-1),
			},
			"expected_version": {
				Type:         schema.TypeString,
				Optional:     true,
				Default:      defaultExpectedCockroachDBVersion,
				Description:  "Specify the expected version of CockroachDB.",
				ValidateFunc: validateExpectedVersion,
			},
		},

		ResourcesMap: map[string]*schema.Resource{
			"postgresql_database":                 resourcePostgreSQLDatabase(),
			"postgresql_default_privileges":       resourcePostgreSQLDefaultPrivileges(),
			"postgresql_grant":                    resourcePostgreSQLGrant(),
			"postgresql_grant_role":               resourcePostgreSQLGrantRole(),
			"postgresql_schema":                   resourcePostgreSQLSchema(),
			"postgresql_role":                     resourcePostgreSQLRole(),
			"postgresql_function":                 resourcePostgreSQLFunction(),
			"postgresql_crdb_changefeed":          resourceCockroachDBChangefeed(),
			"postgresql_crdb_external_connection": resourceCockroachDBExternalConnection(),
		},

		DataSourcesMap: map[string]*schema.Resource{
			"postgresql_schemas":   dataSourcePostgreSQLDatabaseSchemas(),
			"postgresql_tables":    dataSourcePostgreSQLDatabaseTables(),
			"postgresql_sequences": dataSourcePostgreSQLDatabaseSequences(),
		},

		ConfigureFunc: providerConfigure,
	}
}

func validateExpectedVersion(v interface{}, key string) (warnings []string, errors []error) {
	if _, err := semver.ParseTolerant(v.(string)); err != nil {
		errors = append(errors, fmt.Errorf("invalid version (%q): %w", v.(string), err))
	}
	return
}

func providerConfigure(d *schema.ResourceData) (interface{}, error) {
	var sslMode string
	if sslModeRaw, ok := d.GetOk("sslmode"); ok {
		sslMode = sslModeRaw.(string)
	} else {
		sslModeDeprecated := d.Get("ssl_mode").(string)
		if sslModeDeprecated != "" {
			sslMode = sslModeDeprecated
		}
	}
	versionStr := d.Get("expected_version").(string)
	version, _ := semver.ParseTolerant(versionStr)

	config := Config{
		Scheme:            "postgres",
		Host:              d.Get("host").(string),
		Port:              d.Get("port").(int),
		Username:          d.Get("username").(string),
		Password:          d.Get("password").(string),
		DatabaseUsername:  d.Get("database_username").(string),
		SSLMode:           sslMode,
		ApplicationName:   "Terraform provider",
		ConnectTimeoutSec: d.Get("connect_timeout").(int),
		MaxConns:          d.Get("max_connections").(int),
		ExpectedVersion:   version,
		SSLRootCertPath:   d.Get("sslrootcert").(string),
	}

	if value, ok := d.GetOk("clientcert"); ok {
		if spec, ok := value.([]interface{})[0].(map[string]interface{}); ok {
			config.SSLClientCert = &ClientCertificateConfig{
				CertificatePath: spec["cert"].(string),
				KeyPath:         spec["key"].(string),
				SSLInline:       spec["sslinline"].(bool),
			}
		}
	}

	client := config.NewClient(d.Get("database").(string))
	return client, nil
}
