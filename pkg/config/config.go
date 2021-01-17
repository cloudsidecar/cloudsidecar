package config

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
)

type Config struct {
	AwsConfigs       map[string]AWSConfig        `mapstructure:"aws_configs"`
	GcpConfigs       map[string]AWSConfig        `mapstructure:"gcp_configs"`
	Middleware       map[string]MiddlewareConfig `mapstructure:"middleware"`
	Logger           *LogConfig                  `mapstructure:"logger"`
	PanicOnBindError bool                        `mapstructure:"panic_on_bind_error"`
}

type MiddlewareConfig struct {
	Type string `mapstructure:"type"`
}

type LogConfig struct {
	Format *string `mapstructure:"format"`
	Level  *string `mapstructure:"level"`
}

type AWSConfig struct {
	ServiceType          string                `mapstructure:"service_type"`
	Port                 int                   `mapstructure:"port"`
	UrlPrefix            string                `mapstructure:"url_prefix"`
	Middleware           []string              `mapstructure:"middleware"`
	DestinationAWSConfig *AWSDestinationConfig `mapstructure:"aws_destination_config"`
	DestinationGCPConfig *GCPDestinationConfig `mapstructure:"gcp_destination_config"`
}

type AWSDestinationConfig struct {
	Name            string     `mapstructure:"name"`
	AccessKeyId     string     `mapstructure:"access_key_id"`
	SecretAccessKey string     `mapstructure:"secret_access_key"`
	S3Config        *GCSConfig `mapstructure:"s3_config"`
}

type GCPDestinationConfig struct {
	Name            string              `mapstructure:"name"`
	Project         string              `mapstructure:"project"`
	Instance        string              `mapstructure:"instance"`
	GCSConfig       *GCSConfig          `mapstructure:"gcs_config"`
	DatastoreConfig *GCPDatastoreConfig `mapstructure:"datastore_config"`
	KeyFileLocation *string             `mapstructure:"key_file_location"`
	KeyFromUrl      *bool               `mapstructure:"key_from_url"`
	RawKey          *string             `mapstructure:"raw_key"`
}

type GCSConfig struct {
	BucketRename         map[string]string `mapstructure:"bucket_rename"`
	MultipartDBDirectory string            `mapstructure:"multipart_db_directory"`
	MultipartPathPrefix  string            `mapstructure:"multipart_temp_path_prefix"`
}

type GCPDatastoreConfig struct {
	TableKeyNameMap map[string]string `mapstructure:"table_key_map"`
}

/*
type GCPConfig struct {
	ServiceType          string                `mapstructure:"service_type"`
	Port                 int                   `mapstructure:"port"`
	Hostname             *string               `mapstructure:"hostname"`
	DestinationAWSConfig *AWSDestinationConfig `mapstructure:"aws_destination_config"`
	DestinationGCPConfig *GCPDestinationConfig `mapstructure:"gcp_destination_config"`
}
*/

func FromFile(filename string) *Config {
	var config Config
	source, err := ioutil.ReadFile(filename)
	if err != nil {
		panic(err)
	}
	yamlErr := yaml.Unmarshal(source, &config)
	if yamlErr != nil {
		log.Fatalf("error: %v", yamlErr)
	}
	return &config
}
