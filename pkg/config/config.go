package config

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
)


type Config struct {
	AwsConfigs map[string]AWSConfig `mapstructure:"aws_configs"`
	GcpConfigs map[string]GCPConfig `mapstructure:"gcp_configs"`
}

type AWSConfig struct {
	ServiceType string `mapstructure:"service_type"`
	Port int `mapstructure:"port"`
	UrlPrefix string `mapstructure:"url_prefix"`
	DestinationAWSConfig *AWSDestinationConfig `mapstructure:"aws_destination_config"`
	DestinationGCPConfig *GCPDestinationConfig `mapstructure:"gcp_destination_config"`
}

type AWSDestinationConfig struct {
	Name string `mapstructure:"name"`
	AccessKeyId string `mapstructure:"access_key_id"`
	SecretAccessKey string `mapstructure:"secret_access_key"`
}

type GCPDestinationConfig struct {
	Name string `mapstructure:"name"`
	Project string `mapstructure:"project"`
	Instance string `mapstructure:"instance"`
	IsBigTable bool `mapstructure:"is_bigtable"`
	GCSConfig *GCSConfig `mapstructure:"gcs_config"`
	DatastoreConfig *GCPDatastoreConfig `mapstructure:"datastore_config"`
	KeyFileLocation string `mapstructure:"key_file_location"`
}

type GCSConfig struct {
	BucketRename map[string]string `mapstructure:"bucket_rename"`
	MultipartDBDirectory string `mapstructure:"multipart_db_directory"`
}

type GCPDatastoreConfig struct {
	TableKeyNameMap map[string]string `mapstructure:"table_key_map"`
}

type GCPConfig struct {
	ServiceType string `mapstructure:"service_type"`
	Port int `mapstructure:"port"`
	DestinationAWSConfig *AWSDestinationConfig `mapstructure:"aws_destination_config"`
	DestinationGCPConfig *GCPDestinationConfig `mapstructure:"gcp_destination_config"`
}

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
