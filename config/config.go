package config

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
)


type Config struct {
	AwsConfigs []AWSConfig `aws_configs`
	GcpConfigs []GCPConfig `gcp_configs`
}

type AWSConfig struct {
	ServiceType string `service_type`
	Port int `port`
	UrlPrefix string `url_prefix`
	DestinationAWSConfig *AWSDestinationConfig `aws_destination_config`
	DestinationGCPConfig *GCPDestinationConfig `gcp_destination_config`
}

type AWSDestinationConfig struct {
	Name string `name`
	AccessKeyId string `access_key_id`
	SecretAccessKey string `secret_access_key`
}

type GCPDestinationConfig struct {
	Name string `name`
	Project string `project`
	Instance string `instance`
	IsBigTable bool `is_bigtable`
	DatastoreConfig *GCPDatastoreConfig `datastore_config`
	KeyFileLocation string `key_file_location`
}

type GCPDatastoreConfig struct {
	TableKeyNameMap map[string]string `table_key_map`
}

type GCPConfig struct {
	ServiceType string `service_type`
	Port int `port`
	DestinationAWSConfig *AWSDestinationConfig `aws_destination_config`
	DestinationGCPConfig *GCPDestinationConfig `gcp_destination_config`
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
