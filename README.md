# Cloud Sidecar

## Introduction
Cloud Sidecar (CS) is a utility to allow software to be written in a cloud agnostic manner while being able to take advantage
of the features a specific cloud may offer.  It runs next to your existing application and implelments a common API that
is compatible with most cloud SDKs.  Cloud Sidecar allows you to switch providers (or use multiple providers) for common
cloud products like file storage, key vale store, NoSQL database, queues, messaging, etc...

## How It Works
Cloud Sidecar exposes an API that is compatible with some AWS APIs.  It is meant to run next to your application as a
[sidecar](https://docs.microsoft.com/en-us/azure/architecture/patterns/sidecar).  It is configuration driven (hot reloads)
and requires very little code change to be used.

### Boto3 Python Instructions
Just pass in an endpoint_url when you create a resource or any boto object.  Example:
```
client = boto3.resource(
    "dynamodb",
    region_name="us-east",
    aws_access_key_id="meow",
    aws_secret_access_key="cow",
    endpoint_url='http://localhost:3452',
    use_ssl=False,
)
client = boto3.client(
    "kinesis",
    region_name="us-east",
    aws_access_key_id="meow",
    aws_secret_access_key="cow",
    endpoint_url='http://localhost:3451',
    use_ssl=False,
)
```
This is assuming CS is running on the same host on port 3452 and 3451

### Java AWS Instructions
Similar to boto, just set an endpoint when creating your client.  Example:
```
AmazonS3ClientBuilder.standard()
    .withEndpointConfiguration(new EndpointConfiguration("http://localhost:3451", "bleh"))
    .withPathStyleAccessEnabled(true)
    .build()
```
This is assuming that CS is running on the same host on port 3451

### Pyhon Google Cloud API v2 **Not Implemented**
You need to extend the Client and change the service address.  Example:
```
from google.cloud.bigtable_v2.gapic import bigtable_client

class Bleh(bigtable_client.BigtableClient):
  SERVICE_ADDRESS = 'localhost:3453'

bleh = Bleh()
```

### Java Google Cloud API v2 **Not Implemented**
Set the host of the service via the Java API.  This might vary based on service. Example:
```
StorageOptions.newBuilder().setHost("http://localhost:1234").setProjectId("boo").build().getService
```
```
InstantiatingGrpcChannelProvider prov = InstantiatingGrpcChannelProvider.newBuilder().setEndpoint("localhost:1234").build()
BigtableDataSettings settings = BigtableDataSettings.newBuilder().setTransportChannelProvider(prov).setInstanceName(InstanceName.of("project", "instance")).build()
BigtableDataClient.create(settings).readRow("aaa", "bbb")
```

## Installing and compiling
Requires [dep](https://github.com/golang/dep)
Just run clone and run `dep ensure` to get dependencies. run `go build main.go` to compile.

## Configure
Take a look at example.conf

## Run
`./main example.conf`



## Notes
dep ensure -add gopkg.in/yaml.v2
antlr -Dlanguage=Go -o dynamo_parser Dynamo.g4
