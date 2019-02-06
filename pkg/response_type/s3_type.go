package response_type

import (
	"cloud.google.com/go/datastore"
	"encoding/xml"
	"reflect"
)
const (
	ACLXmlNs string = "http://www.w3.org/2001/XMLSchema-instance"
	ACLXmlXsi string = "CanonicalUser"
)

type Map map[string]interface{}


func (m Map) Load(c []datastore.Property) error {
	for _, p := range c {
		if reflect.ValueOf(p.Value).Kind() == reflect.Ptr {
			ptr := p.Value.(* datastore.Entity)
			deref := *ptr
			mapValue := make(Map)
			for _, prop := range deref.Properties{
				mapValue[prop.Name] = prop.Value
			}
			m[p.Name] = mapValue
		} else{
			m[p.Name] = p.Value
		}
	}
	return nil
}

func (m Map) Save() ([]datastore.Property, error) {
	props := make([]datastore.Property, len(m))
	i := 0
	for k, v := range m {
		props[i] = datastore.Property {
			Name: k,
			Value: v,
		}
		i++
	}
	return props, nil
}

type KinesisRequest struct {
	StreamName string
	PartitionKey string
	Data string
	Records []KinesisRecordsRequest
}

type KinesisResponse struct {
	SequenceNumber *string
	ShardId *string
	ErrorCode *string
	ErrorMessage *string
}

type KinesisRecordsRequest struct {
	PartitionKey string
	Data string
}

type KinesisRecordsResponse struct {
	FailedRequestCount int64
	Records []KinesisResponse

}

type AWSACLResponse struct {
	XMLName xml.Name `xml:"AccessControlPolicy"`
	OwnerId string `xml:"Owner>ID"`
	OwnerDisplayName string `xml:"Owner>DisplayName"`
	AccessControlList *AccessControlList `xml:"AccessControlList"`
}

type AccessControlList struct {
	Grants []*Grant `xml:"Grant"`
}

type Grant struct {
	Grantee *Grantee `xml:"Grantee"`
	Permission string `xml:"Permission"`
}

type Grantee struct {
	XMLName xml.Name `xml:"Grantee"`
	Id string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
	XmlNS string `xml:"xmlns:xsi,attr"`
	Xsi string `xml:"xsi:type,attr"`
}

type AWSListBucketResponse struct {
	XMLName xml.Name `xml:"ListBucketResult"`
	XmlNS string `xml:"xmlns,attr"`
	Name *string `xml:"Name"`
	Prefix *string `xml:"Prefix"`
	Delimiter *string `xml:"Delimiter,omitempty"`
	Marker *string `xml:"Marker"`
	KeyCount int64 `xml:"KeyCount"`
	MaxKeys *int64 `xml:"MaxKeys"`
	IsTruncated *bool `xml:"IsTruncated"`
	Contents []*BucketContent `xml:"Contents"`
	CommonPrefixes []*BucketCommonPrefix `xml:"CommonPrefixes,omitempty"`
	ContinuationToken *string `xml:"ContinuationToken"`
	NextContinuationToken *string `xml:"NextContinuationToken"`
}

type BucketContent struct {
	Key string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag string `xml:"ETag"`
	Size int64 `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type BucketCommonPrefix struct {
	Prefix string `xml:"Prefix"`
}

type InitiateMultipartUploadResult struct {
	XMLName xml.Name `xml:"InitiateMultipartUploadResult"`
	XmlNS string `xml:"xmlns,attr"`
	Bucket *string `xml:"Bucket"`
	Key *string `xml:"Key"`
	UploadId *string `xml:"UploadId"`
}

type MultipartPart struct {
	ETag *string `xml:"ETag"`
	PartNumber *int64 `xml:"PartNumber"`
}

type CompleteMultipartUploadInput struct {
	XMLName xml.Name `xml:"CompleteMultipartUpload"`
	XmlNS string `xml:"xmlns,attr"`
	Parts []*MultipartPart `xml:"Part"`
}

type CompleteMultipartUploadResult struct {
	XMLName xml.Name `xml:"CompleteMultipartUploadResult"`
	XmlNS string `xml:"xmlns,attr"`
	Bucket *string `xml:"Bucket"`
	Key *string `xml:"Key"`
	ETag *string `xml:"ETag"`
	Location *string `xml:"Location"`
}
