package response_type

import (
	"cloud.google.com/go/datastore"
	"encoding/xml"
)
const (
	ACLXmlNs string = "http://www.w3.org/2001/XMLSchema-instance"
	ACLXmlXsi string = "CanonicalUser"
)

type Map map[string]interface{}

func (m Map) Load(c []datastore.Property) error {
	for _, p := range c {
		/*if p.Multiple {
			value := reflect.ValueOf(m[p.Name])
			if value.Kind() != reflect.Slice {
				m[p.Name] = []interface{}{p.Value}
			} else {
				m[p.Name] = append(m[p.Name].([]interface{}), p.Value)
			}
		} else {
			m[p.Name] = p.Value
		}*/
		m[p.Name] = p.Value
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
