package response_type

import "encoding/xml"
const (
	ACLXmlNs string = "http://www.w3.org/2001/XMLSchema-instance"
	ACLXmlXsi string = "CanonicalUser"
)

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
	Xsi string `xml:"xsi:response_type,attr"`
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
