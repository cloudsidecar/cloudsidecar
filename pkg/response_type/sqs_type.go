package response_type

import "encoding/xml"

const (
	XmlHeader string = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
	XmlNs string = "http://www.w3.org/2001/XMLSchema-instance"
)

type SqsErrorResponse struct {
	// <?xml version="1.0"?><ErrorResponse xmlns="http://queue.amazonaws.com/doc/2012-11-05/"><Error><Type>Sender</Type><Code>AccessDenied</Code><Message>Access to the resource https://queue.amazonaws.com/ is denied.</Message><Detail/></Error><RequestId>be0bc434-dd62-5a29-bb07-91a3a84796c2</RequestId></ErrorResponse>
	XMLName xml.Name `xml:"ErrorResponse"`
	XmlNS string `xml:"xmlns,attr"`
	Error *SqsError `xml:"Error"`
	RequestId *string `xml:"RequestId"`
}

type SqsError struct {
	Type string `xml:"Type"`
	Code string `xml:"Code"`
	Message string `xml:"Message"`
}

type ListQueuesResponse struct {
	XMLName xml.Name `xml:"ListQueuesResponse"`
	XmlNS string `xml:"xmlns,attr"`
	ListQueuesResult QueueUrls `xml:"ListQueuesResult"`
}

type QueueUrls struct {
	QueueUrl []string `xml:"QueueUrl"`
}

type CreateQueueResponse struct {
	XMLName xml.Name `xml:"CreateQueueResponse"`
	XmlNS string `xml:"xmlns,attr"`
	CreateQueueResult QueueUrls `xml:"CreateQueueResult"`
}

type PurgeQueueResponse struct {
	XMLName xml.Name `xml:"PurgeQueueResponse"`
}

type DeleteQueueResponse struct {
	XMLName xml.Name `xml:"DeleteQueueResponse"`
}

type SendMessageResponse struct {
	XMLName xml.Name `xml:"PurgeQueueResponse"`
	XmlNS string `xml:"xmlns,attr"`
	SendMessageResult SendMessageResult `xml:"SendMessageResult"`
}

type SendMessageResult struct {
	MD5OfMessageBody *string `xml:"MD5OfMessageBody"`
	MD5OfMessageAttributes *string `xml:"MD5OfMessageAttributes"`
	MessageId *string `xml:"MessageId"`
}

type ReceiveMessageResponse struct {
	XMLName xml.Name `xml:"ReceiveMessageResponse"`
	XmlNS string `xml:"xmlns,attr"`
	ReceiveMessageResult ReceiveMessageResult `xml:"ReceiveMessageResult"`
}

type ReceiveMessageResult struct {
	Message []SqsMessage `xml:"Message"`
}

type SqsMessage struct {
	MessageId *string `xml:"MessageId"`
	ReceiptHandle *string `xml:"ReceiptHandle"`
	MD5OfBody *string `xml:"MD5OfBody"`
	Body *string `xml:"Body"`
	Attributes []SqsAttribute `xml:"Attribute"`
}

type SqsAttribute struct {
	Name string `xml:"Name"`
	Value string `xml:"Value"`
}

type DeleteMessageResponse struct {
	XMLName xml.Name `xml:"DeleteMessageResponse"`
}

type DeleteMessageBatchResponse struct {
	XMLName xml.Name `xml:"DeleteMessageBatchResponse"`
	DeleteMessageBatchResult DeleteMessageBatchResult `xml:"DeleteMessageBatchResult"`
}

type DeleteMessageBatchResult struct {
	DeleteMessageBatchResultEntry []DeleteMessageBatchResultEntry `xml:"DeleteMessageBatchResultEntry"`
}

type DeleteMessageBatchResultEntry struct {
	Id *string `xml:"Id"`
}
