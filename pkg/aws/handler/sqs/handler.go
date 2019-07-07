package sqs

import (
	kms "cloud.google.com/go/kms/apiv1"
	"cloud.google.com/go/pubsub"
	"cloudsidecar/pkg/aws/handler/kinesis"
	"cloudsidecar/pkg/logging"
	"cloudsidecar/pkg/response_type"
	"context"
	"encoding/xml"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/gorilla/mux"
	"github.com/spf13/viper"
	"net/http"
	"strconv"
	"strings"
)

type Handler struct {
	SqsClient *sqs.SQS
	GCPClient kinesis.GCPClient
	GCPClientToTopic func(topic string, client kinesis.GCPClient) kinesis.GCPTopic
	GCPResultWrapper func(result *pubsub.PublishResult) kinesis.GCPPublishResult
	GCPKMSClient *kms.KeyManagementClient
	Context *context.Context
	Config *viper.Viper
}

type HandlerInterface interface {
	GetSqsClient() *sqs.SQS
	GetGCPClient() kinesis.GCPClient
	GetContext() *context.Context
	GetConfig() *viper.Viper
	SetSqsClient(sqsClient *sqs.SQS)
	SetGCPClient(gcpClient kinesis.GCPClient)
	SetContext(context *context.Context)
	SetConfig(config *viper.Viper)

	Register(mux *mux.Router)
	New(handler *Handler) *Handler
	Handle(writer http.ResponseWriter, request *http.Request)
	ListHandle(writer http.ResponseWriter, request *http.Request)
	ListHandleParseInput(r *http.Request) (*sqs.ListQueuesInput, error)
	CreateHandle(writer http.ResponseWriter, request *http.Request)
	CreateHandleParseInput(r *http.Request) (*sqs.CreateQueueInput, error)
	PurgeHandle(writer http.ResponseWriter, request *http.Request)
	PurgeHandleParseInput(r *http.Request) (*sqs.PurgeQueueInput, error)
	DeleteHandle(writer http.ResponseWriter, request *http.Request)
	DeleteHandleParseInput(r *http.Request) (*sqs.DeleteQueueInput, error)
	SendHandle(writer http.ResponseWriter, request *http.Request)
	SendHandleParseInput(r *http.Request) (*sqs.SendMessageInput, error)
	ReceiveHandle(writer http.ResponseWriter, request *http.Request)
	ReceiveHandleParseInput(r *http.Request) (*sqs.ReceiveMessageInput, error)
}

func (handler *Handler) GetSqsClient() *sqs.SQS {
	return handler.SqsClient
}
func (handler *Handler) GetGCPClient() kinesis.GCPClient {
	return handler.GCPClient
}
func (handler *Handler) GetContext() *context.Context{
	return handler.Context
}
func (handler *Handler) GetConfig() *viper.Viper {
	return handler.Config
}
func (handler *Handler) SetSqsClient(sqsClient *sqs.SQS){
	handler.SqsClient = sqsClient
}
func (handler *Handler) SetGCPClient(gcpClient kinesis.GCPClient) {
	handler.GCPClient = gcpClient
}
func (handler *Handler) SetContext(context *context.Context) {
	handler.Context = context
}
func (handler *Handler) SetConfig(config *viper.Viper) {
	handler.Config = config
}



func New() *Handler {
	return &Handler{}
}

func (handler *Handler) Register(mux *mux.Router) {
	mux.HandleFunc("/", handler.Handle).Methods("POST")
}

func (handler *Handler) Handle(writer http.ResponseWriter, request *http.Request) {
	request.ParseForm()
	action := request.Form.Get("Action")
	logging.Log.Infof("Action %s", action)
	if action == "ListQueues" {
		handler.ListHandle(writer, request)
	} else if action == "CreateQueue" {
		handler.CreateHandle(writer, request)
	} else if action == "PurgeQueue" {
		handler.PurgeHandle(writer, request)
	} else if action == "DeleteQueue" {
		handler.DeleteHandle(writer, request)
	} else if action == "SendMessage" {
		handler.SendHandle(writer, request)
	} else if action == "ReceiveMessage" {
		handler.ReceiveHandle(writer, request)
	}
}

func processError(err error, writer http.ResponseWriter) {
	var errorResp *response_type.SqsErrorResponse
	if strings.Contains(err.Error(), "AccessDenied") {
		writer.WriteHeader(403)
		errorResp = &response_type.SqsErrorResponse{
			XmlNS: response_type.XmlNs,
			Error: &response_type.SqsError{
				Type:    "Sender",
				Code:    "403",
				Message: err.Error(),
			},
		}
	} else if strings.Contains(err.Error(), "InvalidAddress"){
		writer.WriteHeader(404)
		errorResp = &response_type.SqsErrorResponse{
			XmlNS: response_type.XmlNs,
			Error: &response_type.SqsError{
				Type: "Sender",
				Code: "InvalidAddress",
				Message: err.Error(),
			},
		}
	} else {
		writer.WriteHeader(401)
		errorResp = &response_type.SqsErrorResponse{
			XmlNS: response_type.XmlNs,
			Error: &response_type.SqsError{
				Type: "Sender",
				Code: "Unknown",
				Message: err.Error(),
			},
		}
	}
	output, _ := xml.Marshal(errorResp)
	writer.Write([]byte(response_type.XmlHeader))
	writer.Write([]byte(string(output)))
}

func (handler *Handler) ListHandle(writer http.ResponseWriter, request *http.Request) {
	params, err := handler.ListHandleParseInput(request)
	if err != nil {
		writer.WriteHeader(400)
		writer.Write([]byte(fmt.Sprint(err)))
		return
	}
	var response *response_type.ListQueuesResponse
	if handler.GCPClient != nil {

	} else {
		resp, err := handler.SqsClient.ListQueuesRequest(params).Send()
		if err != nil {
			logging.Log.Errorf("Error listing queues AWS %s", err)
			processError(err, writer)
			return
		}
		response = &response_type.ListQueuesResponse{
			ListQueuesResult: response_type.QueueUrls{
				QueueUrl: resp.QueueUrls,
			},
			XmlNS: response_type.XmlNs,
		}
	}
	output, _ := xml.Marshal(response)
	logging.Log.Debugf("Writing %s", string(output))
	writer.Write([]byte(response_type.XmlHeader))
	writer.Write([]byte(string(output)))
}

func (handler *Handler) ListHandleParseInput(r *http.Request) (*sqs.ListQueuesInput, error) {
	prefix := r.Form.Get("QueueNamePrefix")
	return &sqs.ListQueuesInput{
		QueueNamePrefix: &prefix,
	}, nil
}

func (handler *Handler) CreateHandle(writer http.ResponseWriter, request *http.Request) {
	params, err := handler.CreateHandleParseInput(request)
	if err != nil {
		writer.WriteHeader(400)
		writer.Write([]byte(fmt.Sprint(err)))
		return
	}
	var response *response_type.CreateQueueResponse
	if handler.GCPClient != nil {

	} else {
		resp, err := handler.SqsClient.CreateQueueRequest(params).Send()
		if err != nil {
			logging.Log.Errorf("Error creating queue AWS %s", err)
			processError(err, writer)
			return
		}
		urls := []string{*resp.QueueUrl}
		response = &response_type.CreateQueueResponse{
			CreateQueueResult: response_type.QueueUrls{
				QueueUrl: urls,
			},
			XmlNS: response_type.XmlNs,
		}
	}
	output, _ := xml.Marshal(response)
	logging.Log.Debugf("Writing %s", string(output))
	writer.Write([]byte(response_type.XmlHeader))
	writer.Write([]byte(string(output)))
}
func (handler *Handler) CreateHandleParseInput(r *http.Request) (*sqs.CreateQueueInput, error) {
	name := r.Form.Get("QueueName")
	input := &sqs.CreateQueueInput{
		QueueName: &name,
	}
	attributes := make(map[string]string)
	for key, value := range r.Form {
		if strings.HasPrefix(key, "Attribute") && strings.Contains(key, "Name") {
			valueKey := strings.Replace(key, "Name", "Value", 1)
			attributes[value[0]] = r.Form.Get(valueKey)
		}
	}
	if len(attributes) > 0 {
		input.Attributes = attributes
	}
	return input, nil
}

func (handler *Handler) PurgeHandle(writer http.ResponseWriter, request *http.Request) {
	params, err := handler.PurgeHandleParseInput(request)
	if err != nil {
		writer.WriteHeader(400)
		writer.Write([]byte(fmt.Sprint(err)))
		return
	}
	var response *response_type.PurgeQueueResponse
	if handler.GCPClient != nil {

	} else {
		_, err := handler.SqsClient.PurgeQueueRequest(params).Send()
		if err != nil {
			logging.Log.Errorf("Error purging queue AWS %s", err)
			processError(err, writer)
			return
		}
		response = &response_type.PurgeQueueResponse{}
	}
	output, _ := xml.Marshal(response)
	logging.Log.Debugf("Writing %s", string(output))
	writer.Write([]byte(response_type.XmlHeader))
	writer.Write([]byte(string(output)))
}

func (handler *Handler) PurgeHandleParseInput(r *http.Request) (*sqs.PurgeQueueInput, error) {
	url := r.Form.Get("QueueUrl")
	return &sqs.PurgeQueueInput{
		QueueUrl: &url,
	}, nil
}

func (handler *Handler) DeleteHandle(writer http.ResponseWriter, request *http.Request) {
	params, err := handler.DeleteHandleParseInput(request)
	if err != nil {
		writer.WriteHeader(400)
		writer.Write([]byte(fmt.Sprint(err)))
		return
	}
	var response *response_type.DeleteQueueResponse
	if handler.GCPClient != nil {

	} else {
		_, err := handler.SqsClient.DeleteQueueRequest(params).Send()
		if err != nil {
			logging.Log.Errorf("Error deleting queue AWS %s", err)
			processError(err, writer)
			return
		}
		response = &response_type.DeleteQueueResponse{}
	}
	output, _ := xml.Marshal(response)
	logging.Log.Debugf("Writing %s", string(output))
	writer.Write([]byte(response_type.XmlHeader))
	writer.Write([]byte(string(output)))
}
func (handler *Handler) DeleteHandleParseInput(r *http.Request) (*sqs.DeleteQueueInput, error) {
	url := r.Form.Get("QueueUrl")
	return &sqs.DeleteQueueInput{
		QueueUrl: &url,
	}, nil
}

func (handler *Handler) SendHandle(writer http.ResponseWriter, request *http.Request) {
	params, err := handler.SendHandleParseInput(request)
	if err != nil {
		writer.WriteHeader(400)
		writer.Write([]byte(fmt.Sprint(err)))
		return
	}
	var response *response_type.SendMessageResponse
	if handler.GCPClient != nil {

	} else {
		resp, err := handler.SqsClient.SendMessageRequest(params).Send()
		if err != nil {
			logging.Log.Errorf("Error purging queue AWS %s", err)
			processError(err, writer)
			return
		}
		response = &response_type.SendMessageResponse{
			XmlNS: response_type.XmlNs,
			SendMessageResult: response_type.SendMessageResult{
				MD5OfMessageBody: resp.MD5OfMessageBody,
				MD5OfMessageAttributes: resp.MD5OfMessageAttributes,
				MessageId: resp.MessageId,
			},
		}
	}
	output, _ := xml.Marshal(response)
	logging.Log.Debugf("Writing %s", string(output))
	writer.Write([]byte(response_type.XmlHeader))
	writer.Write([]byte(string(output)))
}
func (handler *Handler) SendHandleParseInput(r *http.Request) (*sqs.SendMessageInput, error) {
	url := r.Form.Get("QueueUrl")
	body := r.Form.Get("MessageBody")
	input := &sqs.SendMessageInput{
		QueueUrl: &url,
		MessageBody: &body,
	}
	attributes := make(map[string]sqs.MessageAttributeValue)
	for key, attributeValue := range r.Form {
		if strings.HasPrefix(key, "MessageAttribute") && strings.Contains(key, "Name") {
			messageAttributeValue := sqs.MessageAttributeValue{}
			valueTypeKey := strings.Replace(key, "Name", "Value.DataType", 1)
			valueType := r.Form.Get(valueTypeKey)
			messageAttributeValue.DataType = &valueType
			if valueType == "String" || valueType == "Number" {
				valueKey := strings.Replace(key, "Name", "Value.StringValue", 1)
				value := r.Form.Get(valueKey)
				messageAttributeValue.StringValue = &value
			} else {
				valueKey := strings.Replace(key, "Name", "Value.BinaryValue", 1)
				value := r.Form.Get(valueKey)
				messageAttributeValue.BinaryValue = []byte(value)
			}
			attributes[attributeValue[0]] = messageAttributeValue
		}
	}
	if len(attributes) > 0 {
		input.MessageAttributes = attributes
	}
	return input, nil

}

func (handler *Handler) ReceiveHandle(writer http.ResponseWriter, request *http.Request) {
	params, err := handler.ReceiveHandleParseInput(request)
	if err != nil {
		writer.WriteHeader(400)
		writer.Write([]byte(fmt.Sprint(err)))
		return
	}
	var response *response_type.ReceiveMessageResponse
	if handler.GCPClient != nil {

	} else {
		resp, err := handler.SqsClient.ReceiveMessageRequest(params).Send()
		if err != nil {
			logging.Log.Errorf("Error purging queue AWS %s", err)
			processError(err, writer)
			return
		}
		messages := make([]response_type.SqsMessage, len(resp.Messages))
		for i, msg := range resp.Messages {
			attributes := make([]response_type.SqsAttribute, len(msg.Attributes))
			attributeIndex := 0
			for attributeKey, attributeValue := range msg.Attributes {
				attributes[attributeIndex] = response_type.SqsAttribute{
					Name: attributeKey,
					Value: attributeValue,
				}
			}
			messages[i] = response_type.SqsMessage{
				MessageId: msg.MessageId,
				ReceiptHandle: msg.ReceiptHandle,
				MD5OfBody: msg.MD5OfBody,
				Body: msg.Body,
				Attributes: attributes,
			}
		}
		response = &response_type.ReceiveMessageResponse{
			XmlNS: response_type.XmlNs,
			ReceiveMessageResult: response_type.ReceiveMessageResult{
				Message: messages,
			},
		}
	}
	output, _ := xml.Marshal(response)
	logging.Log.Debugf("Writing %s", string(output))
	writer.Write([]byte(response_type.XmlHeader))
	writer.Write([]byte(string(output)))
}
func (handler *Handler) ReceiveHandleParseInput(r *http.Request) (*sqs.ReceiveMessageInput, error) {
	url := r.Form.Get("QueueUrl")
	maxMessages := r.Form.Get("MaxNumberOfMessages")
	visibility := r.Form.Get("VisibilityTimeout")
	input := &sqs.ReceiveMessageInput{
		QueueUrl: &url,
	}
	if maxMessages != "" {
		maxNumber, _ := strconv.ParseInt(maxMessages, 10, 64)
		input.MaxNumberOfMessages = &maxNumber
	}
	if visibility != "" {
		visibilityNumber, _ := strconv.ParseInt(visibility, 10, 64)
		input.VisibilityTimeout = &visibilityNumber
	}
	attributes := make([]sqs.QueueAttributeName, 0)
	messageAttributes := make([]string, 0)
	for key, formValue := range r.Form {
		if strings.HasPrefix(key, "AttributeName") {
			attributes = append(attributes, sqs.QueueAttributeName(formValue[0]))
		}
		if strings.HasPrefix(key, "MessageAttributeName") {
			messageAttributes = append(messageAttributes, formValue[0])
		}
	}
	if len(attributes) > 0 {
		input.AttributeNames = attributes
	}
	if len(messageAttributes) > 0 {
		input.MessageAttributeNames = messageAttributes
	}
	return input, nil
}
