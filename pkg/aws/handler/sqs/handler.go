package sqs

import (
	kms "cloud.google.com/go/kms/apiv1"
	"cloud.google.com/go/pubsub"
	"cloudsidecar/pkg/aws/handler/kinesis"
	"cloudsidecar/pkg/logging"
	"cloudsidecar/pkg/response_type"
	"context"
	"crypto/md5"
	"encoding/xml"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/gorilla/mux"
	"github.com/spf13/viper"
	kmsproto "google.golang.org/genproto/googleapis/cloud/kms/v1"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Handler struct {
	SqsClient               *sqs.SQS
	GCPClient               kinesis.GCPClient
	GCPClientToTopic        func(topic string, client kinesis.GCPClient) kinesis.GCPTopic
	GCPResultWrapper        func(result *pubsub.PublishResult) kinesis.GCPPublishResult
	GCPKMSClient            *kms.KeyManagementClient
	GCPClientToSubscription func(client kinesis.GCPClient, id string)
	Context                 *context.Context
	Config                  *viper.Viper
	ToAck                   map[string]chan bool
}

func NewHandler(config *viper.Viper) Handler {
	return Handler{
		Config: config,
		GCPClientToTopic: func(topic string, client kinesis.GCPClient) kinesis.GCPTopic {
			return client.Topic(topic)
		},
		GCPResultWrapper: func(result *pubsub.PublishResult) kinesis.GCPPublishResult {
			return result
		},
		ToAck: make(map[string]chan bool),
	}
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
	SendBatchHandle(writer http.ResponseWriter, request *http.Request)
	SendBatchHandleParseInput(r *http.Request) (*sqs.SendMessageBatchInput, error)
	ReceiveHandle(writer http.ResponseWriter, request *http.Request)
	ReceiveHandleParseInput(r *http.Request) (*sqs.ReceiveMessageInput, error)
	DeleteMessageHandle(writer http.ResponseWriter, request *http.Request)
	DeleteMessageHandleParseInput(r *http.Request) (*sqs.DeleteMessageInput, error)
	DeleteMessageBatchHandle(writer http.ResponseWriter, request *http.Request)
	DeleteMessageBatchHandleParseInput(r *http.Request) (*sqs.DeleteMessageBatchInput, error)
}

type MessageAttributeNameAndValue struct {
	Name  *string
	Value *sqs.MessageAttributeValue
}

func (handler *Handler) Shutdown() {
	logging.Log.Debug("Closing pubsub")
	if err := handler.GCPClient.Close(); err != nil {
		logging.Log.Error("Some error closing pubsub", err)
	}
	if err := handler.GCPKMSClient.Close(); err != nil {
		logging.Log.Error("Some error closing PKMS", err)
	}
}

func (handler *Handler) GetSqsClient() *sqs.SQS {
	return handler.SqsClient
}
func (handler *Handler) GetGCPClient() kinesis.GCPClient {
	return handler.GCPClient
}
func (handler *Handler) GetContext() *context.Context {
	return handler.Context
}
func (handler *Handler) GetConfig() *viper.Viper {
	return handler.Config
}
func (handler *Handler) SetSqsClient(sqsClient *sqs.SQS) {
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
	return &Handler{
		ToAck: make(map[string]chan bool),
	}
}

func (handler *Handler) Register(mux *mux.Router) {
	mux.HandleFunc("/", handler.Handle).Methods("POST")
	mux.HandleFunc("/{subscription}", handler.Handle).Methods("POST")
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
	} else if action == "DeleteMessage" {
		handler.DeleteMessageHandle(writer, request)
	} else if action == "DeleteMessageBatch" {
		handler.DeleteMessageBatchHandle(writer, request)
	} else if action == "SendMessageBatch" {
		handler.SendBatchHandle(writer, request)
	} else {
		processError(errors.New("Invalid function "+action), writer)
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
	} else if strings.Contains(err.Error(), "InvalidAddress") {
		writer.WriteHeader(404)
		errorResp = &response_type.SqsErrorResponse{
			XmlNS: response_type.XmlNs,
			Error: &response_type.SqsError{
				Type:    "Sender",
				Code:    "InvalidAddress",
				Message: err.Error(),
			},
		}
	} else {
		writer.WriteHeader(401)
		errorResp = &response_type.SqsErrorResponse{
			XmlNS: response_type.XmlNs,
			Error: &response_type.SqsError{
				Type:    "Sender",
				Code:    "Unknown",
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
		processError(err, writer)
		return
	}
	var response *response_type.ListQueuesResponse
	if handler.Config.IsSet("gcp_destination_config") {
		err := errors.New("unsupported operation")
		processError(err, writer)
		return
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

func (handler *Handler) createURL(queueName string) string {
	return fmt.Sprintf("http://%s:%d/%s", handler.Config.GetString("hostname"), handler.Config.GetInt("port"), queueName)
}

func (handler *Handler) CreateHandle(writer http.ResponseWriter, request *http.Request) {
	params, err := handler.CreateHandleParseInput(request)
	if err != nil {
		processError(err, writer)
		return
	}
	var response *response_type.CreateQueueResponse
	if handler.Config.IsSet("gcp_destination_config") {
		id := strings.ReplaceAll(*params.QueueName, "-", "")
		_, err := handler.GCPClient.CreateTopic(*handler.Context, *params.QueueName)
		if err != nil {
			if strings.Contains(err.Error(), "code = AlreadyExists") {
				logging.Log.Infof("Topic %s already exists, assuming it is correct", *params.QueueName)
			} else {
				logging.Log.Error("Error creating", err)
				processError(err, writer)
				return
			}
		}
		topic := handler.GCPClient.Topic(*params.QueueName)
		ackDuration, _ := time.ParseDuration("1m")
		retentionDuration, _ := time.ParseDuration("1d")
		_, err = handler.GCPClient.CreateSubscription(*handler.Context, id, pubsub.SubscriptionConfig{
			Topic:             topic,
			AckDeadline:       ackDuration,
			RetentionDuration: retentionDuration,
		})
		if err != nil {
			if strings.Contains(err.Error(), "code = AlreadyExists") {
				logging.Log.Infof("Subscription %s already exists, assuming it is correct", id)
			} else {
				logging.Log.Errorf("Error creating subscription %s", err)
				processError(err, writer)
				return
			}
		}
		response = &response_type.CreateQueueResponse{
			CreateQueueResult: response_type.QueueUrls{
				QueueUrl: []string{handler.createURL(*params.QueueName)},
			},
			XmlNS: response_type.XmlNs,
		}
	} else {
		_, err := handler.SqsClient.CreateQueueRequest(params).Send()
		if err != nil {
			logging.Log.Errorf("Error creating queue AWS %s", err)
			processError(err, writer)
			return
		}
		urls := []string{handler.createURL(*params.QueueName)}
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
		processError(err, writer)
		return
	}
	var response *response_type.PurgeQueueResponse
	if handler.Config.IsSet("gcp_destination_config") {
		err := errors.New("unsupported operation")
		processError(err, writer)
		return
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
		processError(err, writer)
		return
	}
	var response *response_type.DeleteQueueResponse
	if handler.Config.IsSet("gcp_destination_config") {

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
		processError(err, writer)
		return
	}
	var response *response_type.SendMessageResponse
	if handler.Config.IsSet("gcp_destination_config") {
		pieces := strings.Split(*params.QueueUrl, "/")
		id := pieces[len(pieces)-1]
		topic := handler.GCPClientToTopic(id, handler.GCPClient)
		body := []byte(*params.MessageBody)
		req, err := handler.gcpPublish(topic, id, &pubsub.Message{
			Data: body,
		})
		defer topic.Stop()
		if err != nil {
			logging.Log.Error("Error sending", err)
			processError(err, writer)
			return
		}
		resp, err := req.Get(*handler.Context)
		md5OfBody := fmt.Sprintf("%x", md5.Sum(body))
		if err != nil {
			logging.Log.Error("Error sending", err)
			processError(err, writer)
			return
		}
		response = &response_type.SendMessageResponse{
			XmlNS: response_type.XmlNs,
			SendMessageResult: response_type.SendMessageResult{
				MD5OfMessageBody: &md5OfBody,
				MessageId:        &resp,
			},
		}
	} else {
		getUrl, err := handler.SqsClient.GetQueueUrlRequest(&sqs.GetQueueUrlInput{
			QueueName: params.QueueUrl,
		}).Send()
		if err != nil {
			logging.Log.Errorf("Error getting url for AWS %s %s", *params.QueueUrl, err)
			processError(err, writer)
			return
		}
		logging.Log.Infof("Booya %s", *getUrl.QueueUrl)
		params.QueueUrl = getUrl.QueueUrl
		resp, err := handler.SqsClient.SendMessageRequest(params).Send()
		if err != nil {
			logging.Log.Errorf("Error sending AWS %s", err)
			processError(err, writer)
			return
		}
		response = &response_type.SendMessageResponse{
			XmlNS: response_type.XmlNs,
			SendMessageResult: response_type.SendMessageResult{
				MD5OfMessageBody:       resp.MD5OfMessageBody,
				MD5OfMessageAttributes: resp.MD5OfMessageAttributes,
				MessageId:              resp.MessageId,
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
	if url == "" {
		url = r.URL.Path[1:]
		logging.Log.Infof("queue url is %s", url)
	}
	body := r.Form.Get("MessageBody")
	input := &sqs.SendMessageInput{
		QueueUrl:    &url,
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

func (handler *Handler) SendBatchHandle(writer http.ResponseWriter, request *http.Request) {
	params, err := handler.SendBatchHandleParseInput(request)
	if err != nil {
		processError(err, writer)
		return
	}
	var response *response_type.SendMessageBatchResponse
	if handler.Config.IsSet("gcp_destination_config") {
		pieces := strings.Split(*params.QueueUrl, "/")
		id := pieces[len(pieces)-1]
		topic := handler.GCPClientToTopic(id, handler.GCPClient)
		defer topic.Stop()
		success := make([]response_type.SendMessageBatchResultEntry, 0)
		for _, entry := range params.Entries {
			body := []byte(*entry.MessageBody)
			req, err := handler.gcpPublish(topic, id, &pubsub.Message{
				Data: body,
			})
			if err != nil {
				logging.Log.Error("Error sending", err)
				processError(err, writer)
				return
			}
			resp, err := req.Get(*handler.Context)
			md5OfBody := fmt.Sprintf("%x", md5.Sum(body))
			if err != nil {
				logging.Log.Error("Error sending", err)
			} else {
				success = append(success, response_type.SendMessageBatchResultEntry{
					MD5OfMessageBody: &md5OfBody,
					MessageId:        &resp,
					Id:               entry.Id,
				})
			}
		}
		response = &response_type.SendMessageBatchResponse{
			SendMessageBatchResult: response_type.SendMessageBatchResult{
				Entries: success,
			},
		}
	} else {
		logging.Log.Debugf("Sending %v", params)
		resp, err := handler.SqsClient.SendMessageBatchRequest(params).Send()
		if err != nil {
			logging.Log.Errorf("Error sending batch AWS %s", err)
			processError(err, writer)
			return
		}
		entries := make([]response_type.SendMessageBatchResultEntry, len(resp.Successful))
		for i, entry := range resp.Successful {
			entries[i] = response_type.SendMessageBatchResultEntry{
				Id:                     entry.Id,
				MessageId:              entry.MessageId,
				MD5OfMessageAttributes: entry.MD5OfMessageAttributes,
				MD5OfMessageBody:       entry.MD5OfMessageBody,
			}
		}
		response = &response_type.SendMessageBatchResponse{
			SendMessageBatchResult: response_type.SendMessageBatchResult{
				Entries: entries,
			},
		}
	}
	output, _ := xml.Marshal(response)
	logging.Log.Debugf("Writing %s", string(output))
	writer.Write([]byte(response_type.XmlHeader))
	writer.Write([]byte(string(output)))
}
func (handler *Handler) SendBatchHandleParseInput(r *http.Request) (*sqs.SendMessageBatchInput, error) {
	url := r.Form.Get("QueueUrl")
	entries := make(map[string]*sqs.SendMessageBatchRequestEntry)
	messageAttributes := make(map[string]map[string]*MessageAttributeNameAndValue)
	for formKey, formValue := range r.Form {
		if strings.HasPrefix(formKey, "SendMessageBatchRequestEntry") {
			pieces := strings.SplitN(formKey, ".", 3)
			entryIndex := pieces[1]
			entryName := pieces[2]
			entryValue := formValue[0]
			if _, ok := entries[entryIndex]; !ok {
				entries[entryIndex] = &sqs.SendMessageBatchRequestEntry{}
			}
			if entryName == "Id" {
				entries[entryIndex].Id = &entryValue
			} else if entryName == "MessageBody" {
				entries[entryIndex].MessageBody = &entryValue
			} else if strings.Contains(entryName, "MessageAttribute") {
				messageAttributePieces := strings.Split(entryName, ".")
				messageAttributeIndex := messageAttributePieces[1]
				if _, ok := messageAttributes[entryIndex]; !ok {
					messageAttributes[entryIndex] = make(map[string]*MessageAttributeNameAndValue)
					messageAttributes[entryIndex][messageAttributeIndex] = &MessageAttributeNameAndValue{
						Value: &sqs.MessageAttributeValue{},
					}
				}
				if messageAttributePieces[2] == "Name" {
					messageAttributes[entryIndex][messageAttributeIndex].Name = &entryValue
				} else if messageAttributePieces[2] == "Value" {
					if messageAttributePieces[3] == "DataType" {
						messageAttributes[entryIndex][messageAttributeIndex].Value.DataType = &entryValue
					} else if messageAttributePieces[3] == "StringValue" {
						messageAttributes[entryIndex][messageAttributeIndex].Value.StringValue = &entryValue
					} else if messageAttributePieces[3] == "BinaryValue" {
						messageAttributes[entryIndex][messageAttributeIndex].Value.BinaryValue = []byte(entryValue)
					}
				}
			}
		}
	}
	for attributeKey, attributeValue := range messageAttributes {
		entries[attributeKey].MessageAttributes = make(map[string]sqs.MessageAttributeValue)
		for _, messageAttribute := range attributeValue {
			entries[attributeKey].MessageAttributes[*messageAttribute.Name] = *messageAttribute.Value
		}
	}
	entriesList := make([]sqs.SendMessageBatchRequestEntry, len(entries))
	entryIndex := 0
	for _, entry := range entries {
		entriesList[entryIndex] = *entry
		entryIndex++
	}
	input := &sqs.SendMessageBatchInput{
		QueueUrl: &url,
		Entries:  entriesList,
	}
	return input, nil
}

func (handler *Handler) gcpReceive(params sqs.ReceiveMessageInput, subscriptionInfo pubsub.SubscriptionConfig, receivedAll chan []response_type.SqsMessage, subscription pubsub.Subscription, errChan chan error) {
	// VisibilityTimeout if how long to hold a message for before it returns back to the queue
	// WaitTimeSeconds or read timeout is the max time to wait before <= N messages return
	cancelContext, cancelFunc1 := context.WithCancel(*handler.Context)
	// this is to wait for acks
	timeoutConfig := fmt.Sprintf("%ds", *params.VisibilityTimeout)
	timeoutDuration, _ := time.ParseDuration(timeoutConfig)
	// this is the timeout to read up to N entries.  return to user first of getting N entries or timeout
	waitTimeConfig := fmt.Sprintf("%ds", *params.WaitTimeSeconds)
	waitTimeDuration, _ := time.ParseDuration(waitTimeConfig)
	logging.Log.Debugf("Waiting for '%v', %v", waitTimeDuration, timeoutDuration)
	cctx, cancelFunc := context.WithTimeout(cancelContext, timeoutDuration)
	pieces := strings.Split(*params.QueueUrl, "/")
	id := pieces[len(pieces)-1]
	var lock sync.Mutex
	logging.Log.Debugf("Waiting for 1 on %s %v", id, subscriptionInfo)
	topicPieces := strings.Split(subscriptionInfo.Topic.ID(), "/")
	topic := topicPieces[len(topicPieces)-1]
	datas := make([]response_type.SqsMessage, 0)
	receivedCount := 0
	ackedCount := 0
	continueReading := true
	go func() {
		select {
		case <-time.After(waitTimeDuration):
			lock.Lock()
			continueReading = false
			lock.Unlock()
			receivedAll <- datas
			logging.Log.Debugf("receive window timeout")
		}
	}()
	// waitTimeoutChan := time.After(timeoutDuration)
	go func() {
		err := subscription.Receive(cctx, func(context context.Context, message *pubsub.Message) {
			logging.Log.Debugf("Received %s", message.Data)
			data, err := handler.tryToDecrypt(topic, message.Data)
			if err != nil {
				errChan <- err
				message.Nack()
				cancelFunc()
				cancelFunc1()
				return
			}
			dataString := string(data)
			md5OfBody := fmt.Sprintf("%x", md5.Sum(data))
			logging.Log.Debugf("Need to ack %s", message.ID)
			handler.ToAck[message.ID] = make(chan bool)
			if !continueReading {
				delete(handler.ToAck, message.ID)
				logging.Log.Debugf("Should not continue reading (receive window timeout)")
				message.Nack()
				cancelFunc()
				cancelFunc1()
				return
			}
			lock.Lock()
			datas = append(datas, response_type.SqsMessage{
				MessageId:     &message.ID,
				ReceiptHandle: &message.ID,
				MD5OfBody:     &md5OfBody,
				Body:          &dataString,
			})
			receivedCount++
			lock.Unlock()
			if receivedCount >= subscription.ReceiveSettings.MaxOutstandingMessages {
				logging.Log.Debugf("Received all")
				receivedAll <- datas
			}
			logging.Log.Debugf("Entering select")
			select {
			case isAck := <-handler.ToAck[message.ID]:
				delete(handler.ToAck, message.ID)
				logging.Log.Debugf("Got ack for %s %v", message.ID, isAck)
				if isAck {
					message.Ack()
				} else {
					message.Nack()
				}
				ackedCount++
				if ackedCount >= receivedCount {
					cancelFunc1()
					cancelFunc()
					logging.Log.Debugf("Done waiting for acks")
					return
				}
			case <-time.After(timeoutDuration):
				delete(handler.ToAck, message.ID)
				logging.Log.Debugf("Timeout while waiting for acks")
				message.Nack()
				cancelFunc()
				cancelFunc1()
				return
			}
		})
		logging.Log.Debugf("Exited loop")
		if err != nil {
			errChan <- err
		}
	}()
}

func (handler *Handler) ReceiveHandle(writer http.ResponseWriter, request *http.Request) {
	params, err := handler.ReceiveHandleParseInput(request)
	if err != nil {
		processError(err, writer)
		return
	}
	var response *response_type.ReceiveMessageResponse
	if handler.Config.IsSet("gcp_destination_config") {
		pieces := strings.Split(*params.QueueUrl, "/")
		id := strings.ReplaceAll(pieces[len(pieces)-1], "-", "")
		subscription := handler.GCPClient.Subscription(id)
		subscriptionInfo, err := subscription.Config(*handler.Context)
		if err != nil {
			processError(err, writer)
			return
		}
		maxCount := params.MaxNumberOfMessages
		subscription.ReceiveSettings.MaxOutstandingMessages = int(*maxCount)
		subscription.ReceiveSettings.NumGoroutines = 1
		subscription.ReceiveSettings.Synchronous = true
		receivedAll := make(chan []response_type.SqsMessage)
		errChan := make(chan error)
		handler.gcpReceive(*params, subscriptionInfo, receivedAll, *subscription, errChan)
		select {
		case datas := <-receivedAll:
			response = &response_type.ReceiveMessageResponse{
				XmlNS: response_type.XmlNs,
				ReceiveMessageResult: response_type.ReceiveMessageResult{
					Message: datas,
				},
			}
		case err = <-errChan:
			processError(err, writer)
			return
		}
	} else {
		resp, err := handler.SqsClient.ReceiveMessageRequest(params).Send()
		if err != nil {
			logging.Log.Errorf("Error receiving AWS %s", err)
			processError(err, writer)
			return
		}
		messages := make([]response_type.SqsMessage, len(resp.Messages))
		for i, msg := range resp.Messages {
			attributes := make([]response_type.SqsAttribute, len(msg.Attributes))
			attributeIndex := 0
			for attributeKey, attributeValue := range msg.Attributes {
				attributes[attributeIndex] = response_type.SqsAttribute{
					Name:  attributeKey,
					Value: attributeValue,
				}
			}
			messages[i] = response_type.SqsMessage{
				MessageId:     msg.MessageId,
				ReceiptHandle: msg.ReceiptHandle,
				MD5OfBody:     msg.MD5OfBody,
				Body:          msg.Body,
				Attributes:    attributes,
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
	waitTimeout := r.Form.Get("WaitTimeSeconds")
	input := &sqs.ReceiveMessageInput{
		QueueUrl: &url,
	}
	if maxMessages != "" {
		maxNumber, _ := strconv.ParseInt(maxMessages, 10, 64)
		if maxNumber < 1 {
			return nil, errors.New("max messages cannot be less than 1")
		} else if maxNumber > 10 {
			return nil, errors.New("max messages cannot be greater than 10")
		}
		input.MaxNumberOfMessages = &maxNumber
	} else {
		maxNumber := int64(10)
		input.MaxNumberOfMessages = &maxNumber
	}
	if visibility != "" {
		visibilityNumber, _ := strconv.ParseInt(visibility, 10, 64)
		input.VisibilityTimeout = &visibilityNumber
	} else {
		defaultTimeout := int64(30)
		input.VisibilityTimeout = &defaultTimeout
	}
	if waitTimeout != "" {
		waitNumber, _ := strconv.ParseInt(waitTimeout, 10, 64)
		input.WaitTimeSeconds = &waitNumber
	} else {
		readTimeoutConfig := handler.Config.GetString("gcp_destination_config.pub_sub_config.read_timeout")
		readTimeoutDuration, _ := time.ParseDuration(readTimeoutConfig)
		readTimeoutSeconds := int64(readTimeoutDuration.Seconds())
		input.WaitTimeSeconds = &readTimeoutSeconds
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

func (handler *Handler) DeleteMessageHandle(writer http.ResponseWriter, request *http.Request) {
	params, err := handler.DeleteMessageHandleParseInput(request)
	if err != nil {
		processError(err, writer)
		return
	}
	var response *response_type.DeleteMessageResponse
	if handler.Config.IsSet("gcp_destination_config") {
		if handler.ToAck[*params.ReceiptHandle] != nil {
			handler.ToAck[*params.ReceiptHandle] <- true
		}
		response = &response_type.DeleteMessageResponse{}
	} else {
		_, err := handler.SqsClient.DeleteMessageRequest(params).Send()
		if err != nil {
			logging.Log.Errorf("Error deleting queue AWS %s", err)
			processError(err, writer)
			return
		}
		response = &response_type.DeleteMessageResponse{}
	}
	output, _ := xml.Marshal(response)
	logging.Log.Debugf("Writing %s", string(output))
	writer.Write([]byte(response_type.XmlHeader))
	writer.Write([]byte(string(output)))
}
func (handler *Handler) DeleteMessageHandleParseInput(r *http.Request) (*sqs.DeleteMessageInput, error) {
	url := r.Form.Get("QueueUrl")
	receipt := r.Form.Get("ReceiptHandle")
	return &sqs.DeleteMessageInput{
		QueueUrl:      &url,
		ReceiptHandle: &receipt,
	}, nil
}

func (handler *Handler) DeleteMessageBatchHandle(writer http.ResponseWriter, request *http.Request) {
	params, err := handler.DeleteMessageBatchHandleParseInput(request)
	if err != nil {
		processError(err, writer)
		return
	}
	var response *response_type.DeleteMessageBatchResponse
	if handler.Config.IsSet("gcp_destination_config") {
		response = &response_type.DeleteMessageBatchResponse{}
		success := make([]response_type.DeleteMessageBatchResultEntry, 0)
		for _, entry := range params.Entries {
			if handler.ToAck[*entry.ReceiptHandle] != nil {
				handler.ToAck[*entry.ReceiptHandle] <- true
				success = append(success, response_type.DeleteMessageBatchResultEntry{
					Id: entry.Id,
				})
			}
		}
		response.DeleteMessageBatchResult = response_type.DeleteMessageBatchResult{
			DeleteMessageBatchResultEntry: success,
		}
	} else {
		resp, err := handler.SqsClient.DeleteMessageBatchRequest(params).Send()
		if err != nil {
			logging.Log.Errorf("Error deleting queue AWS %s", err)
			processError(err, writer)
			return
		}

		response = &response_type.DeleteMessageBatchResponse{}
		entries := make([]response_type.DeleteMessageBatchResultEntry, len(resp.Successful))
		for i, item := range resp.Successful {
			entries[i] = response_type.DeleteMessageBatchResultEntry{
				Id: item.Id,
			}
		}
		response.DeleteMessageBatchResult = response_type.DeleteMessageBatchResult{
			DeleteMessageBatchResultEntry: entries,
		}
	}
	output, _ := xml.Marshal(response)
	logging.Log.Debugf("Writing %s", string(output))
	writer.Write([]byte(response_type.XmlHeader))
	writer.Write([]byte(string(output)))
}
func (handler *Handler) DeleteMessageBatchHandleParseInput(r *http.Request) (*sqs.DeleteMessageBatchInput, error) {
	url := r.Form.Get("QueueUrl")
	messageAttributes := make([]sqs.DeleteMessageBatchRequestEntry, 0)
	input := &sqs.DeleteMessageBatchInput{
		QueueUrl: &url,
	}
	for key, formValue := range r.Form {
		if strings.HasPrefix(key, "DeleteMessageBatchRequestEntry") && strings.Contains(key, "Id") {
			keyValue := formValue[0]
			receiptKey := strings.Replace(key, "Id", "ReceiptHandle", 1)
			receiptValue := r.Form.Get(receiptKey)
			messageAttributes = append(messageAttributes, sqs.DeleteMessageBatchRequestEntry{
				Id:            &keyValue,
				ReceiptHandle: &receiptValue,
			})
		}
	}
	if len(messageAttributes) > 0 {
		input.Entries = messageAttributes
	}
	return input, nil
}

func (handler *Handler) gcpPublish(topic kinesis.GCPTopic, topicName string, message *pubsub.Message) (kinesis.GCPPublishResult, error) {
	keyMap := handler.Config.GetStringMapString("gcp_destination_config.pub_sub_config.topic_kms_map")
	logging.Log.Debugf("Found keymap looking for %s %s", keyMap, topicName)
	kvmKey := keyMap[topicName]
	if kvmKey != "" {
		req := &kmsproto.EncryptRequest{
			Name:      kvmKey,
			Plaintext: message.Data,
		}
		resp, err := handler.GCPKMSClient.Encrypt(*handler.Context, req)
		if err != nil {
			return nil, err
		} else {
			message.Data = resp.Ciphertext
		}
	}
	return handler.GCPResultWrapper(topic.Publish(*handler.Context, message)), nil
}

func (handler *Handler) tryToDecrypt(topicName string, message []byte) ([]byte, error) {
	keyMap := handler.Config.GetStringMapString("gcp_destination_config.pub_sub_config.topic_kms_map")
	logging.Log.Debugf("Found keymap looking for %s %s", keyMap, topicName)
	kvmKey := keyMap[topicName]
	if kvmKey != "" {
		req := &kmsproto.DecryptRequest{
			Name:       kvmKey,
			Ciphertext: message,
		}
		resp, err := handler.GCPKMSClient.Decrypt(*handler.Context, req)
		if err != nil {
			return nil, err
		} else {
			return resp.Plaintext, nil
		}
	}
	return message, nil
}
