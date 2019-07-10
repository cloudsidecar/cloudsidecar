package kinesis

import (
	"cloud.google.com/go/pubsub"
	"cloudsidecar/pkg/logging"
	"cloudsidecar/pkg/response_type"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"google.golang.org/genproto/googleapis/cloud/kms/v1"
	"net/http"
	"strings"
	"sync"
	"time"
)

type KinesisHandler struct {
	*Handler

}

const GcpShardId string = "shard-0"
const GcpPartitionKey string = "partitionKey"
const GetRecordCountLimit int = 10000

type Kinesis interface {
	StartStreamEncryptionParseInput(r *http.Request) (*kinesis.StartStreamEncryptionInput, error)
	StartStreamEncryptionHandle(writer http.ResponseWriter, request *http.Request)
	GetRecordsParseInput(r *http.Request) (*kinesis.GetRecordsInput, error)
	GetRecordsHandle(writer http.ResponseWriter, request *http.Request)
	GetShardIteratorParseInput(r *http.Request) (*kinesis.GetShardIteratorInput, error)
	GetShardIteratorHandle(writer http.ResponseWriter, request *http.Request)
	DescribeParseInput(r *http.Request) (*kinesis.DescribeStreamInput, error)
	DescribeHandle(writer http.ResponseWriter, request *http.Request)
	PublishHandle(writer http.ResponseWriter, request *http.Request)
	PublishParseInput(r *http.Request) (*response_type.KinesisRequest, error)
	CreateStreamHandle(writer http.ResponseWriter, request *http.Request)
	CreateStreamParseInput(r *http.Request) (*kinesis.CreateStreamInput, error)
	DeleteStreamHandle(writer http.ResponseWriter, request *http.Request)
	DeleteStreamParseInput(r *http.Request) (*kinesis.DeleteStreamInput, error)
	Register(mux *mux.Router)
	Handle(writer http.ResponseWriter, request *http.Request)
	New(handler *Handler) *KinesisHandler
}

func New(handler *Handler) *KinesisHandler {
	return &KinesisHandler{Handler: handler}
}

func (handler *KinesisHandler) Register(mux *mux.Router) {
	mux.HandleFunc("/", handler.Handle).Methods("POST")
}

func (handler *KinesisHandler) Handle(writer http.ResponseWriter, request *http.Request) {
	targetHeader := request.Header.Get("X-Amz-Target")
	targetSplit := strings.SplitN(targetHeader, ".", 2)
	targetFunction := strings.ToLower(targetSplit[1])
	if targetFunction == "createstream" {
		handler.CreateStreamHandle(writer, request)
	} else if targetFunction == "putrecord" || targetFunction == "putrecords" {
		handler.PublishHandle(writer, request)
	} else if targetFunction == "deletestream" {
		handler.DeleteStreamHandle(writer, request)
	} else if targetFunction == "describestream" {
		handler.DescribeHandle(writer, request)
	} else if targetFunction == "getsharditerator" {
		handler.GetShardIteratorHandle(writer, request)
	} else if targetFunction == "getrecords" {
		handler.GetRecordsHandle(writer, request)
	} else {
		logging.Log.Errorf("Func not found %s", targetFunction)
		writer.WriteHeader(400)
	}
}

func (handler *KinesisHandler) StartStreamEncryptionParseInput(r *http.Request) (*kinesis.StartStreamEncryptionInput, error) {
	decoder := json.NewDecoder(r.Body)
	var payload kinesis.StartStreamEncryptionInput
	var err error
	err = decoder.Decode(&payload)
	if err != nil {
		logging.Log.Error("Error reading start stream encryption kinesis payload", err)
	}
	return &payload, err
}
func (handler *KinesisHandler) StartStreamEncryptionHandle(writer http.ResponseWriter, request *http.Request) {
	payload, err := handler.StartStreamEncryptionParseInput(request)
	if err != nil {
		writer.WriteHeader(400)
		writer.Write([]byte(fmt.Sprint(err)))
		return
	}
	if handler.GCPClient != nil {

	} else {
		_, err := handler.KinesisClient.StartStreamEncryptionRequest(payload).Send()
		if err != nil {
			writer.WriteHeader(400)
			writer.Write([]byte(fmt.Sprint(err)))
			return
		}
	}
	writer.WriteHeader(200)
}

func (handler *KinesisHandler) tryToDecrypt(topicName string, message []byte) ([]byte, error) {
	keyMap := handler.Config.GetStringMapString("gcp_destination_config.pub_sub_config.topic_kms_map")
	logging.Log.Debugf("Found keymap looking for %s %s", keyMap, topicName)
	kvmKey := keyMap[topicName]
	if kvmKey != "" {
		req := &kms.DecryptRequest{
			Name: kvmKey,
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

func (handler *KinesisHandler) GetRecordsParseInput(r *http.Request) (*kinesis.GetRecordsInput, error) {
	decoder := json.NewDecoder(r.Body)
	var payload kinesis.GetRecordsInput
	var err error
	err = decoder.Decode(&payload)
	if err != nil {
		logging.Log.Error("Error reading get records kinesis payload", err)
	}
	return &payload, err
}

func (handler *KinesisHandler) GetRecordsHandle(writer http.ResponseWriter, request *http.Request) {
	payload, err := handler.GetRecordsParseInput(request)
	if err != nil {
		writer.WriteHeader(400)
		writer.Write([]byte(fmt.Sprint(err)))
		return
	}
	var output *kinesis.GetRecordsOutput
	if handler.GCPClient != nil {
		cancelContext, cancel1 := context.WithCancel(*handler.Context)
		timeoutConfig := handler.Config.GetString("gcp_destination_config.pub_sub_config.read_timeout")
		if timeoutConfig == "" {
			timeoutConfig = "5s"
		}
		timeoutDuration, _ := time.ParseDuration(timeoutConfig)
		cctx, cancel := context.WithTimeout(cancelContext, timeoutDuration)

		maxCount := int64(GetRecordCountLimit)
		if payload.Limit != nil {
			if *payload.Limit > int64(GetRecordCountLimit) {
				writer.WriteHeader(400)
				writer.Write([]byte(fmt.Sprintf("Payload must be less than %d", GetRecordCountLimit)))
				return
			}
			maxCount = *payload.Limit
		}
		output = &kinesis.GetRecordsOutput{
			NextShardIterator: payload.ShardIterator,
		}
		records := make([]kinesis.Record, maxCount)
		var lock sync.Mutex
		readCount := int64(0)
		gcpPartitionKey := GcpPartitionKey
		logging.Log.Debugf("Waiting maxcount %d", maxCount)
		subscription := handler.GCPClient.Subscription(*payload.ShardIterator)
		subscriptionInfo, err := subscription.Config(*handler.Context)
		if err != nil {
			writer.WriteHeader(400)
			writer.Write([]byte(fmt.Sprint(err)))
			return
		}
		topicPieces := strings.Split(subscriptionInfo.Topic.ID(), "/")
		topic := topicPieces[len(topicPieces) - 1]
		subscription.ReceiveSettings.MaxOutstandingMessages = int(maxCount)
		subscription.ReceiveSettings.NumGoroutines = 1
		subscription.ReceiveSettings.Synchronous = true
		err = subscription.Receive(cctx, func(context context.Context, message *pubsub.Message) {
			lock.Lock()
			defer lock.Unlock()
			logging.Log.Debugf("Received %s", message.Data)
			if readCount >= maxCount {
				message.Nack()
				logging.Log.Debugf("Skipping message over max")
			} else {
				record := kinesis.Record{
					PartitionKey: &gcpPartitionKey,
					SequenceNumber: &message.ID,
					Data: message.Data,
				}
				data, err := handler.tryToDecrypt(topic, message.Data)
				if err != nil {
					writer.WriteHeader(400)
					writer.Write([]byte(fmt.Sprint(err)))
					return
				}
				record.Data = data
				records[readCount] = record
				message.Ack()
			}
			readCount ++
			if readCount >= maxCount {
				logging.Log.Debugf("Canceling %d", readCount)
				cancel()
				cancel1()
			}
			logging.Log.Debugf("Read %d", readCount)
		})
		if readCount > maxCount {
			readCount = maxCount
		}
		records = records[:readCount]
		if err != nil  && !strings.Contains(err.Error(), "context canceled"){
			writer.WriteHeader(400)
			writer.Write([]byte(fmt.Sprint(err)))
			return
		}
		output = &kinesis.GetRecordsOutput{
			NextShardIterator: payload.ShardIterator,
			Records: records,
		}
	} else {
		resp, err := handler.KinesisClient.GetRecordsRequest(payload).Send()
		if err != nil {
			writer.WriteHeader(400)
			writer.Write([]byte(fmt.Sprint(err)))
			return
		}
		output = resp
	}
	json.NewEncoder(writer).Encode(*output)
}

func (handler *KinesisHandler) GetShardIteratorParseInput(r *http.Request) (*kinesis.GetShardIteratorInput, error) {
	decoder := json.NewDecoder(r.Body)
	var payload kinesis.GetShardIteratorInput
	var err error
	err = decoder.Decode(&payload)
	if err != nil {
		logging.Log.Error("Error reading get shard iterator kinesis payload", err)
	}
	return &payload, err

}
func (handler *KinesisHandler) GetShardIteratorHandle(writer http.ResponseWriter, request *http.Request) {
	payload, err := handler.GetShardIteratorParseInput(request)
	if err != nil {
		writer.WriteHeader(400)
		writer.Write([]byte(fmt.Sprint(err)))
		return
	}
	var output *kinesis.GetShardIteratorOutput
	if handler.GCPClient != nil {
		if payload.ShardIteratorType != kinesis.ShardIteratorTypeLatest {
			writer.WriteHeader(400)
			writer.Write([]byte("Only support shard iterator latest"))
			return
		}
		id := strings.ReplaceAll(uuid.New().String(), "-", "")
		id = fmt.Sprintf("gcp%s", id)
		topic := handler.GCPClient.Topic(*payload.StreamName)
		ackDuration, _ := time.ParseDuration("1m")
		retentionDuration, _ := time.ParseDuration("1d")
		_, err := handler.GCPClient.CreateSubscription(*handler.Context, id, pubsub.SubscriptionConfig{
			Topic: topic,
			AckDeadline: ackDuration,
			RetentionDuration: retentionDuration,
		})
		if err != nil {
			logging.Log.Errorf("Error creating subscription %s", err)
			writer.WriteHeader(400)
			writer.Write([]byte(fmt.Sprint(err)))
			return
		}
		output = &kinesis.GetShardIteratorOutput{
			ShardIterator: &id,
		}
	} else {
		resp, err := handler.KinesisClient.GetShardIteratorRequest(payload).Send()
		if err != nil {
			writer.WriteHeader(400)
			writer.Write([]byte(fmt.Sprint(err)))
			return
		}
		output = resp
	}
	json.NewEncoder(writer).Encode(*output)
}

func (handler *KinesisHandler) DescribeParseInput(r *http.Request) (*kinesis.DescribeStreamInput, error) {
	decoder := json.NewDecoder(r.Body)
	var payload kinesis.DescribeStreamInput
	var err error
	err = decoder.Decode(&payload)
	if err != nil {
		logging.Log.Error("Error reading describe kinesis payload", err)
	}
	return &payload, err
}

func (handler *KinesisHandler) DescribeHandle(writer http.ResponseWriter, request *http.Request) {
	payload, err := handler.DescribeParseInput(request)
	if err != nil {
		writer.WriteHeader(400)
		writer.Write([]byte(fmt.Sprint(err)))
		return
	}
	var output *kinesis.DescribeStreamOutput
	if handler.GCPClient != nil {
		_, err := handler.GCPClientToTopic(*payload.StreamName, handler.GCPClient).Config(*handler.Context)
		if err != nil {
			writer.WriteHeader(400)
			writer.Write([]byte(fmt.Sprint(err)))
			return
		}
		falseValue := false
		retentionPeriod := int64(24)
		gcpShardId := GcpShardId
		startHashKey := "0"
		endHashKey := "340282366920938463463374607431768211455"
		hashRange := &kinesis.HashKeyRange{
			StartingHashKey: &startHashKey,
			EndingHashKey: &endHashKey,
		}
		seqRange := &kinesis.SequenceNumberRange{
			StartingSequenceNumber: &startHashKey,
		}
		shards := []kinesis.Shard{
			{
				ShardId: &gcpShardId,
				HashKeyRange: hashRange,
				SequenceNumberRange: seqRange,
			},
		}
		output = &kinesis.DescribeStreamOutput{
			StreamDescription: &kinesis.StreamDescription{
				HasMoreShards: &falseValue,
				RetentionPeriodHours: &retentionPeriod,
				StreamName: payload.StreamName,
				StreamStatus: kinesis.StreamStatusActive,
				Shards: shards,
			},
		}
	} else {
		req := handler.KinesisClient.DescribeStreamRequest(payload)
		resp, err := req.Send()
		if err != nil {
			writer.WriteHeader(400)
			writer.Write([]byte(fmt.Sprint(err)))
			return
		}
		output = resp
	}
	json.NewEncoder(writer).Encode(*output)
}

func (handler *KinesisHandler) DeleteStreamHandle(writer http.ResponseWriter, request *http.Request) {
	payload, err := handler.DeleteStreamParseInput(request)
	if err != nil {
		writer.WriteHeader(400)
		writer.Write([]byte(fmt.Sprint(err)))
		return
	}
	if handler.GCPClient != nil {
		err := handler.GCPClientToTopic(*payload.StreamName, handler.GCPClient).Delete(*handler.Context)
		if err != nil {
			logging.Log.Error("Error Deleting", err)
			writer.WriteHeader(400)
			writer.Write([]byte(fmt.Sprint(err)))
			return
		}
	} else {
		req := handler.KinesisClient.DeleteStreamRequest(payload)
		_, err := req.Send()
		if err != nil {
			logging.Log.Error("Error Deleting", err)
			writer.WriteHeader(400)
			writer.Write([]byte(fmt.Sprint(err)))
			return
		}
	}
	writer.WriteHeader(200)
}
func (handler *KinesisHandler) DeleteStreamParseInput(r *http.Request) (*kinesis.DeleteStreamInput, error) {
	decoder := json.NewDecoder(r.Body)
	var payload kinesis.DeleteStreamInput
	var err error
	err = decoder.Decode(&payload)
	if err != nil {
		logging.Log.Error("Error reading kinesis payload", err)
	}
	return &payload, err
}

func (handler *KinesisHandler) CreateStreamParseInput(r *http.Request) (*kinesis.CreateStreamInput, error) {
	decoder := json.NewDecoder(r.Body)
	var payload kinesis.CreateStreamInput
	var err error
	err = decoder.Decode(&payload)
	if err != nil {
		logging.Log.Error("Error reading kinesis payload", err)
	}
	return &payload, err
}

func (handler *KinesisHandler) CreateStreamHandle(writer http.ResponseWriter, request *http.Request) {
	payload, err := handler.CreateStreamParseInput(request)
	if err != nil {
		writer.WriteHeader(400)
		writer.Write([]byte(fmt.Sprint(err)))
		return
	}
	if handler.GCPClient != nil {
		_, err := handler.GCPClient.CreateTopic(*handler.Context, *payload.StreamName)
		if err != nil {
			logging.Log.Error("Error creating", err)
			writer.WriteHeader(400)
			writer.Write([]byte(fmt.Sprint(err)))
			return
		}

	} else {
		req := handler.KinesisClient.CreateStreamRequest(payload)
		_, err := req.Send()
		if err != nil {
			logging.Log.Error("Error creating", err)
			writer.WriteHeader(400)
			writer.Write([]byte(fmt.Sprint(err)))
			return
		}
	}
	writer.WriteHeader(200)
}

func (handler *KinesisHandler) gcpPublish(topic GCPTopic, topicName string, message *pubsub.Message) (GCPPublishResult, error) {
	keyMap := handler.Config.GetStringMapString("gcp_destination_config.pub_sub_config.topic_kms_map")
	logging.Log.Debugf("Found keymap looking for %s %s", keyMap, topicName)
	kvmKey := keyMap[topicName]
	if kvmKey != "" {
		req := &kms.EncryptRequest{
			Name: kvmKey,
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

func (handler *KinesisHandler) PublishParseInput(r *http.Request) (*response_type.KinesisRequest, error) {
	decoder := json.NewDecoder(r.Body)
	var payload response_type.KinesisRequest
	var err error
	err = decoder.Decode(&payload)
	if err != nil {
		logging.Log.Error("Error reading kinesis payload", err)
	}
	return &payload, err
}

func (handler *KinesisHandler) PublishHandle(writer http.ResponseWriter, request *http.Request) {
	payload, err := handler.PublishParseInput(request)
	if err != nil {
		writer.WriteHeader(400)
		writer.Write([]byte(fmt.Sprint(err)))
		return
	}
	gcpShardId := GcpShardId
	if payload.Data != "" {
		str, _ := base64.StdEncoding.DecodeString(payload.Data)
		if handler.GCPClient != nil {
			topic := handler.GCPClientToTopic(payload.StreamName, handler.GCPClient)
			defer topic.Stop()
			req, err := handler.gcpPublish(topic, payload.StreamName, &pubsub.Message{
				Data: str,
			})
			if err != nil {
				logging.Log.Error("Error sending", err)
				writer.WriteHeader(400)
				writer.Write([]byte(fmt.Sprint(err)))
				return
			}
			response, err := req.Get(*handler.Context)
			if err != nil {
				logging.Log.Error("Error sending", err)
				writer.WriteHeader(400)
				writer.Write([]byte(fmt.Sprint(err)))
				return
			}
			jsonOutput := response_type.KinesisResponse{
				SequenceNumber: &response,
				ShardId: &gcpShardId,
			}
			json.NewEncoder(writer).Encode(jsonOutput)

		} else {
			req := handler.KinesisClient.PutRecordRequest(&kinesis.PutRecordInput{
				Data: str,
				PartitionKey: &payload.PartitionKey,
				StreamName: &payload.StreamName,
			})
			output, err := req.Send()
			if err != nil {
				logging.Log.Error("Error sending", err)
				writer.WriteHeader(400)
				writer.Write([]byte(fmt.Sprint(err)))
				return
			}
			jsonOutput := response_type.KinesisResponse{
				SequenceNumber: output.SequenceNumber,
				ShardId: output.ShardId,
			}
			json.NewEncoder(writer).Encode(jsonOutput)
		}
	} else if len(payload.Records) > 0 {
		if handler.GCPClient != nil {
			results := make([]GCPPublishResult, len(payload.Records))
			var wg sync.WaitGroup
			wg.Add(len(payload.Records))
			topic := handler.GCPClientToTopic(payload.StreamName, handler.GCPClient)
			defer topic.Stop()
			for i, record := range payload.Records {
				str, _ := base64.StdEncoding.DecodeString(record.Data)
				req, err := handler.gcpPublish(topic, payload.StreamName, &pubsub.Message{
					Data: str,
				})
				if err != nil {
					logging.Log.Error("Error sending", err)
					writer.WriteHeader(400)
					writer.Write([]byte(fmt.Sprint(err)))
					return
				}
				results[i] = req
				go func (i int, c <-chan struct{}) {
					<- c
					wg.Done()
				}(i, results[i].Ready())
			}
			wg.Wait()
			failedCount := int64(0)
			records := make([]response_type.KinesisResponse, len(results))
			for i, result := range results {
				serverId, err := result.Get(*handler.Context)
				if err != nil {
					var errorCode = "ERROR"
					var errorMessage = err.Error()
					records[i] = response_type.KinesisResponse{
						ErrorCode: &errorCode,
						ErrorMessage: &errorMessage,
					}
					failedCount += 1
				} else {
					records[i] = response_type.KinesisResponse{
						ShardId: &gcpShardId,
						SequenceNumber: &serverId,
					}
				}
			}
			jsonOutput := response_type.KinesisRecordsResponse{
				FailedRequestCount: failedCount,
				Records: records,
			}
			json.NewEncoder(writer).Encode(jsonOutput)

		} else {
			input := kinesis.PutRecordsInput{
				StreamName: &payload.StreamName,
				Records: make([]kinesis.PutRecordsRequestEntry, len(payload.Records)),
			}
			for i, record := range payload.Records {
				str, _ := base64.StdEncoding.DecodeString(record.Data)
				key := record.PartitionKey
				input.Records[i] = kinesis.PutRecordsRequestEntry{
					Data: str,
					PartitionKey: &key,
				}
			}
			req := handler.KinesisClient.PutRecordsRequest(&input)
			output, err := req.Send()
			if err != nil {
				logging.Log.Error("Error sending", err)
				writer.WriteHeader(400)
				writer.Write([]byte(fmt.Sprint(err)))
				return
			}
			jsonOutput := response_type.KinesisRecordsResponse{
				FailedRequestCount: *output.FailedRecordCount,
				Records: make([]response_type.KinesisResponse, len(output.Records)),
			}
			for i, record := range output.Records {
				if record.ErrorCode != nil && *record.ErrorCode != "" {
					jsonOutput.Records[i] = response_type.KinesisResponse{
						ErrorCode:    record.ErrorCode,
						ErrorMessage: record.ErrorMessage,
					}
				} else {
					jsonOutput.Records[i] = response_type.KinesisResponse{
						SequenceNumber: record.SequenceNumber,
						ShardId: record.ShardId,
					}
				}
			}
			json.NewEncoder(writer).Encode(jsonOutput)

		}
	} else {
		logging.Log.Error("Missing data")
		writer.WriteHeader(400)
	}
}
