package kinesis

import (
	"cloud.google.com/go/pubsub"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/gorilla/mux"
	"net/http"
	"sidecar/pkg/logging"
	"sidecar/pkg/response_type"
	"sync"
)

type KinesisHandler struct {
	*Handler
}

type Kinesis interface {
	PublishHandle(writer http.ResponseWriter, request *http.Request)
	PublishParseInput(r *http.Request) (*response_type.KinesisRequest, error)
	Register(mux *mux.Router)
	New(handler *Handler) *KinesisHandler
}

func New(handler *Handler) *KinesisHandler {
	return &KinesisHandler{Handler: handler}
}

func (handler *KinesisHandler) Register(mux *mux.Router) {
	mux.HandleFunc("/", handler.PublishHandle).Methods("POST")
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
	}
	gcpShardId := "shard-0"
	if payload.Data != "" {
		str, _ := base64.StdEncoding.DecodeString(payload.Data)
		if handler.GCPClient != nil {
			response, err := handler.GCPClient.Topic(payload.StreamName).Publish(*handler.Context, &pubsub.Message{
				Data: str,
			}).Get(*handler.Context)
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
			results := make([]*pubsub.PublishResult, len(payload.Records))
			var wg sync.WaitGroup
			wg.Add(len(payload.Records))
			for i, record := range payload.Records {
				str, _ := base64.StdEncoding.DecodeString(record.Data)
				results[i] = handler.GCPClient.Topic(payload.StreamName).Publish(*handler.Context, &pubsub.Message{
					Data: str,
				})
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
