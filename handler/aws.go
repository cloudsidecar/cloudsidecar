package handler

import (
	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/datastore"
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"net/http"
	"sidecar/config"
	"sidecar/converter"
	"sidecar/response_type"
	"strings"
	"sync"
)

type S3Handler struct {
	S3Client *s3.S3
	GCPClient *storage.Client
	Context *context.Context
	GCSConfig *config.GCSConfig
}

type KinesisHandler struct {
	KinesisClient *kinesis.Kinesis
	GCPClient *pubsub.Client
	Context *context.Context
}

type DynamoDBHandler struct {
	DynamoClient      *dynamodb.DynamoDB
	GCPBigTableClient *bigtable.Client
	GCPDatastoreClient *datastore.Client
	Context           *context.Context
	GCPDatastoreConfig *config.GCPDatastoreConfig
}


func (handler DynamoDBHandler) DynamoOperation(writer http.ResponseWriter, request *http.Request) {
	targetHeader := request.Header.Get("X-Amz-Target")
	targetSplit := strings.SplitN(targetHeader, ".", 2)
	targetFunction := strings.ToLower(targetSplit[1])
	if targetFunction == "scan" {
		handler.DynamoScan(writer, request)
	} else if targetFunction == "getitem" {
		handler.DynamoGetItem(writer, request)
	} else if targetFunction == "query" {
		handler.DynamoQuery(writer, request)
	}
}

func contains(list []interface {}, item interface {}) bool {
	for _, arrItem := range list {
		if arrItem == item {
			return true
		}
	}
	return false
}

func (handler DynamoDBHandler) DynamoQuery(writer http.ResponseWriter, request *http.Request) {
	decoder := json.NewDecoder(request.Body)
	var input dynamodb.QueryInput
	err := decoder.Decode(&input)
	if err != nil {
		writer.WriteHeader(400)
		fmt.Println("Error with decoding input", err)
		write(fmt.Sprint("Error decoding input ", err), &writer)
		return
	}
	var resp *dynamodb.QueryOutput
	if handler.GCPDatastoreClient != nil {
		query, inFilters, _ := converter.AWSQueryToGCPDatastoreQuery(&input)
		fmt.Println(query)
		var items []response_type.Map
		keys, err := handler.GCPDatastoreClient.GetAll(*handler.Context, query, &items)
		fmt.Println(err)
		fmt.Println(keys)
		length := int64(len(keys))
		responseItems := make([]map[string]dynamodb.AttributeValue, length)
		fmt.Println(handler.GCPDatastoreConfig.TableKeyNameMap)
		keyFieldName := handler.GCPDatastoreConfig.TableKeyNameMap[*input.TableName]
		filteredItems := make([]response_type.Map, 0)
		for i, item := range items {
			for filterKey, filterValues := range inFilters {
				if filterKey != keyFieldName {
					if contains(filterValues, item[filterKey]) {
						filteredItems = append(filteredItems, item)
					}
				} else {
					if contains(filterValues, keys[i].Name){

						filteredItems = append(filteredItems, item)
					}
				}
			}
		}
		if len(inFilters) != 0 {
			fmt.Println("BOOOYA")
			items = filteredItems
		}
		for i, item := range items {
			responseItems[i] = make(map[string]dynamodb.AttributeValue)
			for fieldName, field := range item {
				responseItems[i][fieldName] = converter.ValueToAWS(field)
			}
			responseItems[i][keyFieldName] = converter.ValueToAWS(keys[i].Name)
		}
		length = int64(len(items))
		resp = &dynamodb.QueryOutput{
			Count: &length,
			Items: responseItems,
		}

	} else {
		resp, err = handler.DynamoClient.QueryRequest(&input).Send()
	}
	if err != nil {
		writer.WriteHeader(400)
		fmt.Println("Error", err)
		write(fmt.Sprint("Error", err), &writer)
		return
	}
	fmt.Println(resp)
	fmt.Println(input)
	json.NewEncoder(writer).Encode(resp)

}

func (handler DynamoDBHandler) DynamoGetItem(writer http.ResponseWriter, request *http.Request) {
	decoder := json.NewDecoder(request.Body)
	var input dynamodb.GetItemInput
	decodeErr := decoder.Decode(&input)
	if decodeErr != nil {
		writer.WriteHeader(400)
		fmt.Println("Error with decoding input", decodeErr)
		write(fmt.Sprint("Error decoding input ", decodeErr), &writer)
		return
	}
	var resp *dynamodb.GetItemOutput
	var err error
	if handler.GCPBigTableClient != nil {
		// BS CODE
		/*
		muts := make([]*bigtable.Mutation, 2)
		rowKeys := make([]string, 2)
		muts[0] = bigtable.NewMutation()
		muts[0].Set("a", "name", bigtable.Now(), []byte("larry"))
		muts[0].Set("a", "age", bigtable.Now(), []byte("23"))
		muts[1] = bigtable.NewMutation()
		muts[1].Set("a", "name", bigtable.Now(), []byte("fart"))
		muts[1].Set("a", "age", bigtable.Now(), []byte("50"))
		rowKeys[0] = "boo"
		rowKeys[1] = "larrykins"
		errors, error := handler.GCPBigTableClient.Open(*input.TableName).ApplyBulk(*handler.Context, rowKeys, muts)
		fmt.Println(errors)
		fmt.Println(error)
		*/
		// BS DONE
		columnValue, err := converter.GCPBigTableGetById(&input)
		row, err := handler.GCPBigTableClient.Open(*input.TableName).ReadRow(*handler.Context, columnValue)
		if err != nil {
			writer.WriteHeader(400)
			fmt.Println("Error with decoding input", err)
			write(fmt.Sprint("Error decoding input ", err), &writer)
			return
		}
		resp, err = converter.GCPBigTableResponseToAWS(&row)
	} else if handler.GCPDatastoreClient != nil {
		columnValue, _ := converter.GCPBigTableGetById(&input)
		result := make(response_type.Map)
		key := datastore.Key{
			Kind: *input.TableName,
			Name: columnValue,
		}
		fmt.Println("KEY ", key, " RESULT ", result == nil)
		err := handler.GCPDatastoreClient.Get(
			*handler.Context,
			&key,
			result,
		)
		if err != nil {
			writer.WriteHeader(400)
			fmt.Println("Error getting", err)
			write(fmt.Sprint("Error getting", err), &writer)
			return
		}
		resp, err = converter.GCPDatastoreMapToAWS(result)
	} else {
		resp, err = handler.DynamoClient.GetItemRequest(&input).Send()
		if err != nil {
			writer.WriteHeader(400)
			fmt.Println("Error", err)
			write(fmt.Sprint("Error", err), &writer)
			return
		}
	}
	fmt.Println(resp)
	fmt.Println(input)
	json.NewEncoder(writer).Encode(resp)
}

func (handler DynamoDBHandler) DynamoScan(writer http.ResponseWriter, request *http.Request) {
	decoder := json.NewDecoder(request.Body)
	var input dynamodb.ScanInput
	err := decoder.Decode(&input)
	if err != nil {
		writer.WriteHeader(400)
		fmt.Println("Error with decoding input", err)
		write(fmt.Sprint("Error decoding input ", err), &writer)
		return
	}
	var resp *dynamodb.ScanOutput
	if handler.GCPDatastoreClient != nil {
		query, inFilters, _ := converter.AWSScanToGCPDatastoreQuery(&input)
		fmt.Println(query)
		var items []response_type.Map
		keys, err := handler.GCPDatastoreClient.GetAll(*handler.Context, query, &items)
		fmt.Println(keys, err)
		length := int64(len(keys))
		responseItems := make([]map[string]dynamodb.AttributeValue, length)
		keyFieldName := handler.GCPDatastoreConfig.TableKeyNameMap[*input.TableName]
		filteredItems := make([]response_type.Map, 0)
		filteredKeys := make([]*datastore.Key, 0)
		for i, item := range items {
			matchCount := 0
			for filterKey, filterValues := range inFilters {
				fmt.Println("Filter key", filterKey)
				if filterKey != keyFieldName {
					fmt.Println("in filter not on key")
					if contains(filterValues, item[filterKey]) {
						matchCount ++
					}
				} else {
					fmt.Println("in filter on key")
					if contains(filterValues, keys[i].Name){
						matchCount ++
					}
				}
			}
			if matchCount == len(inFilters) {
				filteredItems = append(filteredItems, item)
				filteredKeys = append(filteredKeys, keys[i])

			}
		}
		if len(inFilters) != 0 {
			items = filteredItems
			responseItems = make([]map[string]dynamodb.AttributeValue, len(items))
			keys = filteredKeys
		}
		for i, item := range items {
			responseItems[i] = make(map[string]dynamodb.AttributeValue)
			for fieldName, field := range item {
				responseItems[i][fieldName] = converter.ValueToAWS(field)
				responseItems[i][keyFieldName] = converter.ValueToAWS(keys[i].Name)
			}
		}
		length = int64(len(items))
		resp = &dynamodb.ScanOutput{
			Count: &length,
			Items: responseItems,
		}

	} else {
		resp, err = handler.DynamoClient.ScanRequest(&input).Send()
		if err != nil {
			writer.WriteHeader(400)
			fmt.Println("Error", err)
			write(fmt.Sprint("Error", err), &writer)
			return
		}
	}
	fmt.Println(resp)
	fmt.Println(input)
	json.NewEncoder(writer).Encode(resp)

}

func (handler KinesisHandler) KinesisPublish(writer http.ResponseWriter, request *http.Request) {
	decoder := json.NewDecoder(request.Body)
	var payload response_type.KinesisRequest
	var err error
	err = decoder.Decode(&payload)
	if err != nil {
		fmt.Println("Error reading kinesis payload", err)
	}
	gcpShardId := "shard-0"
	if payload.Data != "" {
		str, _ := base64.StdEncoding.DecodeString(payload.Data)
		if handler.GCPClient != nil {
			response, err := handler.GCPClient.Topic(payload.StreamName).Publish(*handler.Context, &pubsub.Message{
				Data: str,
			}).Get(*handler.Context)
			if err != nil {
				fmt.Println("Error sending", err)
				writer.WriteHeader(400)
				write(fmt.Sprint(err), &writer)
				return
			}
			jsonOutput := response_type.KinesisResponse{
				SequenceNumber: &response,
				ShardId: &gcpShardId,
			}
			json.NewEncoder(writer).Encode(jsonOutput)
			// write(output.String(), &writer)
			fmt.Println("Single payload ", string(str))

		} else {
			req := handler.KinesisClient.PutRecordRequest(&kinesis.PutRecordInput{
				Data: str,
				PartitionKey: &payload.PartitionKey,
				StreamName: &payload.StreamName,
			})
			output, err := req.Send()
			if err != nil {
				fmt.Println("Error sending", err)
				writer.WriteHeader(400)
				write(fmt.Sprint(err), &writer)
				return
			}
			jsonOutput := response_type.KinesisResponse{
				SequenceNumber: output.SequenceNumber,
				ShardId: output.ShardId,
			}
			json.NewEncoder(writer).Encode(jsonOutput)
			// write(output.String(), &writer)
			fmt.Println("Single payload ", string(str))
		}
	} else if len(payload.Records) > 0 {
		fmt.Println("Multiple records")
		if handler.GCPClient != nil {
			results := make([]*pubsub.PublishResult, len(payload.Records))
			var wg sync.WaitGroup
			wg.Add(len(payload.Records))
			for i, record := range payload.Records {
				str, _ := base64.StdEncoding.DecodeString(record.Data)
				fmt.Println("Record ", string(str), " ", record.PartitionKey)
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
			fmt.Println(records)
			json.NewEncoder(writer).Encode(jsonOutput)

		} else {
			input := kinesis.PutRecordsInput{
				StreamName: &payload.StreamName,
				Records: make([]kinesis.PutRecordsRequestEntry, len(payload.Records)),
			}
			for i, record := range payload.Records {
				str, _ := base64.StdEncoding.DecodeString(record.Data)
				fmt.Println("Record ", string(str), " ", record.PartitionKey)
				key := record.PartitionKey
				input.Records[i] = kinesis.PutRecordsRequestEntry{
					Data: str,
					PartitionKey: &key,
				}
			}
			fmt.Println("Records ", input.Records)
			req := handler.KinesisClient.PutRecordsRequest(&input)
			output, err := req.Send()
			if err != nil {
				fmt.Println("Error sending", err)
				writer.WriteHeader(400)
				write(fmt.Sprint(err), &writer)
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
			fmt.Println(output.Records)
			json.NewEncoder(writer).Encode(jsonOutput)

		}
	} else {
		fmt.Println("Missing data")
		writer.WriteHeader(400)
	}
}



func write(input string, writer *http.ResponseWriter) {
	line := fmt.Sprintf("%s", input)
	_, err := (*writer).Write([]byte(line))
	if err != nil {
		panic(fmt.Sprintf("Error %s", err))
	}
}

