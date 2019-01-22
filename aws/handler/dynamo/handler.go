package dynamo

import (
	"cloud.google.com/go/datastore"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"net/http"
	"sidecar/converter"
	"sidecar/response_type"
	"strings"
)

type DynamoHandler struct {
	*Handler
}
type Dynamo interface {
	GetItemHandle(writer http.ResponseWriter, request *http.Request)
	GetItemParseInput(r *http.Request) (*dynamodb.GetItemInput, error)
	QueryHandle(writer http.ResponseWriter, request *http.Request)
	QueryParseInput(r *http.Request) (*dynamodb.QueryInput, error)
	ScanHandle(writer http.ResponseWriter, request *http.Request)
	ScanParseInput(r *http.Request) (*dynamodb.ScanInput, error)
	Handle(writer http.ResponseWriter, request *http.Request)
	New(handler *Handler) *DynamoHandler
}

func New(handler *Handler) *DynamoHandler {
	return &DynamoHandler{Handler: handler}
}

func (handler *DynamoHandler) Handle(writer http.ResponseWriter, request *http.Request) {
	targetHeader := request.Header.Get("X-Amz-Target")
	targetSplit := strings.SplitN(targetHeader, ".", 2)
	targetFunction := strings.ToLower(targetSplit[1])
	if targetFunction == "scan" {
		handler.ScanHandle(writer, request)
	} else if targetFunction == "getitem" {
		handler.GetItemHandle(writer, request)
	} else if targetFunction == "query" {
		handler.QueryHandle(writer, request)
	}
}

func (handler *DynamoHandler) QueryParseInput(r *http.Request) (*dynamodb.QueryInput, error) {
	decoder := json.NewDecoder(r.Body)
	var input dynamodb.QueryInput
	err := decoder.Decode(&input)
	return &input, err
}

func (handler *DynamoHandler) QueryHandle(writer http.ResponseWriter, request *http.Request) {
	decoder := json.NewDecoder(request.Body)
	var input dynamodb.QueryInput
	err := decoder.Decode(&input)
	if err != nil {
		writer.WriteHeader(400)
		fmt.Println("Error with decoding input", err)
		writer.Write([]byte(fmt.Sprint(err)))
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
		tableMap := handler.Config.GetStringMapString("gcp_destination_config.datastore_config.table_key_map")
		keyFieldName := tableMap[*input.TableName]
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
			responseItems[i][keyFieldName] = converter.ValueToAWS(*keys[i])
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
		writer.Write([]byte(fmt.Sprint(err)))
		return
	}
	fmt.Println(resp)
	fmt.Println(input)
	json.NewEncoder(writer).Encode(resp)

}

func (handler *DynamoHandler) ScanParseInput(r *http.Request) (*dynamodb.ScanInput, error) {
	decoder := json.NewDecoder(r.Body)
	var input dynamodb.ScanInput
	err := decoder.Decode(&input)
	return &input, err
}


func contains(list []interface {}, item interface {}) bool {
	for _, arrItem := range list {
		if arrItem == item {
			return true
		}
	}
	return false
}

func (handler *DynamoHandler) ScanHandle(writer http.ResponseWriter, request *http.Request) {
	input, err := handler.ScanParseInput(request)
	if err != nil {
		writer.WriteHeader(400)
		fmt.Println("Error with decoding input", err)
		writer.Write([]byte(fmt.Sprint(err)))
		return
	}
	var resp *dynamodb.ScanOutput
	if handler.GCPDatastoreClient != nil {
		query, inFilters, _ := converter.AWSScanToGCPDatastoreQuery(input)
		fmt.Println("Query", query)
		var items []response_type.Map
		keys, err := handler.GCPDatastoreClient.GetAll(*handler.Context, query, &items)
		fmt.Println(keys, err)
		length := int64(len(keys))
		responseItems := make([]map[string]dynamodb.AttributeValue, length)
		tableMap := handler.Config.GetStringMapString("gcp_destination_config.datastore_config.table_key_map")
		keyFieldName := tableMap[*input.TableName]
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
			}
			fmt.Println("Trying to add key", keys[i])
			responseItems[i][keyFieldName] = converter.ValueToAWS(*keys[i])
		}
		length = int64(len(items))
		resp = &dynamodb.ScanOutput{
			Count: &length,
			Items: responseItems,
		}

	} else {
		resp, err = handler.DynamoClient.ScanRequest(input).Send()
		if err != nil {
			writer.WriteHeader(400)
			fmt.Println("Error", err)
			writer.Write([]byte(fmt.Sprint(err)))
			return
		}
	}
	fmt.Println(resp)
	fmt.Println(input)
	json.NewEncoder(writer).Encode(resp)
}

func (handler *DynamoHandler) GetItemHandle(writer http.ResponseWriter, request *http.Request) {
	input, decodeErr := handler.GetItemParseInput(request)
	if decodeErr != nil {
		writer.WriteHeader(400)
		fmt.Println("Error with decoding input", decodeErr)
		writer.Write([]byte(fmt.Sprint(decodeErr)))
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
		columnValue, err := converter.GCPBigTableGetById(input)
		row, err := handler.GCPBigTableClient.Open(*input.TableName).ReadRow(*handler.Context, columnValue)
		if err != nil {
			writer.WriteHeader(400)
			fmt.Println("Error with decoding input", err)
			writer.Write([]byte(fmt.Sprint(decodeErr)))
			return
		}
		resp, err = converter.GCPBigTableResponseToAWS(&row)
	} else if handler.GCPDatastoreClient != nil {
		columnValue, _ := converter.GCPBigTableGetById(input)
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
			writer.Write([]byte(fmt.Sprint(decodeErr)))
			return
		}
		resp, err = converter.GCPDatastoreMapToAWS(result)
	} else {
		resp, err = handler.DynamoClient.GetItemRequest(input).Send()
		if err != nil {
			writer.WriteHeader(400)
			fmt.Println("Error", err)
			writer.Write([]byte(fmt.Sprint(decodeErr)))
			return
		}
	}
	fmt.Println(resp)
	fmt.Println(input)
	json.NewEncoder(writer).Encode(resp)
}
func (handler *DynamoHandler) GetItemParseInput(r *http.Request) (*dynamodb.GetItemInput, error) {
	decoder := json.NewDecoder(r.Body)
	var input dynamodb.GetItemInput
	err := decoder.Decode(&input)
	return &input, err
}
