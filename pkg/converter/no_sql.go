package converter

import (
	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/datastore"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"cloudsidecar/pkg/aws/handler/dynamo/antlr"
	"cloudsidecar/pkg/logging"
	"cloudsidecar/pkg/response_type"
	"strconv"
	"strings"
)

func GCPBigTableGetById(input *dynamodb.GetItemInput) (string, error) {
	var columnName string
	var columnValue string
	for key, value := range input.Key {
		columnName = key
		if value.S != nil {
			columnValue = *value.S
		}
		break
	}
	logging.Log.Debug("Filtering on ", columnName, "=", columnValue)
	return columnValue, nil
}

func awsAttirbuteToValue(value dynamodb.AttributeValue) interface {} {
	var queryValue interface{}
	if value.S != nil {
		queryValue = *value.S
	} else if len(value.B) > 0 {
		queryValue = value.B
	} else if value.N != nil {
		queryValue, _ = strconv.ParseFloat(*value.N, 64)
	} else if value.BOOL != nil {
		queryValue = *value.BOOL
	}
	return queryValue
}

func AWSQueryToGCPDatastoreQuery(input *dynamodb.QueryInput) (*datastore.Query, map[string][]interface {}, error) {
	query := datastore.NewQuery(*input.TableName)
	if input.Limit != nil {
		query = query.Limit(int(*input.Limit))
	}
	if input.KeyConditionExpression != nil {
		keyExprPieces := strings.SplitN(*input.KeyConditionExpression, " = ", 2)
		filter := awsAttirbuteToValue(input.ExpressionAttributeValues[keyExprPieces[1]])
		key := datastore.NameKey(*input.TableName, fmt.Sprint(filter), nil)
		query = query.Filter("__key__ = ", key)
	}
	var err error
	var inFilters map[string][]interface {}
	if input.FilterExpression != nil {
		if input.FilterExpression != nil {
			query, inFilters, err = handleFilterExpression(query, *input.FilterExpression, input.ExpressionAttributeNames, input.ExpressionAttributeValues)
		}
	}
	return query, inFilters, err
}

func removeParens(queryPiece string) string {
	queryPiece = strings.Replace(queryPiece, "(", "", -1)
	queryPiece = strings.Replace(queryPiece, ")", "", -1)
	return queryPiece
}

func handleFilterExpression(
	query *datastore.Query,
	filterExpression string,
	attributeNames map[string]string,
	attributeValues map[string]dynamodb.AttributeValue) (*datastore.Query, map[string][]interface {}, error){

	newQuery := query
	attributeStringValues := make(map[string]interface {})
	for key, value := range attributeValues {
		attributeStringValues[key] = awsAttirbuteToValue(value)
	}
	parser := antlr.Lex(filterExpression, attributeNames, attributeStringValues)
	for i, queryPiece := range parser.Filters {
		logging.Log.Debug("Adding filter", queryPiece, parser.FilterValues[i])
		newQuery = newQuery.Filter(queryPiece, parser.FilterValues[i])
	}
	return newQuery, parser.InFilters, nil
}

func AWSScanToGCPDatastoreQuery(input *dynamodb.ScanInput) (*datastore.Query, map[string][]interface {}, error) {
	query := datastore.NewQuery(*input.TableName)
	if input.Limit != nil {
		query = query.Limit(int(*input.Limit))
	}
	var err error
	var inFilters map[string][]interface {}
	if input.FilterExpression != nil {
		query, inFilters, err = handleFilterExpression(query, *input.FilterExpression, input.ExpressionAttributeNames, input.ExpressionAttributeValues)
	}
	return query, inFilters, err
}

func ValueToAWS(value interface{}) dynamodb.AttributeValue {
	intValue, ok := value.(int64)
	if ok {
		strOfInt := strconv.FormatInt(intValue, 10)
		return dynamodb.AttributeValue{
			N: &strOfInt,
		}
	}
	floatValue, ok := value.(float64)
	strOfFloat := strconv.FormatFloat(floatValue, 'f', -1, 64)
	if ok {
		return dynamodb.AttributeValue{
			N: &strOfFloat,
		}
	}
	boolValue, ok := value.(bool)
	if ok {
		return dynamodb.AttributeValue{
			BOOL: &boolValue,
		}
	}
	mapValue, ok := value.(response_type.Map)
	if ok {
		attributeMap := make(map[string]dynamodb.AttributeValue)
		for k, v := range mapValue {
			attributeMap[k] = ValueToAWS(v)
		}
		return dynamodb.AttributeValue{
			M: attributeMap,
		}
	}
	keyValue, ok := value.(datastore.Key)
	if ok {
		if keyValue.ID != 0 {
			strOfInt := strconv.FormatInt(keyValue.ID, 10)
			return dynamodb.AttributeValue{
				N: &strOfInt,
			}
		} else {
			return dynamodb.AttributeValue{
				S: &keyValue.Name,
			}
		}
	}
	stringValue := fmt.Sprint(value)
	return dynamodb.AttributeValue{
		S: &stringValue,
	}
}

func GCPDatastoreMapToAWS(row response_type.Map) (*dynamodb.GetItemOutput, error) {
	var result = make(map[string]dynamodb.AttributeValue)
	for key, value := range row {
		result[key] = ValueToAWS(value)
	}
	return &dynamodb.GetItemOutput{
		Item: result,
	}, nil
}

func GCPBigTableResponseToAWS(row *bigtable.Row) (*dynamodb.GetItemOutput, error) {
	var result = make(map[string]dynamodb.AttributeValue)
	for _, values := range *row {
		for _, value := range values {
			stringValue := string(value.Value)
			result[value.Column] = dynamodb.AttributeValue{
				S: &stringValue,
			}
		}
	}
	return &dynamodb.GetItemOutput{
		Item: result,
	}, nil
}
