package converter

import (
	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/datastore"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sidecar/response_type"
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
	fmt.Println("Filtering on ", columnName, "=", columnValue)
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

func handleFilterExpression(
	query *datastore.Query,
	filterExpression string,
	attributeNames map[string]string,
	attributeValues map[string]dynamodb.AttributeValue) (*datastore.Query, map[string][]interface {}, error){
	queryString := filterExpression
	if strings.Index(queryString, "(") != -1 {
		queryString = queryString[1:len(queryString) - 1]
	}
	if attributeNames != nil {
		for key, value := range attributeNames {
			queryString = strings.Replace(queryString, key, value, -1)
		}
	}
	var queryValue interface{}
	queryPieces := strings.Split(queryString, " AND ")
	queryValues := make([]interface{}, len(queryPieces))
	inFilters := make(map[string][]interface {})
	if attributeValues != nil {
		for i, queryPiece := range queryPieces {
			fmt.Println(queryString, "BOO", queryPiece)
			if strings.Contains(queryPiece, " IN ") {
				filterPieces := strings.SplitN(queryPiece, " IN ", 2)
				variableName := strings.Trim(filterPieces[0], " ")
				inClause := filterPieces[1][1:len(filterPieces[1]) - 1]
				inPieces := strings.Split(inClause, ",")
				inFilters[variableName] = make([]interface {}, len(inPieces))
				j := 0
				for key, value := range attributeValues {
					if strings.Contains(inClause, key) {
						inQueryValue := awsAttirbuteToValue(value)
						inFilters[variableName][j] = inQueryValue
						j++
					}
				}
				queryPieces[i] = ""
				fmt.Printf("POO:%s:%s\n", variableName, inClause)
				fmt.Println(inFilters)
			} else {
				for key, value := range attributeValues {
					if strings.Contains(queryPiece, key) {
						queryPiece = strings.Replace(queryPiece, key, "", -1)
						queryPieces[i] = queryPiece
						queryValue = awsAttirbuteToValue(value)
						queryValues[i] = queryValue
					}
				}
			}
		}
	}
	newQuery := query
	for i, queryPiece := range queryPieces {
		if len(queryPiece) != 0 {
			queryPiece = strings.Replace(queryPiece, "(", "", -1)
			queryPiece = strings.Replace(queryPiece, ")", "", -1)
			newQuery = newQuery.Filter(queryPiece, queryValues[i])
		}
	}
	return newQuery, inFilters, nil
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
