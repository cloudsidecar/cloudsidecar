package converter

import (
	"cloud.google.com/go/bigtable"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"sidecar/response_type"
	"strconv"
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

func GCPDatastoreMapToAWS(row response_type.Map) (*dynamodb.GetItemOutput, error) {
	var result = make(map[string]dynamodb.AttributeValue)
	for key, value := range row {
		intValue, ok := value.(int64)
		if ok {
			strOfInt := strconv.FormatInt(intValue, 10)
			result[key] = dynamodb.AttributeValue{
				N: &strOfInt,
			}
			continue
		}
		floatValue, ok := value.(float64)
		strOfFloat := strconv.FormatFloat(floatValue, 'f', -1, 64)
		if ok {
			result[key] = dynamodb.AttributeValue{
				N: &strOfFloat,
			}
			continue
		}
		boolValue, ok := value.(bool)
		if ok {
			result[key] = dynamodb.AttributeValue{
				BOOL: &boolValue,
			}
			continue
		}
		stringValue, ok := value.(string)
		if ok {
			result[key] = dynamodb.AttributeValue{
				S: &stringValue,
			}
			continue
		}
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
