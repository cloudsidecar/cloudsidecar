package converter

import (
	"cloud.google.com/go/storage"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"sidecar/response_type"
)

func gcpPermissionToAWS(role storage.ACLRole) string {
	if role == storage.RoleOwner {
		return string(s3.PermissionFullControl)
	} else if role == storage.RoleReader {
		return string(s3.PermissionRead)
	} else {
		return string(s3.PermissionWrite)
	}
}

func GCSACLResponseToAWS(input []storage.ACLRule) response_type.AWSACLResponse {
	response := response_type.AWSACLResponse{}
	var grants = make([]*response_type.Grant, len(input))
	for i, entry := range input {
		var displayName string
		if entry.Email != "" {
			displayName = entry.Email
		} else {
			displayName = string(entry.Entity)
		}
		if entry.Role == storage.RoleOwner && response.OwnerId == "" {
			response.OwnerId = string(entry.Entity)
			response.OwnerDisplayName = displayName
		}
		grant := &response_type.Grant{
			Permission: gcpPermissionToAWS(entry.Role),
			Grantee: &response_type.Grantee{
				DisplayName: displayName,
				Id: string(entry.Entity),
				XmlNS: response_type.ACLXmlNs,
				Xsi: response_type.ACLXmlXsi,
			},
		}
		grants[i] = grant
	}
	response.AccessControlList = &response_type.AccessControlList{
		Grants: grants,
	}
	return response
}
