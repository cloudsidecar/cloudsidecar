package converter

import (
	"cloud.google.com/go/storage"
	"cloudsidecar/pkg/mock"
	"cloudsidecar/pkg/response_type"
	"crypto/md5"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/s3manager"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"strings"
	"testing"
	"time"
)

func TestGCSACLResponseToAWS(t *testing.T) {
	aclRule := storage.ACLRule{
		Email: "larry@barry.com",
		Role: storage.RoleOwner,
		Entity: "abc123",
	}
	rules := []storage.ACLRule{aclRule}
	resp := GCSACLResponseToAWS(rules)
	if len(resp.AccessControlList.Grants) != 1 {
		t.Error("ACL Grants must be size 1")
	}
	if resp.AccessControlList.Grants[0].Grantee.DisplayName != aclRule.Email {
		t.Error("Displayname wrong")
	}
	if resp.AccessControlList.Grants[0].Grantee.Id != string(aclRule.Entity) {
		t.Error("Id wrong")
	}
}

func TestGCSAttrToHeaders(t *testing.T) {
	utc, _ := time.LoadLocation("UTC")
	attrs := &storage.ObjectAttrs{
		Size: 123456,
		Updated: time.Now(),
	}
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	headers := make(map[string][]string)
	writerMock := mock.NewMockResponseWriter(ctrl)
	writerMock.EXPECT().Header().MaxTimes(2).Return(headers)
	GCSAttrToHeaders(attrs, writerMock)
	expectedTime := strings.Replace(attrs.Updated.In(utc).Format(time.RFC1123), "UTC", "GMT", 1)
	if headers["Content-Length"][0] != "123456" {
		t.Error("Content length was ", headers["Content-Length"][0], " and not ", 123456)
	}
	assert.Equal(t, expectedTime, headers["Last-Modified"][0])
}

func TestGCSAttrToCombine(t *testing.T) {
	hash := md5.Sum([]byte("meow"))
	attrs := &storage.ObjectAttrs{
		Bucket: "buckers",
		MD5: hash[:],
		Name: "myName",
	}
	output := GCSAttrToCombine(attrs)
	assert.Equal(t, attrs.Bucket, *output.Bucket)
	assert.Equal(t, fmt.Sprintf("%x", hash), *output.ETag)
	assert.Equal(t, attrs.Name, *output.Key)
}

func TestGCSItemToPrefix(t *testing.T) {
	item := &storage.ObjectAttrs{
		Prefix: "Mow",
	}
	output := GCSItemToPrefix(item)
	assert.Equal(t, item.Prefix, output.Prefix)
}

func TestGCSItemToContent(t *testing.T) {
	updatedTime := time.Unix(1550463794, 0)
	hash := md5.Sum([]byte("meow"))
	item := &storage.ObjectAttrs{
		Updated: updatedTime,
		Size: 12345,
		MD5: hash[:],
		StorageClass: "storage",
		Name: "Key",
	}
	output := GCSItemToContent(item)
	assert.Equal(t, item.Size, output.Size)
	assert.Equal(t, item.StorageClass, output.StorageClass)
	assert.Equal(t, item.Name, output.Key)
	assert.Equal(t, "2019-02-18T04:23:14.000Z", output.LastModified)
}

func TestGCSListResponseObjectsToAWS(t *testing.T) {
	bucket := "zeBucket"
	prefix := ""
	var keys int64 = 1234
	req := &s3.ListObjectsInput{
		Bucket: &bucket,
		Prefix: &prefix,
		MaxKeys: &keys,
	}
	contents := []*response_type.BucketContent{
		{
			Key: "cow",
			Size: 12,
			LastModified: "2019-02-18T04:23:14.000Z",
		},
	}
	prefixes := []*response_type.BucketCommonPrefix{
		{
			Prefix: "my_dir",
		},
	}
	output := GCSListResponseObjectsToAWS(contents, req, "meow", 1, 0, nil)
	assert.Equal(t, req.MaxKeys, output.MaxKeys)
	assert.Equal(t, req.Prefix, output.Prefix)
	assert.Equal(t, req.Bucket, output.Name)
	assert.Equal(t, true, *output.IsTruncated)
	assert.Equal(t, int64(1), output.KeyCount)
	assert.Equal(t, 1, len(output.Contents))
	assert.Equal(t, 0, len(output.CommonPrefixes))
	assert.Equal(t, "meow", *output.NextContinuationToken)
	assert.Equal(t, "cow", output.Contents[0].Key)
	assert.Equal(t, int64(12), output.Contents[0].Size)
	assert.Equal(t, "2019-02-18T04:23:14.000Z", output.Contents[0].LastModified)

	output = GCSListResponseObjectsToAWS(nil, req, "meow", 0, 1, prefixes)
	assert.Equal(t, 0, len(output.Contents))
	assert.Equal(t, 1, len(output.CommonPrefixes))
	assert.Equal(t, "my_dir", output.CommonPrefixes[0].Prefix)

	contents = []*response_type.BucketContent{
		{
			Key: "cow",
			Size: 12,
			LastModified: "2019-02-18T04:23:14.000Z",
		},
		{
			Key: "now",
			Size: 20,
			LastModified: "2019-02-19T04:23:14.000Z",
		},
	}
	prefixes = []*response_type.BucketCommonPrefix{
		{
			Prefix: "my_dir",
		},
		{
			Prefix: "your_dir",
		},
	}
	output = GCSListResponseObjectsToAWS(contents, req, "meow", 2, 2, prefixes)
	assert.Equal(t, 2, len(output.Contents))
	assert.Equal(t, 2, len(output.CommonPrefixes))
	assert.Equal(t, "cow", output.Contents[0].Key)
	assert.Equal(t, "now", output.Contents[1].Key)
	assert.Equal(t, "my_dir", output.CommonPrefixes[0].Prefix)
	assert.Equal(t, "your_dir", output.CommonPrefixes[1].Prefix)
}

func TestGCPUpload(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	writerMock := mock.NewMockWriter(ctrl)
	fakeBody := "this is a test of the american broadcasting system"
	fakeBodyBytes := []byte(fakeBody)
	uploadInput := &s3manager.UploadInput{
		Body: strings.NewReader(fakeBody),
	}
	writerMock.EXPECT().Write(fakeBodyBytes).Return(len(fakeBodyBytes), nil)
	output, err := GCPUpload(uploadInput, writerMock)
	assert.Nil(t, err)
	assert.Equal(t, output, int64(len(fakeBodyBytes)))

	rand.Seed(time.Now().UnixNano())
	letterRunes := []rune("abc")
	fakeBodyRune := make([]rune, 4100)
	for i := range fakeBodyRune {
		fakeBodyRune[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	fakeBody = string(fakeBodyRune)
	fakeBodyBytes = []byte(fakeBody)
	uploadInput = &s3manager.UploadInput{
		Body: strings.NewReader(fakeBody),
	}
	writerMock2 := mock.NewMockWriter(ctrl)
	firstCall := writerMock2.EXPECT().Write(fakeBodyBytes[:4096]).Return(4096, nil)
	writerMock2.EXPECT().Write(fakeBodyBytes[4096:]).Return(4, nil).After(firstCall)
	output, err = GCPUpload(uploadInput, writerMock2)
	assert.Nil(t, err)
	assert.Equal(t, output, int64(len(fakeBodyBytes)))
}
