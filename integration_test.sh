#!/bin/bash
CONFIG_FIle=$1
BUCKET=$2

gsutil rm -r gs://$BUCKET/integration/
aws --endpoint-url="http://localhost:3450" s3 ls s3://$BUCKET/integration/
aws --endpoint-url="http://localhost:3450" s3 cp LICENSE s3://$BUCKET/integration/
GS_LS=`gsutil ls gs://$BUCKET/integration/`
if [[ "$GS_LS" == "gs://$BUCKET/integration/LICENSE" ]]
then
  echo "File exists"
else
  echo "File does not exist"
  exit 1
fi  
AWS_LS=`aws --endpoint-url="http://localhost:3450" s3 ls s3://$BUCKET/integration/ | wc -l | xargs`
if [[ "$AWS_LS" == "1" ]]
then
  echo "ls count correct"
else
  echo "ls count wrong"
  exit 1
fi
aws --endpoint-url="http://localhost:3450" s3 cp s3://$BUCKET/integration/LICENSE /tmp/
DIFF=`diff LICENSE /tmp/LICENSE | wc -l | xargs`
if [[ "$DIFF" == "0" ]]
then
  echo "diff correct"
else
  echo "diff wrong"
  exit 1
fi
