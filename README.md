# Cross-Account Architecture: Kinesis Data Stream to Lambda & Glue

## Architecture Overview

```text
ACCOUNT A — SOURCE
Glue Job ──> S3
         └──> Kinesis Data Stream
                      │
                      │ Cross-account Access
                      ▼
ACCOUNT B — CONSUMER
Lambda ──> Glue Job ──> S3
```

---

## Account A — Source Account

### Step 1: Gather Required Identifiers
Collect the following identifiers from **Account A**:
- **Kinesis Stream Name**: `source-stream`
- **Kinesis Stream ARN**: `arn:aws:kinesis:<region>:<Account_A_ID>:stream/<stream-name>`
- **AWS Region**: `ap-south-1` (or your target region)
- **KMS Key ARN** (if encrypted): `arn:aws:kms:<region>:<Account_A_ID>:key/<key-id>`

---

### Step 2: Create Kinesis Resource Policy
In **Account A**, attach a resource-based policy to the Kinesis Data Stream allowing Account B’s Lambda execution role access.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowAccountBConsumerRole",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::<Account_B_ID>:role/KinesisConsumerLambdaRole"
      },
      "Action": [
        "kinesis:DescribeStream",
        "kinesis:DescribeStreamSummary",
        "kinesis:ListShards",
        "kinesis:GetShardIterator",
        "kinesis:GetRecords",
        "kinesis:SubscribeToShard"
      ],
      "Resource": "arn:aws:kinesis:<region>:<Account_A_ID>:stream/<stream-name>"
    }
  ]
}
```

---

### Step 3: KMS Key Policy Configuration (Crucial Correction)
> **IMPORTANT NOTE:** Standard AWS-managed keys (`aws/kinesis`) **cannot** be used for cross-account sharing. A **Customer Managed Key (CMK)** is mandatory in Account A if server-side encryption is enabled.

Update the KMS Key Policy in **Account A** to permit Account B's Lambda role to decrypt the stream data:

```json
{
  "Sid": "AllowAccountBLambdaKMSAccess",
  "Effect": "Allow",
  "Principal": {
    "AWS": "arn:aws:iam::<Account_B_ID>:role/KinesisConsumerLambdaRole"
  },
  "Action": [
    "kms:Decrypt",
    "kms:DescribeKey"
  ],
  "Resource": "*"
}
```

---

### Step 4: Configure Source S3 Bucket Policy (If Account B Reads Data Directly)
If Account B's Glue Job needs to read payload files stored in Account A's S3 bucket, grant read access to Account B's Glue role:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowAccountBGlueS3Read",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::<Account_B_ID>:role/AccountBGlueExecutionRole"
      },
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::<Account_A_Bucket_Name>",
        "arn:aws:s3:::<Account_A_Bucket_Name>/*"
      ]
    }
  ]
}
```

---

## Account B — Consumer Account

### Step 5: Create Lambda Execution IAM Role (`KinesisConsumerLambdaRole`)
1. Create role with **Trusted Entity**: `lambda.amazonaws.com`.
2. Attach basic execution logging policies:
   - `logs:CreateLogGroup`
   - `logs:CreateLogStream`
   - `logs:PutLogEvents`
3. Add inline or managed policy for **Account A Kinesis Access**:
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Action": [
           "kinesis:DescribeStream",
           "kinesis:DescribeStreamSummary",
           "kinesis:GetRecords",
           "kinesis:GetShardIterator",
           "kinesis:ListShards"
         ],
         "Resource": "arn:aws:kinesis:<region>:<Account_A_ID>:stream/<stream-name>"
       },
       {
         "Effect": "Allow",
         "Action": [
           "kms:Decrypt",
           "kms:DescribeKey"
         ],
         "Resource": "arn:aws:kms:<region>:<Account_A_ID>:key/<key-id>"
       }
     ]
   }
   ```

---

### Step 6: Grant Glue Trigger Permission to Lambda Role
Add permission to the same `KinesisConsumerLambdaRole` to invoke the Account B Glue Job:

```json
{
  "Effect": "Allow",
  "Action": "glue:StartJobRun",
  "Resource": "arn:aws:glue:<region>:<Account_B_ID>:job/consumer-glue-job"
}
```

---

### Step 7: Create Lambda Function
1. Create `KinesisConsumerLambda` attached to `KinesisConsumerLambdaRole`.
2. Implement trigger handler logic (e.g., in Python/Node.js) to trigger the Glue job on event ingestion.

---

### Step 8: Create Event Source Mapping
Configure the event trigger in Account B linking to Account A's stream:
- **Event Source**: Kinesis
- **Stream ARN**: `arn:aws:kinesis:<region>:<Account_A_ID>:stream/<stream-name>`
- **Starting Position**: `LATEST` or `TRIM_HORIZON`
- **Batch Size**: e.g., `100`

---

### Step 9: Configure Account B Glue Job & Role
Create `AccountBGlueExecutionRole` with:
- `AWSGlueServiceRole` AWS managed policy.
- Access to local S3 buckets in Account B.
- Access to Account A's S3 bucket (if reading source objects directly).

---

### Step 10: Lambda Code Implementation Pattern

```python
import json
import boto3
import os

glue_client = boto3.client('glue')
GLUE_JOB_NAME = os.environ.get('GLUE_JOB_NAME', 'consumer-glue-job')

def lambda_handler(event, context):
    for record in event['Records']:
        # Extract payload from Kinesis base64 data
        payload = json.loads(boto3.loads(record['kinesis']['data']))
        
        # Trigger Glue Job passing event metadata as arguments
        response = glue_client.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                '--EVENT_ID': payload.get('eventId', ''),
                '--SOURCE_BUCKET': payload.get('bucket', ''),
                '--SOURCE_KEY': payload.get('key', '')
            }
        )
    return {'statusCode': 200, 'body': 'Triggered Glue job successfully'}
```

---

## Final Verification Checklist

| Account | Item | Status |
| :--- | :--- | :--- |
| **Account A** | Customer-Managed KMS Key configured (if SSE active) | [ ] |
| **Account A** | Resource policy attached to Kinesis Stream allowing Account B Lambda Role | [ ] |
| **Account A** | KMS Key policy updated allowing `kms:Decrypt` for Account B Lambda Role | [ ] |
| **Account A** | Bucket policy configured (if Account B Glue directly reads S3 files) | [ ] |
| **Account B** | IAM Execution Role created for Lambda with Kinesis & Glue permissions | [ ] |
| **Account B** | Event Source Mapping created pointing to Account A Stream ARN | [ ] |
| **Account B** | Glue job created with correct execution role and target storage access | [ ] |