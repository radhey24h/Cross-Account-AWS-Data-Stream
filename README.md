# Cross-Account Kinesis to Lambda to Glue

This repository contains a CloudFormation implementation for a cross-account pipeline in `ap-south-1`:

```text
Account A: source-stream (KMS encrypted) -> cross-account stream policy
Account B: Kinesis event source mapping -> KinesisConsumerLambda -> consumer-glue-job
Account A: optional source-data-bucket read access for ConsumerGlueJobRole
```

## Input Contract

Each Kinesis record must contain UTF-8 JSON encoded as the record's base64 `data` field:

```json
{
  "s3_bucket": "source-data-bucket",
  "s3_key": "incoming/object.json",
  "job_run_id": "optional-upstream-run-id"
}
```

The Lambda starts one Glue run per valid record with `--S3_SOURCE_BUCKET` and `--S3_SOURCE_KEY`; when provided, `job_run_id` is forwarded as `--SOURCE_JOB_RUN_ID`. Invalid records and failed `StartJobRun` calls are returned in `batchItemFailures` using their Kinesis sequence numbers.

## Step-by-Step Deployment

The templates and Lambda source are [cloudformation/account-a-source.yaml](cloudformation/account-a-source.yaml), [cloudformation/account-b-consumer.yaml](cloudformation/account-b-consumer.yaml), and [lambda/index.py](lambda/index.py). The order below is required because Account A's resource policies name the Account B roles, while the final Account B deployment needs the stream and KMS ARNs created in Account A.

One operator can run every step if they have deployment access to both accounts. Otherwise, the Account B operator completes steps 1, 3, and 4; the Account A operator completes step 2. Share the role ARN from step 1 with Account A, then share the stream and key ARNs from step 2 with Account B. Use separate CLI profiles named `source` and `consumer` below, or substitute your own.

### Account B — Consumer (Steps 1, 3, 4, and 5)

**1. Prepare Account B and bootstrap its roles.** Create the Account B artifacts bucket and prepare a Glue ETL script that accepts `--S3_SOURCE_BUCKET`, `--S3_SOURCE_KEY`, and optionally `--SOURCE_JOB_RUN_ID`. The deployment identity needs permission to create/manage IAM roles, Lambda, Glue, and the event source mapping, plus upload access to the artifacts bucket. Bootstrap creates the fixed-name Lambda and Glue roles before Account A references them. The Lambda role's KMS ARN is a temporary placeholder, replaced in step 4.

   ```sh
   aws cloudformation deploy \
     --profile consumer \
     --region ap-south-1 \
     --stack-name cross-account-kinesis-consumer \
     --template-file cloudformation/account-b-consumer.yaml \
     --capabilities CAPABILITY_NAMED_IAM \
     --parameter-overrides \
       BootstrapOnly=true \
       SourceStreamArn=arn:aws:kinesis:ap-south-1:111111111111:stream/source-stream \
       SourceKmsKeyArn=arn:aws:kms:ap-south-1:111111111111:key/BOOTSTRAP_PLACEHOLDER \
       SourceDataBucketName=source-data-bucket \
       GlueArtifactsBucketName=consumer-artifacts-ap-south-1
   ```

### Account A — Source (Step 2)

**2. Deploy Account A.** The Account B role now exists, allowing it to be named as a principal in the KMS key policy and Kinesis stream resource policy. The Account A deployment identity needs permission to create/manage KMS keys, Kinesis streams, resource policies, and (if enabled) the S3 bucket policy. The stack creates `source-stream` with customer-managed KMS encryption and a seven-day retention period. If Glue reads objects directly from Account A, set `SourceDataBucketName` to that existing bucket; otherwise set it to an empty value.

The templates grant these cross-account permissions:
- Account A's Kinesis resource policy grants `KinesisConsumerLambdaRole` `DescribeStream`, `DescribeStreamSummary`, `ListShards`, `GetShardIterator`, and `GetRecords` on `source-stream`.
- Account A's customer-managed KMS key policy grants that Lambda role `kms:Decrypt` and `kms:DescribeKey`. The AWS-managed `aws/kinesis` key cannot be shared cross-account.
- Account B's Lambda role has matching Kinesis and KMS identity permissions, CloudWatch Logs permissions, and `glue:StartJobRun` on `consumer-glue-job`.
- Account B's Glue role uses `AWSGlueServiceRole` and has S3 read access to its script/artifact bucket. If configured, Account A's bucket policy and the Glue role's identity policy grant `s3:ListBucket` and `s3:GetObject` for the source bucket.

   ```sh
   aws cloudformation deploy \
     --profile source \
     --region ap-south-1 \
     --stack-name cross-account-kinesis-source \
     --template-file cloudformation/account-a-source.yaml \
     --parameter-overrides \
       ConsumerLambdaRoleArn=arn:aws:iam::222222222222:role/KinesisConsumerLambdaRole \
       SourceDataBucketName=source-data-bucket
   ```

   Record `SourceStreamArn` and `SourceKmsKeyArn` from the stack outputs:

   ```sh
   aws cloudformation describe-stacks --profile source --region ap-south-1 \
     --stack-name cross-account-kinesis-source \
     --query 'Stacks[0].Outputs[*].[OutputKey,OutputValue]' --output table
   ```

### Account B — Consumer (continued)

**3. Upload the Lambda package and Glue script.** Create the ZIP from the repository root; boto3 is included in the Python 3.12 Lambda runtime.

   ```sh
   zip -j kinesis-consumer.zip lambda/index.py
   aws s3 cp kinesis-consumer.zip s3://consumer-artifacts-ap-south-1/lambda/kinesis-consumer.zip --profile consumer --region ap-south-1
   aws s3 cp path/to/consumer.py s3://consumer-artifacts-ap-south-1/glue/consumer.py --profile consumer --region ap-south-1
   ```

   Replace `path/to/consumer.py` with the workload's Glue ETL script. It must accept `--S3_SOURCE_BUCKET`, `--S3_SOURCE_KEY`, and optionally `--SOURCE_JOB_RUN_ID` as Glue job arguments.

**4. Activate the Account B consumer.** Supply the exact stream and key output ARNs from step 2, artifact bucket, and actual Glue script URI. If source objects use SSE-KMS, provide `SourceDataKmsKeyArn` and separately authorize the Account B Glue role in that key's key policy. Otherwise leave it empty. The event source mapping uses `LATEST`, batch size `100`, a `10` second batching window, and `ReportBatchItemFailures`.

   ```sh
   aws cloudformation deploy \
     --profile consumer \
     --region ap-south-1 \
     --stack-name cross-account-kinesis-consumer \
     --template-file cloudformation/account-b-consumer.yaml \
     --capabilities CAPABILITY_NAMED_IAM \
     --parameter-overrides \
       BootstrapOnly=false \
       SourceStreamArn=arn:aws:kinesis:ap-south-1:111111111111:stream/source-stream \
       SourceKmsKeyArn=arn:aws:kms:ap-south-1:111111111111:key/ACCOUNT_A_KEY_ID \
       SourceDataBucketName=source-data-bucket \
       SourceDataKmsKeyArn='' \
       GlueArtifactsBucketName=consumer-artifacts-ap-south-1 \
       LambdaArtifactBucket=consumer-artifacts-ap-south-1 \
       LambdaArtifactKey=lambda/kinesis-consumer.zip \
       GlueScriptLocation=s3://consumer-artifacts-ap-south-1/glue/consumer.py \
       GlueJobName=consumer-glue-job
   ```

**5. Verify.** Confirm the event source mapping is enabled, check the `KinesisConsumerLambda` CloudWatch log group, and verify Glue runs in Account B. Publish a test record matching the input contract to `source-stream`.

**Important:** The optional Account A `AWS::S3::BucketPolicy` manages the entire bucket policy. Preserve existing policy statements when deploying it. If source objects use SSE-KMS, also grant the Account B Glue role decrypt access in the source object's KMS key policy. Kinesis event-source processing is at-least-once, so make the Glue workload idempotent to avoid duplicate processing after retries.