"""Start one Glue job for each valid Kinesis record."""

import base64
import binascii
import json
import logging
import os
from typing import Any

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

GLUE_JOB_NAME = os.environ.get("GLUE_JOB_NAME", "consumer-glue-job")
glue = boto3.client("glue")


class RecordValidationError(ValueError):
    """Raised when a Kinesis record cannot be converted into Glue arguments."""


def _decode_payload(encoded_data: str) -> dict[str, Any]:
    """Decode Kinesis base64 data and require a JSON object payload."""
    try:
        decoded = base64.b64decode(encoded_data, validate=True).decode("utf-8")
        payload = json.loads(decoded)
    except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RecordValidationError("Kinesis data is not valid base64-encoded UTF-8 JSON") from exc

    if not isinstance(payload, dict):
        raise RecordValidationError("Kinesis payload must be a JSON object")
    return payload


def _required_string(payload: dict[str, Any], name: str) -> str:
    """Return a non-empty string value required by the Glue job."""
    value = payload.get(name)
    if not isinstance(value, str) or not value.strip():
        raise RecordValidationError(f"Payload field {name!r} must be a non-empty string")
    return value


def _start_glue_job(record: dict[str, Any]) -> None:
    """Validate a record and submit its source object to the configured Glue job."""
    kinesis_data = record.get("kinesis", {}).get("data")
    if not isinstance(kinesis_data, str):
        raise RecordValidationError("Record is missing Kinesis data")

    payload = _decode_payload(kinesis_data)
    source_bucket = _required_string(payload, "s3_bucket")
    source_key = _required_string(payload, "s3_key")

    arguments = {
        "--S3_SOURCE_BUCKET": source_bucket,
        "--S3_SOURCE_KEY": source_key,
    }
    job_run_id = payload.get("job_run_id")
    if job_run_id is not None:
        if not isinstance(job_run_id, str) or not job_run_id.strip():
            raise RecordValidationError("Payload field 'job_run_id' must be a non-empty string")
        arguments["--SOURCE_JOB_RUN_ID"] = job_run_id

    glue.start_job_run(JobName=GLUE_JOB_NAME, Arguments=arguments)


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, list[dict[str, str]]]:
    """Process records independently and report failed sequence numbers to Lambda."""
    del context  # The function does not need request metadata.
    batch_item_failures: list[dict[str, str]] = []

    for record in event.get("Records", []):
        kinesis = record.get("kinesis", {})
        sequence_number = kinesis.get("sequenceNumber")
        if not sequence_number:
            # Lambda requires the Kinesis sequence number as the partial failure ID.
            # Raising retries the batch when the record envelope itself is unusable.
            raise ValueError("Kinesis record is missing its sequence number")

        try:
            _start_glue_job(record)
            logger.info("Started Glue job for Kinesis sequence %s", sequence_number)
        except RecordValidationError as exc:
            logger.warning("Invalid Kinesis record %s: %s", sequence_number, exc)
            batch_item_failures.append({"itemIdentifier": sequence_number})
        except Exception:
            logger.exception("Could not start Glue job for Kinesis sequence %s", sequence_number)
            batch_item_failures.append({"itemIdentifier": sequence_number})

    return {"batchItemFailures": batch_item_failures}
