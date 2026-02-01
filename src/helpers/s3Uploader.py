from src.helpers.setup_logger import logger
from src.helpers.datadog_instrumentation import (
    trace_function,
    get_statsd,
    Metrics,
)
import boto3
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
import io
import time

statsd = get_statsd()


@trace_function("s3.upload", resource="S3Uploader")
def upload_file(data: str, bucket: str, object_name: str) -> bool:
    """Upload data to an S3 bucket.

    :param data: Data (string) to upload
    :param bucket: S3 bucket to upload to
    :param object_name: S3 object name (file path in bucket)
    :return: True if file was uploaded successfully, else False
    """
    start_time = time.time()

    try:
        s3_client = boto3.client("s3")
        logger.debug(f"Attempting S3 upload: s3://{bucket}/{object_name}")

        file_obj = io.BytesIO(data.encode("utf-8"))
        s3_client.put_object(Bucket=bucket, Key=object_name, Body=file_obj)

        # Record success metrics
        duration_ms = (time.time() - start_time) * 1000
        statsd.increment(Metrics.S3_UPLOAD_SUCCESS)
        statsd.histogram(Metrics.S3_UPLOAD_DURATION, duration_ms)

        logger.debug(f"Successfully uploaded to S3: s3://{bucket}/{object_name}")
        return True

    except NoCredentialsError:
        statsd.increment(Metrics.S3_UPLOAD_FAILURE, tags=["error:no_credentials"])
        logger.error(
            "AWS credentials not found. Ensure AWS credentials are configured."
        )
        return False
    except PartialCredentialsError as e:
        statsd.increment(Metrics.S3_UPLOAD_FAILURE, tags=["error:partial_credentials"])
        logger.error(f"Incomplete AWS credentials: {e}")
        return False
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        error_msg = e.response.get("Error", {}).get("Message", str(e))

        statsd.increment(
            Metrics.S3_UPLOAD_FAILURE, tags=[f"error:{error_code.lower()}"]
        )

        if error_code == "NoSuchBucket":
            logger.error(f"S3 bucket does not exist: {bucket}")
        elif error_code == "AccessDenied":
            logger.error(f"Access denied to S3 bucket: {bucket}")
        else:
            logger.error(f"S3 upload failed ({error_code}): {error_msg}")

        return False
    except Exception as e:
        statsd.increment(Metrics.S3_UPLOAD_FAILURE, tags=["error:unknown"])
        logger.exception(f"Unexpected error during S3 upload: {e}")
        return False
