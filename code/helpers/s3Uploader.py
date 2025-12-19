from helpers.setup_logger import logger
import boto3
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
import io


def upload_file(data: str, bucket: str, object_name: str) -> bool:
    """Upload data to an S3 bucket.

    :param data: Data (string) to upload
    :param bucket: S3 bucket to upload to
    :param object_name: S3 object name (file path in bucket)
    :return: True if file was uploaded successfully, else False
    """
    try:
        s3_client = boto3.client("s3")
        logger.debug(f"Attempting S3 upload: s3://{bucket}/{object_name}")

        file_obj = io.BytesIO(data.encode("utf-8"))
        s3_client.put_object(Bucket=bucket, Key=object_name, Body=file_obj)

        logger.debug(f"Successfully uploaded to S3: s3://{bucket}/{object_name}")
        return True

    except NoCredentialsError:
        logger.error(
            "AWS credentials not found. Ensure AWS credentials are configured."
        )
        return False
    except PartialCredentialsError as e:
        logger.error(f"Incomplete AWS credentials: {e}")
        return False
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "Unknown")
        error_msg = e.response.get("Error", {}).get("Message", str(e))

        if error_code == "NoSuchBucket":
            logger.error(f"S3 bucket does not exist: {bucket}")
        elif error_code == "AccessDenied":
            logger.error(f"Access denied to S3 bucket: {bucket}")
        else:
            logger.error(f"S3 upload failed ({error_code}): {error_msg}")

        return False
    except Exception as e:
        logger.exception(f"Unexpected error during S3 upload: {e}")
        return False
