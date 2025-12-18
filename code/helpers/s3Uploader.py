from helpers.setup_logger import logger
import boto3
from botocore.exceptions import ClientError
import io


def upload_file(data: str, bucket: str, object_name: str) -> bool:
    """Upload data to an S3 bucket

    :param data: Data(Str) to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used (S3 File Name)
    :return: True if file was uploaded, else False
    """

    # Upload the file
    s3_client = boto3.client("s3")
    try:
        file_obj = io.BytesIO(data.encode("utf-8"))
        s3_client.put_object(Bucket=bucket, Key=object_name, Body=file_obj)
    except ClientError as e:
        logger.error(e)
        return False
    return True
