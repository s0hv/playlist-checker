import logging
import os
from datetime import datetime
from typing import Optional, Any
from urllib import parse

import boto3
from boto3.exceptions import S3UploadFailedError
from botocore.exceptions import ClientError, WaiterError

logger = logging.getLogger('debug')

S3_CLIENT = boto3.client('s3')
S3_RESOURCE = boto3.resource('s3')


def delete_files(keys: list[str], bucket: str):
    if not keys:
        return

    logger.info(f'Deleting {len(keys)} files from S3. {keys}')

    resp = S3_CLIENT.delete_objects(
        Bucket=bucket,
        Delete={
            'Objects': [{'Key': key} for key in keys]
        }
    )

    if errors := resp.get('Errors'):
        logger.error(f'S3 delete objects returned errors. {errors}')


def upload_file(file_path: str, bucket: str, tags: dict[str, Any], object_name: str = None) -> Optional[tuple[str, int]]:
    """
    Uploads a file to S3 storage.
    Returns object_name if given or filename of file_path when successful.
    """
    if object_name is None:
        object_name = os.path.basename(file_path)

    now = datetime.utcnow()

    try:
        S3_CLIENT.upload_file(file_path, bucket, object_name,
                              ExtraArgs={
                                  'Tagging': parse.urlencode(tags)
                              })
    except (ClientError, S3UploadFailedError):
        logger.exception(f'Failed to upload file {file_path}')
        return
    else:
        # https://stackoverflow.com/a/60892027/6046713
        s3_obj = S3_RESOURCE.Object(bucket, object_name)
        try:
            s3_obj.wait_until_exists(IfModifiedSince=now)
        except WaiterError:
            logger.error(f'File upload to S3 failed. Filename: {object_name}')
        else:
            head = S3_CLIENT.head_object(Bucket=bucket, Key=object_name)
            filesize = head['ContentLength']
            if filesize:
                return object_name, filesize

def get_file_size(bucket: str, object_name: str) -> int:
    head = S3_CLIENT.head_object(Bucket=bucket, Key=object_name)
    return head['ContentLength']
