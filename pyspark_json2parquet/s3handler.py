import boto3
from botocore.exceptions import ClientError
import sys

"""
Use Cases:
-> get and put objects into s3 
"""


class S3Handler:

    def __init__(self,bucket, logger,region="us-west-2", prefix="/"):

        self.log = logger
        self.s3 = boto3.client('s3', region_name=region)
        try:
            resp = self.s3.head_bucket(Bucket=bucket)
        except Exception:
            self.log.error('Bucket [{}] does not exist'.format(bucket))
            raise Exception('Bucket does not exist')

        self.bucket = bucket
        self.prefix = prefix

    def list_bucket_prefix(self, post_date=None):
        objects = self.s3.list_objects(Bucket=self.bucket, Prefix=self.prefix)
        if not ('Contents' in objects.keys()):
            self.log.info('list_bucket_prefix No keys found for prefix - {}'.format(self.prefix))
            return None

        prefixes = []
        if not post_date:
            for prefix in objects['Contents']:
                prefixes.append(prefix)
        else:
            for prefix in objects['Contents']:
                if prefix['LastModified'].replace(tzinfo=None) >= post_date:
                    prefixes.append(prefix)
        return prefixes

    def get_object(self, key):
        try:
            self.log.info('Downloading content from s3 - {}'.format(key))
            resp = self.s3.get_object(Bucket=self.bucket,Key=key)
            return resp['Body'].read()

        except ClientError as ce:
            if ce.response['Error']['Code'] == 'NoSuchKey':
                raise S3NoSuchKey
            else:
                self.log.error('Unexpected Client Error {}'.format(sys.exc_info()))
                raise Exception
        except Exception:
            raise Exception

    def put_object(self, key, body):

        try:

            self.log.info('Uploading content to s3 - {}'.format(key))
            resp = self.s3.put_object(Bucket=self.bucket, Key=key, Body=body)
            return resp

        except ClientError as ce:

                self.log.error('Unexpected Client Error {}'.format(sys.exc_info()))
                raise Exception

        except Exception:
            raise Exception

    def delete_object(self, key):

        try:

            self.log.info('Deleting content to s3 - {}'.format(key))
            resp = self.s3.delete_object(Bucket=self.bucket, Key=key)
            return resp

        except ClientError as ce:

            self.log.error('Unexpected Client Error {}'.format(sys.exc_info()))
            raise Exception

        except Exception:
            raise Exception


# Custom Exception to be raised from an existing ClientError exception in boto3 when the code is 'NoSuchKey'
# in order to pass down to the caller properly and cleanly without the baggage


class S3NoSuchKey(Exception):
    pass
