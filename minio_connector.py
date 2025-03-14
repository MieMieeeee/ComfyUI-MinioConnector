import os.path

from minio import Minio
from minio.error import S3Error
from .utils import mie_log

MY_CATEGORY = "🐑 Minio-Connector/🐑 Minio Connector"


class MinioConnector:
    def __init__(self, endpoint, access_key, secret_key, secure=True):
        self.client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
        mie_log("Minio client initialized")

    def create_bucket(self, bucket_name):
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                return mie_log(f"Bucket '{bucket_name}' created successfully")
            else:
                return mie_log(f"Bucket '{bucket_name}' already exists")
        except S3Error as e:
            return mie_log(f"Failed to create bucket: {e}")

    def upload(self, bucket_name, object_name, file_path):
        try:
            self.client.fput_object(bucket_name, object_name, file_path)
            return mie_log(f"Image uploaded to {bucket_name}/{object_name}")
        except S3Error as e:
            return mie_log(f"Failed to upload image: {e}")

    def download(self, bucket_name, object_name, file_path):
        try:
            self.client.fget_object(bucket_name, object_name, file_path)
            return mie_log(f"Image downloaded from {bucket_name}/{object_name} to {file_path}")
        except S3Error as e:
            return mie_log(f"Failed to download image: {e}")


class InitMinioConnector(object):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "endpoint": ("STRING", {"default": "192.168.31.200:9000"}),
                "access_key": ("STRING", {"default": ""}),
                "secret_key": ("STRING", {"default": ""}),
                "secure": ("BOOLEAN", {"default": False}),
            },
        }

    RETURN_TYPES = ("MINIO_CONNECTOR",)
    RETURN_NAMES = ("minio_connector",)
    FUNCTION = "execute"

    CATEGORY = MY_CATEGORY

    def execute(self, endpoint, access_key, secret_key, secure):
        client = MinioConnector(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
        mie_log("Minio client initialized")
        return client,


class MinioCreateBucketIfNotExists(object):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "minio_connector": ("MINIO_CONNECTOR",),
                "bucket_name": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("STRING", "STRING")
    RETURN_NAMES = ("bucket_name", "log")
    FUNCTION = "execute"

    CATEGORY = MY_CATEGORY

    def execute(self, minio_connector, bucket_name):
        if not bucket_name:
            raise Exception("bucket_name is empty")
        return bucket_name, minio_connector.create_bucket(bucket_name),


class MinioUploadFile(object):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "minio_connector": ("MINIO_CONNECTOR",),
                "bucket_name": ("STRING", {"default": ""}),
                "object_name": ("STRING", {"default": ""}),
                "file_path": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("STRING",)
    RETURN_NAMES = ("log",)
    FUNCTION = "execute"

    CATEGORY = MY_CATEGORY

    def execute(self, minio_connector, bucket_name, object_name, file_path):
        if not bucket_name or not file_path:
            raise Exception("bucket_name or file_path is empty")
        if not object_name:
            object_name = os.path.basename(file_path)
        return minio_connector.upload(bucket_name, object_name, file_path),
