import os.path
from minio import Minio
from minio.error import S3Error

import folder_paths
from .utils import mie_log, calculate_file_hash

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
            local_file_hash = calculate_file_hash(file_path)
            remote_file_hash = self.get_object_hash(bucket_name, object_name)
            if local_file_hash == remote_file_hash:
                return mie_log(f"File '{file_path}' has not changed, skipping upload")

            self.client.fput_object(bucket_name, object_name, file_path)
            return mie_log(f"File uploaded to {bucket_name}/{object_name}")
        except S3Error as e:
            return mie_log(f"Failed to upload file: {e}")

    def download(self, bucket_name, object_name, file_path):
        try:
            remote_file_hash = self.get_object_hash(bucket_name, object_name)
            if os.path.exists(file_path):
                local_file_hash = calculate_file_hash(file_path)
                if local_file_hash == remote_file_hash:
                    return mie_log(f"File '{file_path}' has not changed, skipping download")

            self.client.fget_object(bucket_name, object_name, file_path)
            return mie_log(f"File downloaded from {bucket_name}/{object_name} to {file_path}")
        except S3Error as e:
            return mie_log(f"Failed to download file: {e}")

    def get_object_hash(self, bucket_name, object_name):
        try:
            stat = self.client.stat_object(bucket_name, object_name)
            return stat.etag
        except S3Error as e:
            return None


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
                "file_path": ("STRING", {"default": ""}),
                "object_name": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("STRING",)
    RETURN_NAMES = ("log",)
    FUNCTION = "execute"
    DESCRIPTION = """
    For safety, only allow to upload folder under ComfyUI directory, such as temp/abc, input/def, output/ghi etc.
    """

    CATEGORY = MY_CATEGORY

    def execute(self, minio_connector, bucket_name, object_name, file_path):
        if not bucket_name or not file_path:
            raise Exception("bucket_name or file_path is empty")

        file_path = os.path.join(folder_paths.base_path, file_path)
        if not object_name:
            object_name = os.path.basename(file_path)
        return minio_connector.upload(bucket_name, object_name, file_path),


class MinioUploadFolder(object):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "minio_connector": ("MINIO_CONNECTOR",),
                "bucket_name": ("STRING", {"default": ""}),
                "folder_path": ("STRING", {"default": "output"}),
            },
        }

    RETURN_TYPES = ("STRING",)
    RETURN_NAMES = ("log",)
    FUNCTION = "execute"
    DESCRIPTION = """
    For safety, only allow to upload folder under ComfyUI directory, such as temp/abc, input/def, output/ghi etc.
    """

    CATEGORY = MY_CATEGORY

    def execute(self, minio_connector, bucket_name, folder_path):
        if not bucket_name or not folder_path:
            raise Exception("bucket_name or folder_path is empty")

        folder_path = os.path.join(folder_paths.base_path, folder_path)

        logs = []
        for root, _, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                object_name = os.path.relpath(file_path, folder_path)
                log = minio_connector.upload(bucket_name, object_name, file_path)
                logs.append(log)

        return "\n".join(logs),


class MinioDownloadBucket(object):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "minio_connector": ("MINIO_CONNECTOR",),
                "bucket_name": ("STRING", {"default": ""}),
                "folder_path": ("STRING", {"default": "temp"}),
            },
        }

    RETURN_TYPES = ("STRING",)
    RETURN_NAMES = ("log",)
    FUNCTION = "execute"
    DESCRIPTION = """
    For safety, only allow to download files under ComfyUI directory, such as temp, input, output etc.
    """

    CATEGORY = MY_CATEGORY

    def execute(self, minio_connector, bucket_name, folder_path):
        if not bucket_name or not folder_path:
            raise Exception("bucket_name or folder_path is empty")

        folder_path = os.path.join(folder_paths.base_path, folder_path)
        folder_path = os.path.join(folder_path, bucket_name)
        os.makedirs(folder_path, exist_ok=True)

        logs = []
        objects = minio_connector.client.list_objects(bucket_name, recursive=True)
        for obj in objects:
            file_path = os.path.join(folder_path, obj.object_name)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            log = minio_connector.download(bucket_name, obj.object_name, file_path)
            logs.append(log)

        return "\n".join(logs),
