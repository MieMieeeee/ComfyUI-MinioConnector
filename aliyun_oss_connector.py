import os
import oss2
import glob

import folder_paths
from .utils import mie_log, calculate_file_hash

MY_CATEGORY = "🐑 Minio-Connector/🐑 Aliyun OSS Connector"


class AliyunOSSConnector:
    def __init__(self, endpoint, access_key_id, access_key_secret, bucket_name):
        self.bucket = oss2.Bucket(oss2.Auth(access_key_id, access_key_secret), endpoint, bucket_name)
        mie_log("Aliyun OSS client initialized")

    def create_bucket(self, bucket_name):
        try:
            service = oss2.Service(oss2.Auth(self.bucket.auth.access_key_id, self.bucket.auth.access_key_secret),
                                   self.bucket.endpoint)
            if bucket_name not in [b.name for b in oss2.BucketIterator(service)]:
                self.bucket.create_bucket(bucket_name)
                return mie_log(f"Bucket '{bucket_name}' created successfully")
            else:
                return mie_log(f"Bucket '{bucket_name}' already exists")
        except oss2.exceptions.OssError as e:
            return mie_log(f"Failed to create bucket: {e}")

    def upload(self, object_name, file_path):
        try:
            local_file_hash = calculate_file_hash(file_path)
            remote_file_hash = self.get_object_hash(object_name)
            if local_file_hash == remote_file_hash:
                return mie_log(f"File '{file_path}' has not changed, skipping upload")

            self.bucket.put_object_from_file(object_name, file_path)
            return mie_log(f"File uploaded to {object_name}")
        except oss2.exceptions.OssError as e:
            return mie_log(f"Failed to upload file: {e}")

    def download(self, object_name, file_path):
        try:
            remote_file_hash = self.get_object_hash(object_name)
            if os.path.exists(file_path):
                local_file_hash = calculate_file_hash(file_path)
                if local_file_hash == remote_file_hash:
                    return mie_log(f"File '{file_path}' has not changed, skipping download")

            self.bucket.get_object_to_file(object_name, file_path)
            return mie_log(f"File downloaded from {object_name} to {file_path}")
        except oss2.exceptions.OssError as e:
            return mie_log(f"Failed to download file: {e}")

    def get_object_hash(self, object_name):
        try:
            head = self.bucket.head_object(object_name)
            return head.etag
        except oss2.exceptions.OssError as e:
            return None


class InitAliyunOSSConnector(object):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "endpoint": ("STRING", {"default": ""}),
                "access_key_id": ("STRING", {"default": ""}),
                "access_key_secret": ("STRING", {"default": ""}),
                "bucket_name": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("ALIYUN_OSS_CONNECTOR",)
    RETURN_NAMES = ("aliyun_oss_connector",)
    FUNCTION = "execute"

    CATEGORY = MY_CATEGORY

    def execute(self, endpoint, access_key_id, access_key_secret, bucket_name):
        client = AliyunOSSConnector(endpoint, access_key_id, access_key_secret, bucket_name)
        mie_log("Aliyun OSS client initialized")
        return client,


class AliyunOSSUploadFile(object):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "aliyun_oss_connector": ("ALIYUN_OSS_CONNECTOR",),
                "object_name": ("STRING", {"default": ""}),
                "file_path": ("STRING", {"default": ""}),
            },
        }

    RETURN_TYPES = ("STRING",)
    RETURN_NAMES = ("log",)
    FUNCTION = "execute"
    DESCRIPTION = """
    For safety, only allow to upload folder under ComfyUI directory, such as temp/abc, input/def, output/ghi etc.
    """

    CATEGORY = MY_CATEGORY

    def execute(self, aliyun_oss_connector, object_name, file_path):
        if not file_path:
            raise Exception("object_name or file_path is empty")

        file_paths = glob.glob(os.path.join(folder_paths.base_path, file_path))
        if not file_paths:
            raise Exception(f"No files matched the pattern: {file_path}")

        logs = []
        for path in file_paths:
            if not object_name:
                object_name = os.path.basename(path)
            log = aliyun_oss_connector.upload(object_name, path)
            logs.append(log)

        return "\n".join(logs),


class AliyunOSSUploadFolder(object):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "aliyun_oss_connector": ("ALIYUN_OSS_CONNECTOR",),
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

    def execute(self, aliyun_oss_connector, object_name, file_path):
        if not file_path:
            raise Exception("object_name or file_path is empty")

        file_paths = glob.glob(os.path.join(folder_paths.base_path, file_path))
        if not file_paths:
            raise Exception(f"No files matched the pattern: {file_path}")

        logs = []
        for path in file_paths:
            current_object_name = object_name or os.path.basename(path)
            log = aliyun_oss_connector.upload(current_object_name, path)
            logs.append(log)  # Append the log message directly

        return "\n".join(logs),


class AliyunOSSDownloadBucket(object):
    @classmethod
    def INPUT_TYPES(cls):
        return {
            "required": {
                "aliyun_oss_connector": ("ALIYUN_OSS_CONNECTOR",),
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

    def execute(self, aliyun_oss_connector, folder_path):
        if not folder_path:
            raise Exception("folder_path is empty")

        folder_path = os.path.join(folder_paths.base_path, folder_path)
        os.makedirs(folder_path, exist_ok=True)

        logs = []
        for obj in oss2.ObjectIterator(aliyun_oss_connector.bucket):
            file_path = os.path.join(folder_path, obj.key)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            log = aliyun_oss_connector.download(obj.key, file_path)
            logs.append(log)

        return "\n".join(logs),
