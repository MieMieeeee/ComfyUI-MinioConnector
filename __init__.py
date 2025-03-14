from .minio_connector import InitMinioConnector, MinioUploadFile, MinioCreateBucketIfNotExists
from .utils import add_suffix, add_emoji

NODE_CLASS_MAPPINGS = {
    add_suffix("InitMinioConnector"): InitMinioConnector,
    add_suffix("MinioUploadFile"): MinioUploadFile,
    add_suffix("MinioCreateBucketIfNotExists"): MinioCreateBucketIfNotExists,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    add_suffix("InitMinioConnector"): add_emoji("Init Minio Connector"),
    add_suffix("MinioUploadFile"): add_emoji("Minio Upload File"),
    add_suffix("MinioCreateBucketIfNotExists"): add_emoji("Minio Create Bucket If Not Exists"),
}

__all__ = ["NODE_CLASS_MAPPINGS", "NODE_DISPLAY_NAME_MAPPINGS"]
