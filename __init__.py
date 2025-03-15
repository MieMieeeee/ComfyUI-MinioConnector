from .minio_connector import InitMinioConnector, MinioUploadFile, MinioCreateBucketIfNotExists, MinioDownloadBucket, \
    MinioUploadFolder
from .utils import add_suffix, add_emoji

NODE_CLASS_MAPPINGS = {
    add_suffix("InitMinioConnector"): InitMinioConnector,
    add_suffix("MinioUploadFile"): MinioUploadFile,
    add_suffix("MinioCreateBucketIfNotExists"): MinioCreateBucketIfNotExists,
    add_suffix("MinioDownloadBucket"): MinioDownloadBucket,
    add_suffix("MinioUploadFolder"): MinioUploadFolder,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    add_suffix("InitMinioConnector"): add_emoji("Init Minio Connector"),
    add_suffix("MinioUploadFile"): add_emoji("Minio Upload File"),
    add_suffix("MinioCreateBucketIfNotExists"): add_emoji("Minio Create Bucket If Not Exists"),
    add_suffix("MinioDownloadBucket"): add_emoji("Minio Download Bucket"),
    add_suffix("MinioUploadFolder"): add_emoji("Minio Upload Folder"),
}

__all__ = ["NODE_CLASS_MAPPINGS", "NODE_DISPLAY_NAME_MAPPINGS"]
