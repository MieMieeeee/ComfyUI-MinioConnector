from .minio_connector import InitMinioConnector, MinioUploadFile, MinioCreateBucketIfNotExists, MinioDownloadBucket, \
    MinioUploadFolder
from .aliyun_oss_connector import InitAliyunOSSConnector, AliyunOSSUploadFile, \
    AliyunOSSDownloadBucket, AliyunOSSUploadFolder
from .utils import add_suffix, add_emoji

NODE_CLASS_MAPPINGS = {
    add_suffix("InitMinioConnector"): InitMinioConnector,
    add_suffix("MinioUploadFile"): MinioUploadFile,
    add_suffix("MinioCreateBucketIfNotExists"): MinioCreateBucketIfNotExists,
    add_suffix("MinioDownloadBucket"): MinioDownloadBucket,
    add_suffix("MinioUploadFolder"): MinioUploadFolder,

    add_suffix("InitAliyunOSSConnector"): InitAliyunOSSConnector,
    add_suffix("AliyunOSSUploadFile"): AliyunOSSUploadFile,
    add_suffix("AliyunOSSDownloadBucket"): AliyunOSSDownloadBucket,
    add_suffix("AliyunOSSUploadFolder"): AliyunOSSUploadFolder,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    add_suffix("InitMinioConnector"): add_emoji("Init Minio Connector"),
    add_suffix("MinioUploadFile"): add_emoji("Minio Upload File"),
    add_suffix("MinioCreateBucketIfNotExists"): add_emoji("Minio Create Bucket If Not Exists"),
    add_suffix("MinioDownloadBucket"): add_emoji("Minio Download Bucket"),
    add_suffix("MinioUploadFolder"): add_emoji("Minio Upload Folder"),

    add_suffix("InitAliyunOSSConnector"): add_emoji("Init Aliyun OSS Connector"),
    add_suffix("AliyunOSSUploadFile"): add_emoji("Aliyun OSS Upload File"),
    add_suffix("AliyunOSSDownloadBucket"): add_emoji("Aliyun OSS Download Bucket"),
    add_suffix("AliyunOSSUploadFolder"): add_emoji("Aliyun OSS Upload Folder"),
}

__all__ = ["NODE_CLASS_MAPPINGS", "NODE_DISPLAY_NAME_MAPPINGS"]
