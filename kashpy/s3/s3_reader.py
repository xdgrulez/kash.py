from kashpy.filesystem_reader import FileSystemReader

from minio import Minio

#

class S3Reader(FileSystemReader):
    def __init__(self, s3_config_dict, kash_config_dict, file, **kwargs):
        self.s3_config_dict = s3_config_dict
        self.kash_config_dict = kash_config_dict
        #
        self.file_str = file
        #
        self.key_type_str = kwargs["key_type"] if "key_type" in kwargs else "str"
        self.value_type_str = kwargs["value_type"] if "value_type" in kwargs else "str"
        self.key_value_separator_bytes = kwargs["key_value_separator"] if "key_value_separator" in kwargs else None
        self.message_separator_bytes = kwargs["message_separator"] if "message_separator" in kwargs else b"\n"
        #
        self.minio = Minio(self.s3_config_dict["endpoint"], access_key=self.s3_config_dict["access.key"], secret_key=self.s3_config_dict["secret.key"], secure=False)
        #
        object = self.minio.stat_object(self.s3_config_dict["bucket.name"], self.file_str)
        self.file_size_int = object.size

    #

    def close(self):
        return self.file_str

    #

    def read_bytes(self, offset_int, buffer_size_int):
        response = self.minio.get_object(self.s3_config_dict["bucket.name"], self.file_str, offset=offset_int, length=buffer_size_int)
        batch_bytes = response.data
        #
        return batch_bytes
