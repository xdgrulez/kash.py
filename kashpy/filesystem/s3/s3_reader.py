from kashpy.filesystem.filesystem_reader import FileSystemReader

from minio import Minio

#

class S3Reader(FileSystemReader):
    def __init__(self, fileystem_obj, file, **kwargs):
        self.s3_config_dict = fileystem_obj.s3_config_dict
        self.kash_config_dict = fileystem_obj.kash_config_dict
        #
        self.file_str = file
        #
        (self.key_type_str, self.value_type_str) = fileystem_obj.get_key_value_type_tuple(**kwargs)
        #
        (self.key_value_separator_bytes, self.message_separator_bytes) = fileystem_obj.get_key_value_separator_message_separator_tuple(**kwargs)
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
