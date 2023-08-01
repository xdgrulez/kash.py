import os
import tempfile

from minio import Minio

from kashpy.filesystem.filesystem_writer import FileSystemWriter

class S3Writer(FileSystemWriter):
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
        temp_path_str = f"/{tempfile.gettempdir()}/kash.py/s3"
        os.makedirs(temp_path_str, exist_ok=True)
        self.temp_file_str = f"{temp_path_str}/{self.file_str}"
        self.bufferedWriter = open(self.temp_file_str, "wb")
        #
        self.minio = Minio(self.s3_config_dict["endpoint"], access_key=self.s3_config_dict["access.key"], secret_key=self.s3_config_dict["secret.key"], secure=False)

    def __del__(self):
        self.close()

    #

    def close(self):
        self.bufferedWriter.close()
        self.flush()

    #

    def flush(self):
        self.minio.fput_object(self.s3_config_dict["bucket.name"], self.file_str, self.temp_file_str)
        #
        return self.file_str

    #

    def write_bytes(self, bytes, **kwargs):
        self.bufferedWriter.write(bytes)
