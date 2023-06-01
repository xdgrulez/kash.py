import os
import tempfile

from minio import Minio

class S3Writer:
    def __init__(self, kash_config_dict, file, **kwargs):
        self.kash_config_dict = kash_config_dict
        #
        self.file_str = file
        #
        self.key_type_str = kwargs["key_type"] if "key_type" in kwargs else "str"
        self.value_type_str = kwargs["value_type"] if "value_type" in kwargs else "str"
        #
        self.key_value_separator_bytes = kwargs["key_value_separator"] if "key_value_separator" in kwargs else None
        self.message_separator_bytes = kwargs["message_separator"] if "message_separator" in kwargs else b"\n"
        #
        temp_path_str = f"/{tempfile.gettempdir()}/kash.py/s3"
        os.makedirs(temp_path_str, exist_ok=True)
        self.temp_file_str = f"{temp_path_str}/{self.file_str}"
        self.bufferedWriter = open(self.temp_file_str, "wb")
        #
        self.minio = Minio("localhost:9000", access_key="admin", secret_key="password", secure=False)

    def __del__(self):
        self.close()

    #

    def close(self):
        self.bufferedWriter.close()
        self.flush()

    #

    def flush(self):
        self.minio.fput_object("minio-test-bucket", self.file_str, self.temp_file_str)
        #
        return self.file_str

    #

    def write(self, value, key=None):
        key_bytes = key_or_value_to_bytes(key, self.key_type_str)
        value_bytes = key_or_value_to_bytes(value, self.value_type_str)
        #
        if key_bytes is None:
            message_bytes = value_bytes + self.message_separator_bytes
        else:
            message_bytes = key_bytes + self.key_value_separator_bytes + value_bytes + self.message_separator_bytes
        #
        self.bufferedWriter.write(message_bytes)

#

def str_to_bytes(str):
    if str is None:
        bytes = None
    else:
        bytes = str.encode("utf-8")
    #
    return bytes


def dict_to_bytes(dict):
    if dict is None:
        bytes = None
    else:
        bytes = str(dict).encode("utf-8")
    #
    return bytes

def key_or_value_to_bytes(key_or_value, type_str):
    if type_str == "bytes":
        bytes = key_or_value
    elif type_str == "str":
        bytes = str_to_bytes(key_or_value)
    elif type_str == "json" or type_str == "dict":
        bytes = dict_to_bytes(key_or_value)
    #
    return bytes
