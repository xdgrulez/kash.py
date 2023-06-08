import os

from kashpy.filesystem_reader import FileSystemReader

# Constants

ALL_MESSAGES = -1

#

class LocalReader(FileSystemReader):
    def __init__(self, local_config_dict, kash_config_dict, file, **kwargs):
        self.local_config_dict = local_config_dict
        self.kash_config_dict = kash_config_dict
        #
        self.file_str = file
        #
        self.key_type_str = kwargs["key_type"] if "key_type" in kwargs else "str"
        self.value_type_str = kwargs["value_type"] if "value_type" in kwargs else "str"
        self.key_value_separator_bytes = kwargs["key_value_separator"] if "key_value_separator" in kwargs else None
        self.message_separator_bytes = kwargs["message_separator"] if "message_separator" in kwargs else b"\n"
        #
        self.bufferedReader = open(file, "rb")
        #
        self.file_size_int = os.stat(self.file_str).st_size

    def __del__(self):
        self.close()

    #

    def close(self):
        self.bufferedReader.close()
        #
        return self.file_str

    #

    def read_bytes(self, _, buffer_size_int):
        batch_bytes = self.bufferedReader.read(buffer_size_int)
        #
        return batch_bytes
