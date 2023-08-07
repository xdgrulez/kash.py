import os

from kashpy.filesystem.filesystem_reader import FileSystemReader

# Constants

ALL_MESSAGES = -1

#

class LocalReader(FileSystemReader):
    def __init__(self, fileystem_obj, file, **kwargs):
        self.local_config_dict = fileystem_obj.local_config_dict
        self.kash_config_dict = fileystem_obj.kash_config_dict
        #
        self.file_str = file
        #
        (self.key_type_str, self.value_type_str) = fileystem_obj.get_key_value_type_tuple(**kwargs)
        #
        (self.key_value_separator_bytes, self.message_separator_bytes) = fileystem_obj.get_key_value_separator_message_separator_tuple(**kwargs)
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

    def read_bytes(self, _, n_int):
        batch_bytes = self.bufferedReader.read(n_int)
        #
        return batch_bytes
