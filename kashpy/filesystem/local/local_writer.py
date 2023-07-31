from kashpy.filesystem.filesystem_writer import FileSystemWriter


class LocalWriter(FileSystemWriter):
    def __init__(self, local_config_dict, kash_config_dict, file, **kwargs):
        self.local_config_dict = local_config_dict
        self.kash_config_dict = kash_config_dict
        #
        self.file_str = file
        #
        self.key_type_str = kwargs["key_type"] if "key_type" in kwargs else "str"
        self.value_type_str = kwargs["value_type"] if "value_type" in kwargs else "str"
        #
        self.key_value_separator_bytes = kwargs["key_value_separator"] if "key_value_separator" in kwargs else b"::"
        self.message_separator_bytes = kwargs["message_separator"] if "message_separator" in kwargs else b"\n"
        self.overwrite_bool = kwargs["overwrite"] if "overwrite" in kwargs else True
        #
        mode_str = "wb" if self.overwrite_bool else "ab"
        self.bufferedWriter = open(file, mode_str)

    def __del__(self):
        self.close()

    #

    def close(self):
        self.bufferedWriter.close()

    #

    def write_bytes(self, bytes, **kwargs):
        self.bufferedWriter.write(bytes)
