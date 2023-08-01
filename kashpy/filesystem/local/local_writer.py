from kashpy.filesystem.filesystem_writer import FileSystemWriter


class LocalWriter(FileSystemWriter):
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
