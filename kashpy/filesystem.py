from kashpy.storage import Storage

#

class FileSystem(Storage):
    def __init__(self, dir_str, config_str, mandatory_section_str_list, optional_section_str_list):
        super().__init__(dir_str, config_str, mandatory_section_str_list, optional_section_str_list)
        #
        self.dir_str = dir_str
        self.config_str = config_str
        #
        self.kash_config_dict = self.config_dict["kash"]
        #
        if "progress.num.messages" not in self.kash_config_dict:
            self.progress_num_messages(1000)
        else:
            self.progress_num_messages(int(self.kash_config_dict["progress.num.messages"]))
        #
        if "read.buffer.size" not in self.kash_config_dict:
            self.read_buffer_size(10000)
        else:
            self.read_buffer_size(int(self.kash_config_dict["read.buffer.size"]))


    #

    def get_set_config(self, config_key_str, new_value=None):
        if new_value is not None:
            self.kash_config_dict[config_key_str] = new_value
        #
        return new_value

    def progress_num_messages(self, new_value=None): # int
        return self.get_set_config("progress.num.messages", new_value)

    def read_buffer_size(self, new_value=None): # int
        return self.get_set_config("read.buffer.size", new_value)

    # Open

    def openr(self, file, key_type="str", value_type="str", key_value_separator=None, message_separator=b"\n"):
        reader = self.get_reader(file, key_type, value_type, key_value_separator, message_separator)
        #
        return reader
    
    def openw(self, file, key_type="str", value_type="str", key_value_separator=None, message_separator=b"\n", overwrite=True):
        writer = self.get_writer(file, key_type, value_type, key_value_separator, message_separator, overwrite)
        #
        return writer
