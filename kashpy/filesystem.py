from kashpy.storage import Storage
from kashpy.helpers import is_interactive

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
        if "verbose" not in self.kash_config_dict:
            verbose_int = 1 if is_interactive() else 0
            self.verbose(verbose_int)
        else:
            self.verbose(int(self.kash_config_dict["verbose"]))

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
    
    def verbose(self, new_value=None): # int
        return self.get_set_config("verbose", new_value)

    # Open
    def openr(self, file, **kwargs):
        reader = self.get_reader(file, **kwargs)
        #
        print(file)
        #
        return reader
    
    def openw(self, file, **kwargs):
        writer = self.get_writer(file, **kwargs)
        #
        print(file)
        #
        return writer
