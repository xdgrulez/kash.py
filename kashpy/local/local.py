from kashpy.filesystem import FileSystem
from kashpy.local.local_reader import LocalReader
from kashpy.local.local_writer import LocalWriter

#

class Local(FileSystem):
    def __init__(self, config_str):
        super().__init__("locals", config_str, [], ["kash"])
    
    #

    def get_reader(self, file, key_type="str", value_type="str", key_value_separator=None, message_separator=b"\n"):
        reader = LocalReader(self.kash_config_dict, self.verbose_int, file, key_type, value_type, key_value_separator, message_separator)
        #
        return reader

    #

    def get_writer(self, file, key_type="str", value_type="str", key_value_separator=None, message_separator=b"\n", overwrite=True):
        writer = LocalWriter(self.kash_config_dict, self.verbose_int, file, key_type, value_type, key_value_separator, message_separator, overwrite)
        #
        return writer
