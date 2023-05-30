from kashpy.filesystem import FileSystem
from kashpy.local.local_reader import LocalReader
from kashpy.local.local_writer import LocalWriter

#

class Local(FileSystem):
    def __init__(self, config_str):
        super().__init__("locals", config_str, [], ["kash"])
    
    #

    def get_reader(self, file, **kwargs):
        reader = LocalReader(self.kash_config_dict, file, **kwargs)
        #
        return reader

    #

    def get_writer(self, file, **kwargs):
        writer = LocalWriter(self.kash_config_dict, file, **kwargs)
        #
        return writer
