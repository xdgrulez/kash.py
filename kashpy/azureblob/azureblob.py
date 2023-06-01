from kashpy.filesystem import FileSystem
from kashpy.azureblob.azureblob_reader import AzureBlobReader
from kashpy.azureblob.azureblob_writer import AzureBlobWriter

#

class AzureBlob(FileSystem):
    def __init__(self, config_str):
        super().__init__("azureblobs", config_str, [], ["kash"])
    
    #

    def get_reader(self, file, **kwargs):
        reader = AzureBlobReader(self.kash_config_dict, file, **kwargs)
        #
        return reader

    #

    def get_writer(self, file, **kwargs):
        writer = AzureBlobWriter(self.kash_config_dict, file, **kwargs)
        #
        return writer
