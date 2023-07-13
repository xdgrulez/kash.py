from kashpy.filesystem.filesystem import FileSystem
from kashpy.filesystem.azureblob.azureblob_admin import AzureBlobAdmin
from kashpy.filesystem.azureblob.azureblob_reader import AzureBlobReader
from kashpy.filesystem.azureblob.azureblob_writer import AzureBlobWriter

#

class AzureBlob(FileSystem):
    def __init__(self, config_str):
        super().__init__("azureblobs", config_str, ["azure_blob"], [])
    
    #

    def get_admin(self):
        admin = AzureBlobAdmin(self.azure_blob_config_dict, self.kash_config_dict)
        #
        return admin

    #

    def get_reader(self, file, **kwargs):
        reader = AzureBlobReader(self.azure_blob_config_dict, self.kash_config_dict, file, **kwargs)
        #
        return reader

    #

    def get_writer(self, file, **kwargs):
        writer = AzureBlobWriter(self.azure_blob_config_dict, self.kash_config_dict, file, **kwargs)
        #
        return writer
