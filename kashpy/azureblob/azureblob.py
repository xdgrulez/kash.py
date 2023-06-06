from kashpy.filesystem import FileSystem
from kashpy.azureblob.azureblob_admin import AzureBlobAdmin
from kashpy.azureblob.azureblob_reader import AzureBlobReader
from kashpy.azureblob.azureblob_writer import AzureBlobWriter

#

class AzureBlob(FileSystem):
    def __init__(self, config_str):
        super().__init__("azureblobs", config_str, ["azure_blob"], ["kash"])
    
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
