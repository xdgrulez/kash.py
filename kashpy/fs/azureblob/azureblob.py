from kashpy.fs.fs import FS
from kashpy.fs.azureblob.azureblob_admin import AzureBlobAdmin
from kashpy.fs.azureblob.azureblob_reader import AzureBlobReader
from kashpy.fs.azureblob.azureblob_writer import AzureBlobWriter

#

class AzureBlob(FS):
    def __init__(self, config_str):
        super().__init__("azureblobs", config_str, ["azure_blob"], [])
    
    #

    def get_admin(self):
        admin = AzureBlobAdmin(self)
        #
        return admin

    #

    def get_reader(self, file, **kwargs):
        reader = AzureBlobReader(self, file, **kwargs)
        #
        return reader

    #

    def get_writer(self, file, **kwargs):
        writer = AzureBlobWriter(self, file, **kwargs)
        #
        return writer
