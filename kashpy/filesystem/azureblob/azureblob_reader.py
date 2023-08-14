from azure.storage.blob import BlobClient

from kashpy.filesystem.filesystem_reader import FileSystemReader

# Constants

ALL_MESSAGES = -1

#

class AzureBlobReader(FileSystemReader):
    def __init__(self, azureblob_obj, file, **kwargs):
        super().__init__(azureblob_obj, file, **kwargs)
        #
        self.blobClient = BlobClient.from_connection_string(conn_str=azureblob_obj.azure_blob_config_dict["connection.string"], container_name=azureblob_obj.azure_blob_config_dict["container.name"], blob_name=self.file_str)
        #
        blobProperties_dict = self.blobClient.get_blob_properties()
        self.file_size_int = blobProperties_dict["size"]

    #

    def close(self):
        return self.file_str

    #

    def read_bytes(self, offset_int, n_int):
        storageStreamDownloader = self.blobClient.download_blob(offset=offset_int, length=n_int)
        batch_bytes = storageStreamDownloader.read()
        #
        return batch_bytes
