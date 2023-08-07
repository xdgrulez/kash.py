from azure.storage.blob import BlobClient

from kashpy.filesystem.filesystem_reader import FileSystemReader

# Constants

ALL_MESSAGES = -1

#

class AzureBlobReader(FileSystemReader):
    def __init__(self, fileystem_obj, file, **kwargs):
        self.azure_blob_config_dict = fileystem_obj.azure_blob_config_dict
        self.kash_config_dict = fileystem_obj.kash_config_dict
        #
        self.file_str = file
        #
        (self.key_type_str, self.value_type_str) = fileystem_obj.get_key_value_type_tuple(**kwargs)
        #
        (self.key_value_separator_bytes, self.message_separator_bytes) = fileystem_obj.get_key_value_separator_message_separator_tuple(**kwargs)
        #
        self.blobClient = BlobClient.from_connection_string(conn_str=self.azure_blob_config_dict["connection.string"], container_name=self.azure_blob_config_dict["container.name"], blob_name=self.file_str)
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
