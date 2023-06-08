from azure.storage.blob import BlobClient

from kashpy.filesystem_reader import FileSystemReader

# Constants

ALL_MESSAGES = -1

#

class AzureBlobReader(FileSystemReader):
    def __init__(self, azure_blob_config_dict, kash_config_dict, file, **kwargs):
        self.azure_blob_config_dict = azure_blob_config_dict
        self.kash_config_dict = kash_config_dict
        #
        self.file_str = file
        #
        self.key_type_str = kwargs["key_type"] if "key_type" in kwargs else "str"
        self.value_type_str = kwargs["value_type"] if "value_type" in kwargs else "str"
        self.key_value_separator_bytes = kwargs["key_value_separator"] if "key_value_separator" in kwargs else None
        self.message_separator_bytes = kwargs["message_separator"] if "message_separator" in kwargs else b"\n"
        #
        self.blobClient = BlobClient.from_connection_string(conn_str=self.azure_blob_config_dict["connection.string"], container_name=self.azure_blob_config_dict["container.name"], blob_name=self.file_str)
        #
        blobProperties_dict = self.blobClient.get_blob_properties()
        self.file_size_int = blobProperties_dict["size"]

    #

    def close(self):
        return self.file_str

    #

    def read_bytes(self, offset_int, buffer_size_int):
        storageStreamDownloader = self.blobClient.download_blob(offset=offset_int, length=buffer_size_int)
        batch_bytes = storageStreamDownloader.read()
        #
        return batch_bytes
