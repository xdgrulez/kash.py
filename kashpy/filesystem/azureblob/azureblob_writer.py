import os
import tempfile

from azure.storage.blob import BlobClient

from kashpy.filesystem.filesystem_writer import FileSystemWriter


class AzureBlobWriter(FileSystemWriter):
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
        temp_path_str = f"/{tempfile.gettempdir()}/kash.py/azureblob"
        os.makedirs(temp_path_str, exist_ok=True)
        self.temp_file_str = f"{temp_path_str}/{self.file_str}"
        self.bufferedWriter = open(self.temp_file_str, "wb")
        #
        self.blobClient = BlobClient.from_connection_string(conn_str=self.azure_blob_config_dict["connection.string"], container_name=self.azure_blob_config_dict["container.name"], blob_name=self.file_str)

    def __del__(self):
        self.close()

    #

    def close(self):
        self.bufferedWriter.close()
        self.flush()

    #

    def flush(self):
        with open(self.temp_file_str, "rb") as bufferedReader:
            self.blobClient.upload_blob(bufferedReader, overwrite=True)
        #
        return self.file_str

    #

    def write_bytes(self, bytes, **kwargs):
        self.bufferedWriter.write(bytes)
