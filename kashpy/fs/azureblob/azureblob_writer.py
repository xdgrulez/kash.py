import os
import tempfile

from azure.storage.blob import BlobClient

from kashpy.fs.fs_writer import FSWriter


class AzureBlobWriter(FSWriter):
    def __init__(self, azureblob_obj, file, **kwargs):
        super().__init__(azureblob_obj, file, **kwargs)
        #
        temp_path_str = f"/{tempfile.gettempdir()}/kash.py/azureblob"
        os.makedirs(temp_path_str, exist_ok=True)
        self.temp_file_str = f"{temp_path_str}/{self.file_str}"
        self.bufferedWriter = open(self.temp_file_str, "wb")
        #
        self.blobClient = BlobClient.from_connection_string(conn_str=azureblob_obj.azure_blob_config_dict["connection.string"], container_name=azureblob_obj.azure_blob_config_dict["container.name"], blob_name=self.file_str)

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
