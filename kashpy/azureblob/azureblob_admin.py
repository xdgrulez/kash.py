from fnmatch import fnmatch

from azure.storage.blob import BlobServiceClient

#

class AzureBlobAdmin:
    def __init__(self, kash_config_dict):
        self.kash_config_dict = kash_config_dict
        #
        blobServiceClient = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;")
        self.containerClient = blobServiceClient.get_container_client("test")

    #

    def list(self, pattern=None, size=False):
        pattern_str_or_str_list = "*" if pattern is None else pattern
        pattern_str_list = [pattern_str_or_str_list] if isinstance(pattern_str_or_str_list, str) else pattern_str_or_str_list
        #
        if size:
            blobProperties_dict_itemPaged = self.containerClient.list_blobs()
            #
            blob_str_size_int_tuple_list = [(blobProperties_dict["name"], blobProperties_dict["size"]) for blobProperties_dict in blobProperties_dict_itemPaged if any(fnmatch(blobProperties_dict["name"], pattern_str) for pattern_str in pattern_str_list)]
            #
            blob_str_size_int_tuple_list.sort()
            #
            return blob_str_size_int_tuple_list
        else:
            blob_str_itemPaged = self.containerClient.list_blob_names()
            blob_str_list = [blob_str for blob_str in blob_str_itemPaged if any(fnmatch(blob_str, pattern_str) for pattern_str in pattern_str_list)]
            #
            blob_str_list.sort()
            #
            return blob_str_list

    #

    def delete(self, pattern=None):
        pattern_str_or_str_list = [] if pattern is None else pattern
        pattern_str_list = [pattern_str_or_str_list] if isinstance(pattern_str_or_str_list, str) else pattern_str_or_str_list
        #
        blob_str_itemPaged = self.containerClient.list_blob_names()
        blob_str_list = [blob_str for blob_str in blob_str_itemPaged if any(fnmatch(blob_str, pattern_str) for pattern_str in pattern_str_list)]
        for blob_str in blob_str_list:
            self.containerClient.delete_blob(blob_str)
        #
        return blob_str_list
