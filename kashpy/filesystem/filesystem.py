from kashpy.storage import Storage

#

class FileSystem(Storage):
    def __init__(self, dir_str, config_str, mandatory_section_str_list, optional_section_str_list):
        super().__init__(dir_str, config_str, mandatory_section_str_list, optional_section_str_list)
        #
        self.dir_str = dir_str
        self.config_str = config_str
        # azure_blob
        if "azure_blob" in mandatory_section_str_list:
            self.azure_blob_config_dict = self.config_dict["azure_blob"]
            #
            if "container.name" not in self.azure_blob_config_dict:
                self.container_name("test")
            else:
                self.container_name(str(self.azure_blob_config_dict["container.name"]))
        else:
            self.azure_blob_config_dict = None
        # local
        if "local" in mandatory_section_str_list:
            self.local_config_dict = self.config_dict["local"]
            #
            if "root.dir" not in self.local_config_dict:
                self.root_dir(".")
            else:
                self.root_dir(str(self.local_config_dict["root.dir"]))
        else:
            self.local_config_dict = None
        # s3
        if "s3" in mandatory_section_str_list:
            self.s3_config_dict = self.config_dict["s3"]
            #
            if "bucket.name" not in self.s3_config_dict:
                self.bucket_name("minio-test-bucket")
            else:
                self.bucket_name(str(self.s3_config_dict["bucket.name"]))
        else:
            self.s3_config_dict = None
        #
        self.admin = self.get_admin()

    # azure_blob

    def container_name(self, new_value=None): # str
        return self.get_set_config("container.name", new_value, dict=self.azure_blob_config_dict)

    # local
    
    def root_dir(self, new_value=None): # str
        return self.get_set_config("root.dir", new_value, dict=self.local_config_dict)

    # s3
    
    def bucket_name(self, new_value=None): # str
        return self.get_set_config("bucket.name", new_value, dict=self.s3_config_dict)

    #

    def ls(self, pattern=None):
        return self.list(pattern, size=False)
    
    def l(self, pattern=None):
        return self.list(pattern, size=True)

    ll = l

    def list(self, pattern, size):
        return self.admin.list(pattern, size)

    def exists(self, file):
        file_str = file
        #
        return self.list(file_str) != []
    #

    def delete(self, pattern):
        return self.admin.delete(pattern)

    rm = delete

    # Shared

    def get_key_value_separator_message_separator_tuple(self, **kwargs):
        key_value_separator_bytes = bytes(kwargs["key_value_separator"], encoding="utf-8") if "key_value_separator" in kwargs else b"::"
        message_separator_bytes = bytes(kwargs["message_separator"], encoding="utf-8") if "message_separator" in kwargs else b"\n"
        #
        return (key_value_separator_bytes, message_separator_bytes)

    # Open
    def openr(self, file, **kwargs):
        reader = self.get_reader(file, **kwargs)
        #
        return reader
    
    def openw(self, file, **kwargs):
        writer = self.get_writer(file, **kwargs)
        #
        return writer
