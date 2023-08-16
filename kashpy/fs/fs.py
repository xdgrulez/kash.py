from kashpy.storage import Storage

#

class FS(Storage):
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
        # all kash section
        if "message.separator" not in self.kash_config_dict:
            self.message_separator(b"\n")
        else:
            self.message_separator(bytes(self.kash_config_dict["message.separator"]))
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

    # all

    def message_separator(self, new_value=None): # str
        return self.get_set_config("message.separator", new_value, dict=self.s3_config_dict)

    #

    def files(self, pattern=None, size=False, **kwargs):
        return self.admin.files(pattern, size, **kwargs)
    
    ls = files

    def l(self, pattern=None, size=True, **kwargs):
        return self.admin.files(pattern=pattern, size=size, **kwargs)

    ll = l

    def exists(self, file):
        file_str = file
        #
        return self.admin.files(file_str) != []

    #

    def create(self, file, **kwargs):
        file_str = file
        #
        writer = self.openw(file_str, **kwargs)
        writer.close()
        #
        return file_str
    
    touch = create

    def delete(self, pattern):
        return self.admin.delete(pattern)

    rm = delete

    def partitions(self, pattern=None, verbose=False):
        return self.admin.partitions(pattern, verbose)

    # Open
    def openr(self, file, **kwargs):
        reader = self.get_reader(file, **kwargs)
        #
        return reader
    
    def openw(self, file, **kwargs):
        writer = self.get_writer(file, **kwargs)
        #
        return writer
