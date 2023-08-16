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

    def files(self, pattern=None, size=False, **kwargs):
        pattern_str_or_str_list = "*" if pattern is None else pattern
        pattern_str_list = [pattern_str_or_str_list] if isinstance(pattern_str_or_str_list, str) else pattern_str_or_str_list
        size_bool = size
        filesize_bool = "filesize" in kwargs and kwargs["filesize"]
        #
        file_str_list = self.admin(list_files) os.listdir(self.root_dir_str)
        file_str_list = [file_str for file_str in file_str_list if any(fnmatch(file_str, pattern_str) for pattern_str in pattern_str_list)]
        #
        if size_bool:
            if filesize_bool:
                file_str_size_int_filesize_int_tuple_dict = {file_str: (self.local_obj.cat(file_str)[1], os.stat(os.path.join(self.root_dir_str, file_str)).st_size) for file_str in file_str_list}
                return file_str_size_int_filesize_int_tuple_dict
            else:
                file_str_size_int_dict = {file_str: self.local_obj.cat(file_str)[1] for file_str in file_str_list}
                return file_str_size_int_dict
        else:
            if filesize_bool:
                file_str_filesize_int_dict = {file_str: os.stat(os.path.join(self.root_dir_str, file_str)).st_size for file_str in file_str_list}
                return file_str_filesize_int_dict
            else:
                file_str_list.sort()
                return file_str_list

    def ls(self, pattern=None, size=False, **kwargs):
        return self.files(pattern, size=size, **kwargs)
    
    def l(self, pattern=None, size=True, **kwargs):
        return self.files(pattern, size=size, **kwargs)

    ll = l

    def files(self, pattern=None, size=False, **kwargs):
        return self.admin.files(pattern, size, **kwargs)

    def exists(self, file):
        file_str = file
        #
        return self.files(file_str) != []

    # Shell.tail
    def get_offsets(self, file_str, n_int):
        offset_int = self.files(file_str, size=True)[file_str] - n_int
        #
        return offset_int, "offset"

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

    # Open
    def openr(self, file, **kwargs):
        reader = self.get_reader(file, **kwargs)
        #
        return reader
    
    def openw(self, file, **kwargs):
        writer = self.get_writer(file, **kwargs)
        #
        return writer
