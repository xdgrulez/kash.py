from kashpy.storage import Storage
from kashpy.helpers import is_interactive

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
        self.kash_config_dict = self.config_dict["kash"]
        #
        if "progress.num.messages" not in self.kash_config_dict:
            self.progress_num_messages(1000)
        else:
            self.progress_num_messages(int(self.kash_config_dict["progress.num.messages"]))
        #
        if "read.buffer.size" not in self.kash_config_dict:
            self.read_buffer_size(10000)
        else:
            self.read_buffer_size(int(self.kash_config_dict["read.buffer.size"]))
        #
        if "verbose" not in self.kash_config_dict:
            verbose_int = 1 if is_interactive() else 0
            self.verbose(verbose_int)
        else:
            self.verbose(int(self.kash_config_dict["verbose"]))
        #
        self.admin = self.get_admin()

    #

    def get_set_config(self, config_key_str, new_value=None, dict=None):
        if new_value is not None:
            dict = self.kash_config_dict if dict is None else dict
            dict[config_key_str] = new_value
        #
        return new_value

    # azure_blob

    def container_name(self, new_value=None): # str
        return self.get_set_config("container.name", new_value, dict=self.azure_blob_config_dict)

    # local
    
    def root_dir(self, new_value=None): # str
        return self.get_set_config("root.dir", new_value, dict=self.local_config_dict)

    # s3
    
    def bucket_name(self, new_value=None): # str
        return self.get_set_config("bucket.name", new_value, dict=self.s3_config_dict)

    # kash

    def progress_num_messages(self, new_value=None): # int
        return self.get_set_config("progress.num.messages", new_value)

    def read_buffer_size(self, new_value=None): # int
        return self.get_set_config("read.buffer.size", new_value)
    
    def verbose(self, new_value=None): # int
        return self.get_set_config("verbose", new_value)

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

    # Open
    def openr(self, file, **kwargs):
        reader = self.get_reader(file, **kwargs)
        #
        return reader
    
    def openw(self, file, **kwargs):
        writer = self.get_writer(file, **kwargs)
        #
        return writer
