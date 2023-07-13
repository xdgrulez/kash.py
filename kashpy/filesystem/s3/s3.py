from kashpy.filesystem.filesystem import FileSystem
from kashpy.filesystem.s3.s3_admin import S3Admin
from kashpy.filesystem.s3.s3_reader import S3Reader
from kashpy.filesystem.s3.s3_writer import S3Writer

#

class S3(FileSystem):
    def __init__(self, config_str):
        super().__init__("s3s", config_str, ["s3"], [])
    
    #

    def get_admin(self):
        reader = S3Admin(self.s3_config_dict, self.kash_config_dict)
        #
        return reader

    #

    def get_reader(self, file, **kwargs):
        reader = S3Reader(self.s3_config_dict, self.kash_config_dict, file, **kwargs)
        #
        return reader

    #

    def get_writer(self, file, **kwargs):
        writer = S3Writer(self.s3_config_dict, self.kash_config_dict, file, **kwargs)
        #
        return writer
