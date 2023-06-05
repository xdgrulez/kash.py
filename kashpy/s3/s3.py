from kashpy.filesystem import FileSystem
from kashpy.s3.s3_admin import S3Admin
from kashpy.s3.s3_reader import S3Reader
from kashpy.s3.s3_writer import S3Writer

#

class S3(FileSystem):
    def __init__(self, config_str):
        super().__init__("s3s", config_str, [], ["kash"])
    
    #

    def get_admin(self):
        reader = S3Admin(self.kash_config_dict)
        #
        return reader

    #

    def get_reader(self, file, **kwargs):
        reader = S3Reader(self.kash_config_dict, file, **kwargs)
        #
        return reader

    #

    def get_writer(self, file, **kwargs):
        writer = S3Writer(self.kash_config_dict, file, **kwargs)
        #
        return writer
