from kashpy.fs.fs import FS
from kashpy.fs.s3.s3_admin import S3Admin
from kashpy.fs.s3.s3_reader import S3Reader
from kashpy.fs.s3.s3_writer import S3Writer

#

class S3(FS):
    def __init__(self, config_str):
        super().__init__("s3s", config_str, ["s3"], [])
    
    #

    def get_admin(self):
        reader = S3Admin(self)
        #
        return reader

    #

    def get_reader(self, file, **kwargs):
        reader = S3Reader(self, file, **kwargs)
        #
        return reader

    #

    def get_writer(self, file, **kwargs):
        writer = S3Writer(self, file, **kwargs)
        #
        return writer
