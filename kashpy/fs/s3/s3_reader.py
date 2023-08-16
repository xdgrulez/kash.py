from kashpy.fs.fs_reader import FSReader

from minio import Minio

#

class S3Reader(FSReader):
    def __init__(self, s3_obj, file, **kwargs):
        super().__init__(s3_obj, file, **kwargs)
        #
        self.bucket_name_str = s3_obj.s3_config_dict["bucket.name"]
        #
        self.minio = Minio(s3_obj.s3_config_dict["endpoint"], access_key=s3_obj.s3_config_dict["access.key"], secret_key=s3_obj.s3_config_dict["secret.key"], secure=False)
        #
        object = self.minio.stat_object(self.bucket_name_str, self.file_str)
        self.file_size_int = object.size
        #
        self.file_offset_int = self.find_file_offset_by_offset(**kwargs)

    #

    def close(self):
        return self.file_str

    #

    def read_bytes(self, **kwargs):
        offset_int = kwargs["offset"] if "offset" in kwargs else 0
        n_int = kwargs["n"] if "n" in kwargs else 0
        #
        response = self.minio.get_object(self.bucket_name_str, self.file_str, offset=offset_int, length=n_int)
        batch_bytes = response.data
        #
        return batch_bytes
