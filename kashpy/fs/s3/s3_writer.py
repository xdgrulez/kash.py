import os
import tempfile

from minio import Minio

from kashpy.fs.fs_writer import FSWriter

class S3Writer(FSWriter):
    def __init__(self, s3_obj, file, **kwargs):
        super().__init__(s3_obj, file, **kwargs)
        #
        self.bucket_name_str = s3_obj.s3_config_dict["bucket.name"]
        #
        temp_path_str = f"/{tempfile.gettempdir()}/kash.py/s3"
        os.makedirs(temp_path_str, exist_ok=True)
        self.temp_file_str = f"{temp_path_str}/{self.file_str}"
        self.bufferedWriter = open(self.temp_file_str, "wb")
        #
        self.minio = Minio(s3_obj.s3_config_dict["endpoint"], access_key=s3_obj.s3_config_dict["access.key"], secret_key=s3_obj.s3_config_dict["secret.key"], secure=False)

    def __del__(self):
        self.close()

    #

    def close(self):
        self.bufferedWriter.close()
        self.flush()

    #

    def flush(self):
        self.minio.fput_object(self.bucket_name_str, self.file_str, self.temp_file_str)
        #
        return self.file_str

    #

    def write_bytes(self, bytes, **kwargs):
        self.bufferedWriter.write(bytes)
