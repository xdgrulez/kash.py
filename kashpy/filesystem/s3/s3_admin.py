from fnmatch import fnmatch

from minio import Minio

#

class S3Admin:
    def __init__(self, s3_obj):
        self.s3_obj = s3_obj
        #
        self.bucket_name_str = s3_obj.s3_config_dict["bucket.name"]
        #
        self.minio = Minio(s3_obj.s3_config_dict["endpoint"], access_key=s3_obj.s3_config_dict["access.key"], secret_key=s3_obj.s3_config_dict["secret.key"], secure=False)

    #

    def files(self, pattern=None, size=False, **kwargs):
        pattern_str_or_str_list = "*" if pattern is None else pattern
        pattern_str_list = [pattern_str_or_str_list] if isinstance(pattern_str_or_str_list, str) else pattern_str_or_str_list
        size_bool = size
        filesize_bool = "filesize" in kwargs and kwargs["filesize"]
        #
        object_generator = self.minio.list_objects(self.bucket_name_str)
        file_str_file_size_int_tuple_list = [(object.object_name, object.size) for object in object_generator if any(fnmatch(object.object_name, pattern_str) for pattern_str in pattern_str_list)]
        #
        if size_bool:
            if filesize_bool:
                file_str_size_int_filesize_int_tuple_dict = {file_str: (self.s3_obj.cat(file_str)[1], file_size_int) for file_str, file_size_int in file_str_file_size_int_tuple_list}
                return file_str_size_int_filesize_int_tuple_dict
            else:
                file_str_size_int_dict = {file_str: self.s3_obj.cat(file_str)[1] for file_str, _ in file_str_file_size_int_tuple_list}
                return file_str_size_int_dict
        else:
            if filesize_bool:
                file_str_filesize_int_dict = {file_str: file_size_int for file_str, file_size_int in file_str_file_size_int_tuple_list}
                return file_str_filesize_int_dict
            else:
                file_str_list = [file_str for file_str, _ in file_str_file_size_int_tuple_list]
                file_str_list.sort()
                return file_str_list

    #

    def delete(self, pattern=None):
        pattern_str_or_str_list = [] if pattern is None else pattern
        pattern_str_list = [pattern_str_or_str_list] if isinstance(pattern_str_or_str_list, str) else pattern_str_or_str_list
        #
        object_generator = self.minio.list_objects(self.bucket_name_str)
        file_str_list = [object.object_name for object in object_generator if any(fnmatch(object.object_name, pattern_str) for pattern_str in pattern_str_list)]
        #
        filtered_file_str_list = [file_str for file_str in file_str_list if any(fnmatch(file_str, pattern_str) for pattern_str in pattern_str_list)]
        for file_str in filtered_file_str_list:
            self.minio.remove_object(self.bucket_name_str, file_str)
        #
        return filtered_file_str_list
