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
        #
        if size_bool:
            object_generator = self.minio.list_objects(self.bucket_name_str)
            #
            object_str_size_int_tuple_list = [(object.object_name, object.size) for object in object_generator if any(fnmatch(object.object_name, pattern_str) for pattern_str in pattern_str_list)]
            #
            object_str_size_int_tuple_list.sort()
            #
            return object_str_size_int_tuple_list
        else:
            object_generator = self.minio.list_objects(self.bucket_name_str)
            object_str_list = [object.object_name for object in object_generator if any(fnmatch(object.object_name, pattern_str) for pattern_str in pattern_str_list)]
            #
            object_str_list.sort()
            #
            return object_str_list

    #

    def delete(self, pattern=None):
        pattern_str_or_str_list = [] if pattern is None else pattern
        pattern_str_list = [pattern_str_or_str_list] if isinstance(pattern_str_or_str_list, str) else pattern_str_or_str_list
        #
        object_generator = self.minio.list_objects(self.bucket_name_str)
        object_str_list = [object.object_name for object in object_generator if any(fnmatch(object.object_name, pattern_str) for pattern_str in pattern_str_list)]
        for object_str in object_str_list:
            self.minio.remove_object(self.bucket_name_str, object_str)
        #
        return object_str_list
