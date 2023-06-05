from fnmatch import fnmatch

from minio import Minio

#

class S3Admin:
    def __init__(self, kash_config_dict):
        self.kash_config_dict = kash_config_dict
        #
        self.minio = Minio("localhost:9000", access_key="admin", secret_key="password", secure=False)

    #

    def list(self, pattern=None, size=False):
        pattern_str_or_str_list = "*" if pattern is None else pattern
        pattern_str_list = [pattern_str_or_str_list] if isinstance(pattern_str_or_str_list, str) else pattern_str_or_str_list
        #
        if size:
            object_generator = self.minio.list_objects("minio-test-bucket")
            #
            object_str_size_int_tuple_list = [(object.object_name, object.size) for object in object_generator if any(fnmatch(object.object_name, pattern_str) for pattern_str in pattern_str_list)]
            #
            object_str_size_int_tuple_list.sort()
            #
            return object_str_size_int_tuple_list
        else:
            object_generator = self.minio.list_objects("minio-test-bucket")
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
        object_generator = self.minio.list_objects("minio-test-bucket")
        object_str_list = [object.object_name for object in object_generator if any(fnmatch(object.object_name, pattern_str) for pattern_str in pattern_str_list)]
        for object_str in object_str_list:
            self.minio.remove_object("minio-test-bucket", object_str)
        #
        return object_str_list
