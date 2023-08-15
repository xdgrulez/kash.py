from fnmatch import fnmatch
from pathlib import Path    
import os

#

class LocalAdmin:
    def __init__(self, local_obj):
        self.local_obj = local_obj
        #
        self.root_dir_str = local_obj.root_dir()

    #

    def files(self, pattern=None, size=False, **kwargs):
        pattern_str_or_str_list = "*" if pattern is None else pattern
        pattern_str_list = [pattern_str_or_str_list] if isinstance(pattern_str_or_str_list, str) else pattern_str_or_str_list
        size_bool = size
        filesize_bool = "filesize" in kwargs and kwargs["filesize"]
        #
        file_str_list = os.listdir(self.root_dir_str)
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

    #

    def delete(self, pattern=None):
        pattern_str_or_str_list = [] if pattern is None else pattern
        pattern_str_list = [pattern_str_or_str_list] if isinstance(pattern_str_or_str_list, str) else pattern_str_or_str_list
        #
        file_str_list = os.listdir(self.root_dir_str)
        #
        filtered_file_str_list = [file_str for file_str in file_str_list if any(fnmatch(file_str, pattern_str) for pattern_str in pattern_str_list)]
        for file_str in filtered_file_str_list:
            path_file_str = os.path.join(self.root_dir_str, file_str)
            os.remove(path_file_str)
        #
        return filtered_file_str_list
