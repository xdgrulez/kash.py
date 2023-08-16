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

    def list_files(self, pattern=None):
        self.admin(list_files) os.listdir(self.root_dir_str)
        pattern_str_or_str_list = pattern


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
