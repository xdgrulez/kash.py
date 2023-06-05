from fnmatch import fnmatch
import os

#

class LocalAdmin:
    def __init__(self, kash_config_dict):
        self.kash_config_dict = kash_config_dict

    #

    def list(self, pattern=None, size=False):
        pattern_str_or_str_list = "*" if pattern is None else pattern
        pattern_str_list = [pattern_str_or_str_list] if isinstance(pattern_str_or_str_list, str) else pattern_str_or_str_list
        #
        if size:
            file_str_list = os.listdir(".")
            #
            file_str_size_int_tuple_list = [(file_str, os.stat(file_str).st_size) for file_str in file_str_list if any(fnmatch(file_str, pattern_str) for pattern_str in pattern_str_list)]
            #
            file_str_size_int_tuple_list.sort()
            #
            return file_str_size_int_tuple_list
        else:
            file_str_list = os.listdir(".")
            #
            file_str_list = [file_str for file_str in file_str_list if any(fnmatch(file_str, pattern_str) for pattern_str in pattern_str_list)]
            #
            file_str_list.sort()
            #
            return file_str_list

    #

    def delete(self, pattern=None):
        pattern_str_or_str_list = [] if pattern is None else pattern
        pattern_str_list = [pattern_str_or_str_list] if isinstance(pattern_str_or_str_list, str) else pattern_str_or_str_list
        #
        file_str_list = os.listdir(".")
        #
        file_str_list = [file_str for file_str in file_str_list if any(fnmatch(file_str, pattern_str) for pattern_str in pattern_str_list)]
        for file_str in file_str_list:
            os.remove(file_str)
        #
        return file_str_list
