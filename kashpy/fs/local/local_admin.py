from fnmatch import fnmatch
import os

from kashpy.fs.fs_admin import FSAdmin

#

class LocalAdmin(FSAdmin):
    def __init__(self, local_obj):
        super().__init__(local_obj)

    #

    def list_files(self, pattern=None, filesize=False):
        pattern_str_or_str_list = pattern
        #
        dir_file_str_list = os.listdir(self.fs_obj.root_dir())
        file_str_list = [dir_file_str for dir_file_str in dir_file_str_list if os.path.isfile(os.path.join(self.fs_obj.root_dir(), dir_file_str))]
        #
        if pattern_str_or_str_list is not None:
            if isinstance(pattern_str_or_str_list, str):
                pattern_str_or_str_list = [pattern_str_or_str_list]
            #
            return_file_str_list = [file_str for file_str in file_str_list if any(fnmatch(file_str, pattern_str) for pattern_str in pattern_str_or_str_list)]
            return_file_str_list.sort()
            #
            if filesize:
                return_file_str_filesize_int_dict = {file_str: os.stat(os.path.join(self.fs_obj.root_dir(), file_str)).st_size for file_str in return_file_str_list}
                return return_file_str_filesize_int_dict
            else:
                return_file_str_list = file_str_list
        #
        return return_file_str_list

    #

    def delete(self, pattern=None):
        pattern_str_or_str_list = [] if pattern is None else pattern
        pattern_str_list = [pattern_str_or_str_list] if isinstance(pattern_str_or_str_list, str) else pattern_str_or_str_list
        #
        file_str_list = os.listdir(self.fs_obj.root_dir())
        #
        filtered_file_str_list = [file_str for file_str in file_str_list if any(fnmatch(file_str, pattern_str) for pattern_str in pattern_str_list)]
        for file_str in filtered_file_str_list:
            path_file_str = os.path.join(self.fs_obj.root_dir(), file_str)
            os.remove(path_file_str)
        #
        return filtered_file_str_list
