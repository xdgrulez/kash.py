import os

from kashpy.filesystem.filesystem_reader import FileSystemReader
from kashpy.helpers import find_file_offset_of_line 

# Constants

ALL_MESSAGES = -1

#

class LocalReader(FileSystemReader):
    def __init__(self, local_obj, file, **kwargs):
        super().__init__(local_obj, file, **kwargs)
        #
        self.path_file_str = os.path.join(local_obj.root_dir(), self.file_str)
        #
        self.bufferedReader = open(self.path_file_str, "rb")
        #
        self.file_size_int = os.stat(self.path_file_str).st_size

    def __del__(self):
        self.close()

    #

    def close(self):
        self.bufferedReader.close()
        #
        return self.file_str

    #

    def read_bytes(self, n_int, file_offset_int=0, **kwargs):
        if "offset" in kwargs and kwargs["offset"] > 0 and file_offset_int == 0:
            found_file_offset_int = find_file_offset_of_line(self.path_file_str, kwargs["offset"])
            self.bufferedReader.seek(found_file_offset_int)
        elif file_offset_int > 0:
            self.bufferedReader.seek(file_offset_int)
        #
        batch_bytes = self.bufferedReader.read(n_int)
        #
        return batch_bytes
