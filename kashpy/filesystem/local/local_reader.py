import os

from kashpy.filesystem.filesystem_reader import FileSystemReader

# Constants

ALL_MESSAGES = -1

#

class LocalReader(FileSystemReader):
    def __init__(self, local_obj, file, **kwargs):
        super().__init__(local_obj, file, **kwargs)
        #
        self.bufferedReader = open(os.path.join(local_obj.root_dir(), self.file_str), "rb")
        #
        self.file_size_int = os.stat(os.path.join(local_obj.root_dir(), self.file_str)).st_size

    def __del__(self):
        self.close()

    #

    def close(self):
        self.bufferedReader.close()
        #
        return self.file_str

    #

    def read_bytes(self, _, n_int):
        batch_bytes = self.bufferedReader.read(n_int)
        #
        return batch_bytes
