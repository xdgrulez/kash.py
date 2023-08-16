import os

from kashpy.fs.fs_reader import FSReader

# Constants

ALL_MESSAGES = -1

#

class LocalReader(FSReader):
    def __init__(self, local_obj, file, **kwargs):
        super().__init__(local_obj, file, **kwargs)
        #
        self.path_file_str = os.path.join(local_obj.root_dir(), self.file_str)
        #
        self.bufferedReader = open(self.path_file_str, "rb")
        #
        self.file_size_int = local_obj.ls(self.file_str, filesize=True)[self.file_str]
        #
        self.file_offset_int = self.find_file_offset_by_offset(**kwargs)

    def __del__(self):
        self.close()

    #

    def close(self):
        self.bufferedReader.close()
        #
        return self.file_str

    #

    def read_bytes(self, n_int, file_offset_int=0, **kwargs):
        if file_offset_int > 0:
            self.bufferedReader.seek(file_offset_int)
        #
        batch_bytes = self.bufferedReader.read(n_int)
        #
        return batch_bytes
