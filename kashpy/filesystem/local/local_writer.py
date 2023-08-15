import os

from kashpy.filesystem.filesystem_writer import FileSystemWriter


class LocalWriter(FileSystemWriter):
    def __init__(self, local_obj, file, **kwargs):
        super().__init__(local_obj, file, **kwargs)
        #
        self.path_file_str = os.path.join(local_obj.root_dir(), self.file_str)
        #
        self.overwrite_bool = kwargs["overwrite"] if "overwrite" in kwargs else False
        #
        mode_str = "wb" if self.overwrite_bool else "ab"
        self.bufferedWriter = open(self.path_file_str, mode_str)

    def __del__(self):
        self.close()

    #

    def close(self):
        self.bufferedWriter.close()

    #

    def write_bytes(self, bytes, **kwargs):
        self.bufferedWriter.write(bytes)
