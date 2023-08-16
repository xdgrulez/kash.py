class FSAdmin:
    def __init__(self, fs_obj):
        self.fs_obj = fs_obj

    #

    def files(self, pattern=None, size=False, **kwargs):
        pattern_str_or_str_list = "*" if pattern is None else pattern
        pattern_str_list = [pattern_str_or_str_list] if isinstance(pattern_str_or_str_list, str) else pattern_str_or_str_list
        size_bool = size
        partitions_bool = "partitions" in kwargs and kwargs["partitions"] # included only to keep the APIs for Kafka/FS in sync
        filesize_bool = "filesize" in kwargs and kwargs["filesize"]
        #
        file_str_filesize_int_dict = self.list_files(pattern_str_list, filesize=True)
        #
        if size_bool:
            file_str_size_int_filesize_int_tuple_dict = {file_str: (self.fs_obj.stat(file_str), filesize_int) for file_str, filesize_int in file_str_filesize_int_dict.items()}
            if partitions_bool:
                if filesize_bool:
                    # e.g. {"file": {"size": 42, "partitions": {0: 42}, "filesize": 4711}}
                    file_str_size_int_partitions_dict_filesize_int_dict_dict = {file_str: {"size": size_int_filesize_int_tuple[0], "partitions": {0: size_int_filesize_int_tuple[0]}, "filesize": size_int_filesize_int_tuple[1]} for file_str, size_int_filesize_int_tuple in file_str_size_int_filesize_int_tuple_dict.items()}
                    return file_str_size_int_partitions_dict_filesize_int_dict_dict
                else:
                    # e.g. {"file": {"size": 42, "partitions": {0: 42}}}
                    file_str_size_int_partitions_dict_dict = {file_str: {"size": size_int_filesize_int_tuple[0], "partitions": {0: size_int_filesize_int_tuple[0]}} for file_str, size_int_filesize_int_tuple in file_str_size_int_filesize_int_tuple_dict.items()}
                    return file_str_size_int_partitions_dict_dict
            else:
                if filesize_bool:
                    # e.g. {"file": {"size": 42, "filesize": 4711}}
                    file_str_size_int_filesize_int_dict_dict = {file_str: {"size": size_int_filesize_int_tuple[0], "filesize": size_int_filesize_int_tuple[1]} for file_str, size_int_filesize_int_tuple in file_str_size_int_filesize_int_tuple_dict.items()}
                    return file_str_size_int_filesize_int_dict_dict
                else:
                    # e.g. {"file": 42}
                    file_str_size_int_dict = {file_str: size_int_filesize_int_tuple[0] for file_str, size_int_filesize_int_tuple in file_str_size_int_filesize_int_tuple_dict.items()}
                    return file_str_size_int_dict
        else:
            if partitions_bool:
                file_str_size_int_filesize_int_tuple_dict = {file_str: (self.fs_obj.stat(file_str), filesize_int) for file_str, filesize_int in file_str_filesize_int_dict.items()}
                if filesize_bool:
                    # e.g. {"file": {"partitions": {0: 42}, "filesize": 4711}}
                    file_str_partitions_dict_filesize_int_dict_dict = {file_str: {"partitions": {0: size_int_filesize_int_tuple[0]}, "filesize": size_int_filesize_int_tuple[1]} for file_str, size_int_filesize_int_tuple in file_str_size_int_filesize_int_tuple_dict.items()}
                    return file_str_partitions_dict_filesize_int_dict_dict
                else:
                    # e.g. {"file": {0: 42}}
                    file_str_partitions_dict_dict = {file_str: {0: size_int_filesize_int_tuple[0]} for file_str, size_int_filesize_int_tuple in file_str_size_int_filesize_int_tuple_dict.items()}
                    return file_str_partitions_dict_dict
            else:
                if filesize_bool:
                    # e.g. {"file": 4711}
                    file_str_filesize_int_dict = {file_str: filesize_int for file_str, filesize_int in file_str_filesize_int_dict.items()}
                    return file_str_filesize_int_dict
                else:
                    # e.g. ["file"]
                    file_str_list = [file_str for file_str in file_str_filesize_int_dict]
                    return file_str_list

    #

    def partitions(self, pattern=None, verbose=False):
        file_str_list = self.list_files(pattern)
        #
        file_str_num_partitions_int_dict = {file_str: 1 for file_str in file_str_list}
        #
        return file_str_num_partitions_int_dict
