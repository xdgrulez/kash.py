class LocalWriter:
    def __init__(self, kash_config_dict, verbose_int, file, key_type="str", value_type="str", key_value_separator=None, message_separator=b"\n", overwrite=True):
        self.kash_config_dict = kash_config_dict
        self.verbose_int = verbose_int
        #
        self.file_str = file
        self.key_type_str = key_type
        self.value_type_str = value_type
        self.key_value_separator_bytes = key_value_separator
        self.message_separator_bytes = message_separator
        self.overwrite_bool = overwrite
        #
        mode_str = "wb" if self.overwrite_bool else "ab"
        self.bufferedWriter = open(file, mode_str)

    def __del__(self):
        self.close()

    #

    def close(self):
        self.bufferedWriter.close()

    #

    def write(self, value, key=None):
        key_bytes = key_or_value_to_bytes(key)
        value_bytes = key_or_value_to_bytes(value)
        #
        if key_bytes is None:
            message_bytes = value_bytes + self.message_separator_bytes
        else:
            message_bytes = key_bytes + self.key_value_separator_bytes + value_bytes + self.message_separator_bytes
        #
        self.bufferedWriter.write(message_bytes)

#

def str_to_bytes(str):
    if str is None:
        bytes = None
    else:
        bytes = str.encode("utf-8")
    #
    return bytes


def dict_to_bytes(dict):
    if dict is None:
        bytes = None
    else:
        bytes = str(dict).encode("utf-8")
    #
    return bytes

def key_or_value_to_bytes(key_or_value, type_str):
    if type_str == "bytes":
        bytes = key_or_value
    elif type_str == "str":
        bytes = str_to_bytes(key_or_value)
    elif type_str == "json" or type_str == "dict":
        bytes = dict_to_bytes(key_or_value)
    #
    return bytes
