class FileSystemWriter():
    def write(self, value, key=None, **kwargs):
        keys = key if isinstance(key, list) else [key]
        values = value if isinstance(value, list) else [value]
        if keys == [None]:
            keys = [None for _ in values]
        #
        message_bytes = b""
        for key, value in zip(keys, values):
            key_bytes = payload_to_bytes(key, self.key_type_str)
            value_bytes = payload_to_bytes(value, self.value_type_str)
            #
            if key_bytes is None:
                message_bytes += value_bytes + self.message_separator_bytes
            else:
                message_bytes += key_bytes + self.key_value_separator_bytes + value_bytes + self.message_separator_bytes
        #
        self.write_bytes(message_bytes)

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

def payload_to_bytes(key_or_value, type_str):
    if type_str == "bytes":
        bytes = key_or_value
    elif type_str == "str":
        bytes = str_to_bytes(key_or_value)
    elif type_str == "json" or type_str == "dict":
        bytes = dict_to_bytes(key_or_value)
    #
    return bytes
