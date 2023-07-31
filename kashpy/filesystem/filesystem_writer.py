class FileSystemWriter():
    def write(self, value, key=None, **kwargs):
        keys = key if isinstance(key, list) else [key]
        values = value if isinstance(value, list) else [value]
        if keys == [None]:
            keys = [None for _ in values]
        #
        message_bytes = b""
        for key, value in zip(keys, values):
            key_bytes = payload_to_bytes(key)
            value_bytes = payload_to_bytes(value)
            #
            if key_bytes == b"":
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

def payload_to_bytes(key_or_value):
    if isinstance(key_or_value, bytes):
        return_bytes = key_or_value
    elif isinstance(key_or_value, str):
        return_bytes = str_to_bytes(key_or_value)
    elif isinstance(key_or_value, dict):
        return_bytes = dict_to_bytes(key_or_value)
    #
    return return_bytes
