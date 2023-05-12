from json import loads

# Constants

ALL_MESSAGES = -1

#

class LocalReader:
    def __init__(self, kash_config_dict, verbose_int, file, key_type="str", value_type="str", key_value_separator=None, message_separator=b"\n"):
        self.kash_config_dict = kash_config_dict
        self.verbose_int = verbose_int
        #
        self.file_str = file
        #
        self.key_type_str = key_type
        self.value_type_str = value_type
        self.key_value_separator_bytes = key_value_separator
        self.message_separator_bytes = message_separator
        #
        self.bufferedReader = open(file, "rb")

    def __del__(self):
        self.close()

    #

    def close(self):
        self.bufferedReader.close()
        #
        return self.file_str

    #

    def read(self, n=ALL_MESSAGES):
        def foldl_function(message_dict_list, message_dict):
            message_dict_list.append(message_dict)
            #
            return message_dict_list
        #
        return self.foldl(foldl_function, [], n)

    #

    def foldl(self, foldl_function, initial_acc, n=ALL_MESSAGES, break_function=lambda _, _1: False):
        n_int = n
        #
        buf_bytes = b""
        message_counter_int = 0
        break_bool = False
        buffer_size_int = self.kash_config_dict["read.buffer.size"]
        progress_num_messages_int = self.kash_config_dict["progress.num.messages"]
        acc = initial_acc
        #
        while True:
            newbuf_bytes = self.bufferedReader.read(buffer_size_int)
            if not newbuf_bytes:
                if buf_bytes:
                    last_message_bytes = buf_bytes
                    #
                    key_bytes_value_bytes_tuple = split_key_value(last_message_bytes, self.key_value_separator_bytes)
                    #
                    key = bytes_to_key_or_value(key_bytes_value_bytes_tuple[0], self.key_type_str)
                    value = bytes_to_key_or_value(key_bytes_value_bytes_tuple[1], self.value_type_str)
                    #
                    message_dict = {"headers": None, "partition": 0, "offset": message_counter_int, "timestamp": None, "key": key, "value": value}
                    #
                    if break_function(acc, message_dict):
                        break_bool = True
                        break
                    #
                    acc = foldl_function(acc, message_dict)
                    #
                    message_counter_int += 1
                    if self.verbose_int > 0 and message_counter_int % progress_num_messages_int == 0:
                        print(f"Read: {message_counter_int}")
                break
            buf_bytes += newbuf_bytes
            message_bytes_list = buf_bytes.split(self.message_separator_bytes)
            for message_bytes in message_bytes_list[:-1]:
                key_bytes_value_bytes_tuple = split_key_value(message_bytes, self.key_value_separator_bytes)
                #
                key = bytes_to_key_or_value(key_bytes_value_bytes_tuple[0], self.key_type_str)
                value = bytes_to_key_or_value(key_bytes_value_bytes_tuple[1], self.value_type_str)
                message_dict = {"headers": None, "partition": 0, "offset": message_counter_int, "timestamp": None, "key": key, "value": value}
                #
                if break_function(acc, message_dict):
                    break_bool = True
                    break
                #
                acc = foldl_function(acc, message_dict)
                #
                message_counter_int += 1
                if self.verbose_int > 0 and message_counter_int % progress_num_messages_int == 0:
                    print(f"Read: {message_counter_int}")
                #
                if n_int != ALL_MESSAGES:
                    if message_counter_int >= n_int:
                        break_bool = True
                        break
            #
            if break_bool:
                break
            #
            buf_bytes = message_bytes_list[-1]
        #
        return acc

#

def split_key_value(message_bytes, key_value_separator_bytes):
    key_bytes = None
    value_bytes = None
    #
    if message_bytes:
        if key_value_separator_bytes is not None:
            split_bytes_list = message_bytes.split(key_value_separator_bytes)
            if len(split_bytes_list) == 2:
                key_bytes = split_bytes_list[0]
                value_bytes = split_bytes_list[1]
            else:
                value_bytes = message_bytes
        else:
            value_bytes = message_bytes
    #
    return key_bytes, value_bytes 


def bytes_to_str(bytes):
    if bytes is None:
        str = None
    else:
        str = bytes.decode("utf-8")
    #
    return str

def bytes_to_dict(bytes):
    if bytes is None:
        dict = None
    else:
        dict = loads(bytes.decode("utf-8"))
    #
    return dict

def bytes_to_key_or_value(key_or_value_bytes, type_str):
    if type_str == "bytes":
        key_or_value = key_or_value_bytes
    elif type_str == "str":
        key_or_value = bytes_to_str(key_or_value_bytes)
    elif type_str == "json" or type_str == "dict":
        key_or_value = bytes_to_dict(key_or_value_bytes)
    #
    return key_or_value
