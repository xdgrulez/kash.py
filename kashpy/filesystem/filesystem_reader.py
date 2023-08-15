import ast

from kashpy.helpers import bytes_to_dict, bytes_to_str

# Constants

ALL_MESSAGES = -1

#

class FileSystemReader():
    def __init__(self, filesystem_obj, file, **kwargs):
        self.filesystem_obj = filesystem_obj
        #
        self.file_str = file
        #
        (self.key_type_str, self.value_type_str) = filesystem_obj.get_key_value_type_tuple(**kwargs)
        #
        (self.payload_separator_bytes, self.message_separator_bytes, self.null_indicator_bytes) = filesystem_obj.get_payload_separator_message_separator_null_indicator_tuple(**kwargs)

    #
    
    def foldl(self, foldl_function, initial_acc, n=ALL_MESSAGES, **kwargs):
        n_int = n
        #
        read_batch_size_int = kwargs["read_batch_size"] if "read_batch_size" in kwargs else self.filesystem_obj.read_batch_size()
        if read_batch_size_int > n_int:
            read_batch_size_int = n_int
        #
        break_function = kwargs["break_function"] if "break_function" in kwargs else lambda _, _1: False
        #
        buffer_bytes = b""
        message_counter_int = 0
        break_bool = False
        acc = initial_acc
        offset_int = 0
        #
        size_int = self.file_size_int
        #
        def split_payload(message_bytes, payload_separator_bytes):
            value_bytes = b""
            key_bytes = None
            headers_bytes = None
            #
            if message_bytes:
                if payload_separator_bytes is not None:
                    split_bytes_list = message_bytes.split(payload_separator_bytes)
                    if len(split_bytes_list) == 3:
                        headers_bytes = split_bytes_list[0]
                        key_bytes = split_bytes_list[1]
                        value_bytes = split_bytes_list[2]
                    elif len(split_bytes_list) == 2:
                        key_bytes = split_bytes_list[0]
                        value_bytes = split_bytes_list[1]
                    else:
                        value_bytes = split_bytes_list[0]
                else:
                    value_bytes = message_bytes
            #
            return headers_bytes, key_bytes, value_bytes
        #

        def bytes_to_headers(headers_bytes):
            if headers_bytes is not None and len(headers_bytes) > len(b'[("", b"")]') and headers_bytes[0:2] == b"[(" and headers_bytes[-2:] == b")]":
                headers = ast.literal_eval(str(headers_bytes))
            else:
                headers = None
            #
            return headers
        #

        def bytes_to_key_or_value(key_or_value_bytes, type_str):
            if key_or_value_bytes == self.null_indicator_bytes:
                key_or_value = None
            else:
                if type_str == "bytes":
                    key_or_value = key_or_value_bytes
                elif type_str == "str":
                    key_or_value = bytes_to_str(key_or_value_bytes)
                elif type_str == "json" or type_str == "dict":
                    key_or_value = bytes_to_dict(key_or_value_bytes)
            #
            return key_or_value


        while True:
            def acc_bytes_to_acc(acc, bytes, break_bool, message_counter_int):
                (headers_bytes, key_bytes, value_bytes) = split_payload(bytes, self.payload_separator_bytes)
                #
                headers = bytes_to_headers(headers_bytes)
                key = bytes_to_key_or_value(key_bytes, self.key_type_str)
                value = bytes_to_key_or_value(value_bytes, self.value_type_str)
                #
                message_dict = {"headers": headers, "partition": 0, "offset": message_counter_int, "timestamp": None, "key": key, "value": value}
                #
                if break_function(acc, message_dict):
                    break_bool = True
                    return (acc, break_bool)
                #
                acc = foldl_function(acc, message_dict)
                #
                message_counter_int += 1
                #
                return (acc, break_bool, message_counter_int)
            #

            if offset_int > size_int:
                batch_bytes = b""
            else:
                batch_bytes = self.read_bytes(offset_int, read_batch_size_int)
                offset_int += read_batch_size_int
            #
            if batch_bytes == b"":
                if buffer_bytes != b"":
                    (acc, break_bool, message_counter_int) = acc_bytes_to_acc(acc, buffer_bytes, break_bool, message_counter_int)
                break
            #
            buffer_bytes += batch_bytes
            message_bytes_list = buffer_bytes.split(self.message_separator_bytes)
            for message_bytes in message_bytes_list[:-1]:
                (acc, break_bool, message_counter_int) = acc_bytes_to_acc(acc, message_bytes, break_bool, message_counter_int)
                if break_bool:
                    break
                #
                if n_int != ALL_MESSAGES:
                    if message_counter_int >= n_int:
                        break_bool = True
                        break
            #
            if break_bool:
                break
            #
            buffer_bytes = message_bytes_list[-1]
        #
        return acc

    #

    def read(self, n=ALL_MESSAGES):
        def foldl_function(message_dict_list, message_dict):
            message_dict_list.append(message_dict)
            #
            return message_dict_list
        #
        return self.foldl(foldl_function, [], n)

