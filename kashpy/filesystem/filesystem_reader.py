import ast
import json

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
    
    def foldl(self, foldl_function, initial_acc, n=ALL_MESSAGES, **kwargs):
        n_int = n
        #
        read_batch_size_int = kwargs["read_batch_size"] if "read_batch_size" in kwargs else self.filesystem_obj.read_batch_size()
        #
        size_int = self.file_size_int
        if read_batch_size_int > size_int:
            read_batch_size_int = size_int
        #
        buffer_bytes = b""
        message_counter_int = 0
        break_bool = False
        acc = initial_acc
        file_offset_int = 0
        #
        def acc_bytes_to_acc(acc, bytes, break_bool, message_counter_int):
            serialized_message_dict = ast.literal_eval(bytes.decode("utf-8"))
            #
            deserialized_message_dict = self.deserialize(serialized_message_dict, self.key_type_str, self.value_type_str)
            #
            acc = foldl_function(acc, deserialized_message_dict)
            #
            message_counter_int += 1
            #
            return (acc, break_bool, message_counter_int)
        #
        
        while True:
            if file_offset_int > size_int:
                batch_bytes = b""
            else:
                batch_bytes = self.read_bytes(read_batch_size_int, file_offset_int=file_offset_int, **kwargs)
                file_offset_int += len(batch_bytes)
                
            if batch_bytes == b"":
                if buffer_bytes != b"":
                    (acc, break_bool, message_counter_int) = acc_bytes_to_acc(acc, buffer_bytes, break_bool, message_counter_int)
                break
            #
            buffer_bytes += batch_bytes
            message_bytes_list = buffer_bytes.split(b"\n")
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

    def deserialize(self, message_dict, key_type, value_type):
        key_type_str = key_type
        value_type_str = value_type
        #

        def to_str(x):
            if isinstance(x, bytes):
                return x.decode("utf-8")
            elif isinstance(x, dict):
                return str(x)
            else:
                return x
        #

        def to_bytes(x):
            if isinstance(x, str):
                return x.encode("utf-8")
            elif isinstance(x, dict):
                return str(x).encode("utf-8")
            else:
                return x
        #

        def to_dict(x):
            if isinstance(x, bytes) or isinstance(x, str):
                return json.loads(x)
            else:
                return x
        #

        if key_type_str.lower() == "str":
            decode_key = to_str
        elif key_type_str.lower() == "bytes":
            decode_key = to_bytes
        elif key_type_str.lower() == "json":
            decode_key = to_dict
        #
        if value_type_str.lower() == "str":
            decode_value = to_str
        elif value_type_str.lower() == "bytes":
            decode_value = to_bytes
        elif value_type_str.lower() == "json":
            decode_value = to_dict
        #
        return_message_dict = {"headers": message_dict["headers"], "timestamp": message_dict["timestamp"], "key": decode_key(message_dict["key"]), "value": decode_value(message_dict["value"])}
        return return_message_dict

    #

    def read(self, n=ALL_MESSAGES):
        def foldl_function(message_dict_list, message_dict):
            message_dict_list.append(message_dict)
            #
            return message_dict_list
        #
        return self.foldl(foldl_function, [], n)
