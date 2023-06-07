from minio import Minio

from kashpy.helpers import split_key_value, bytes_to_payload

# Constants

ALL_MESSAGES = -1

#

class S3Reader:
    def __init__(self, s3_config_dict, kash_config_dict, file, **kwargs):
        self.s3_config_dict = s3_config_dict
        self.kash_config_dict = kash_config_dict
        #
        self.file_str = file
        #
        self.key_type_str = kwargs["key_type"] if "key_type" in kwargs else "str"
        self.value_type_str = kwargs["value_type"] if "value_type" in kwargs else "str"
        self.key_value_separator_bytes = kwargs["key_value_separator"] if "key_value_separator" in kwargs else None
        self.message_separator_bytes = kwargs["message_separator"] if "message_separator" in kwargs else b"\n"
        #
        self.minio = Minio(self.s3_config_dict["endpoint"], access_key=self.s3_config_dict["access.key"], secret_key=self.s3_config_dict["secret.key"], secure=False)

    #

    def close(self):
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

    def foldl(self, foldl_function, initial_acc, n=ALL_MESSAGES, **kwargs):
        n_int = n
        #
        break_function = kwargs["break_function"] if "break_function" in kwargs else lambda _, _1: False
        #
        buffer_bytes = b""
        message_counter_int = 0
        break_bool = False
        buffer_size_int = self.kash_config_dict["read.buffer.size"]
        progress_num_messages_int = self.kash_config_dict["progress.num.messages"]
        verbose_int = self.kash_config_dict["verbose"]
        acc = initial_acc
        offset_int = 0
        #
        object = self.minio.stat_object(self.s3_config_dict["bucket.name"], self.file_str)
        size_int = object.size
        while True:
            def acc_bytes_to_acc(acc, bytes, break_bool, message_counter_int):
                key_bytes_value_bytes_tuple = split_key_value(bytes, self.key_value_separator_bytes)
                #
                key = bytes_to_payload(key_bytes_value_bytes_tuple[0], self.key_type_str)
                value = bytes_to_payload(key_bytes_value_bytes_tuple[1], self.value_type_str)
                #
                message_dict = {"headers": None, "partition": 0, "offset": message_counter_int, "timestamp": None, "key": key, "value": value}
                #
                if break_function(acc, message_dict):
                    break_bool = True
                    return (acc, break_bool)
                #
                acc = foldl_function(acc, message_dict)
                #
                message_counter_int += 1
                if verbose_int > 0 and message_counter_int % progress_num_messages_int == 0:
                    print(f"Read: {message_counter_int}")
                #
                return (acc, break_bool, message_counter_int)
            #

            if offset_int > size_int:
                batch_bytes = b""
            else:
                response = self.minio.get_object(self.s3_config_dict["bucket.name"], self.file_str, offset=offset_int, length=buffer_size_int)
                batch_bytes = response.data
                offset_int += buffer_size_int
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
        return (acc, message_counter_int)
