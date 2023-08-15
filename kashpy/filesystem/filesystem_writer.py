from kashpy.helpers import bytes_or_str_to_bytes, dict_to_bytes, str_to_bytes


class FileSystemWriter():
    def __init__(self, filesystem_obj, file, **kwargs):
        self.filesystem_obj = filesystem_obj
        #
        self.file_str = file
        #
        (self.key_type_str, self.value_type_str) = filesystem_obj.get_key_value_type_tuple(**kwargs)
        #
        (self.payload_separator_bytes, self.message_separator_bytes, self.null_indicator_bytes) = filesystem_obj.get_payload_separator_message_separator_null_indicator_tuple(**kwargs)

    #

    def write(self, value, key=None, headers=None, **kwargs):
        value_list = value if isinstance(value, list) else [value]
        key_list = key if isinstance(key, list) and len(key) == len(value_list) else [key for _ in value_list]
        #
        headers_list = headers if isinstance(headers, list) and len(headers) == len(value_list) else [headers for _ in value_list]
        headers_str_bytes_tuple_list_list = [self.filesystem_obj.headers_to_headers_str_bytes_tuple_list(headers) for headers in headers_list]

        #
        def key_value_payload_to_bytes(payload):
            if isinstance(payload, bytes):
                return_bytes = payload
            elif isinstance(payload, str):
                return_bytes = str_to_bytes(payload)
            elif isinstance(payload, dict):
                return_bytes = dict_to_bytes(payload)
            else:
                return_bytes = b""
            #
            return return_bytes
        #
        
        def headers_to_bytes(headers):
            if headers is not None:
                return_bytes = str(headers_str_bytes_tuple_list).encode("utf-8")
            else:
                return_bytes = b""
            #
            return return_bytes
        #

        message_bytes = b""
        for value, key, headers_str_bytes_tuple_list in zip(value_list, key_list, headers_str_bytes_tuple_list_list):
            value_bytes = key_value_payload_to_bytes(value)
            key_bytes = key_value_payload_to_bytes(key)
            headers_bytes = headers_to_bytes(headers_str_bytes_tuple_list)
            #
            message_bytes += headers_bytes + self.payload_separator_bytes + key_bytes + self.payload_separator_bytes + value_bytes + self.message_separator_bytes
        #
        self.write_bytes(message_bytes)
