import json

from kashpy.helpers import get_millis

# Constants

CURRENT_TIME = 0

#

class FileSystemWriter():
    def __init__(self, filesystem_obj, file, **kwargs):
        self.filesystem_obj = filesystem_obj
        #
        self.file_str = file
        #
        (self.key_type_str, self.value_type_str) = filesystem_obj.get_key_value_type_tuple(**kwargs)

    #

    def write(self, value, **kwargs):
        key = kwargs["key"] if "key" in kwargs else None
        timestamp = kwargs["timestamp"] if "timestamp" in kwargs and kwargs["timestamp"] is not None else CURRENT_TIME
        headers = kwargs["headers"] if "headers" in kwargs else None
        #
        value_list = value if isinstance(value, list) else [value]
        #
        key_list = key if isinstance(key, list) else [key for _ in value_list]
        #
        timestamp_int_list = timestamp if isinstance(timestamp, list) else [timestamp for _ in value_list]
        headers_list = headers if isinstance(headers, list) and len(headers) == len(value_list) else [headers for _ in value_list]
        headers_str_bytes_tuple_list_list = [self.filesystem_obj.headers_to_headers_str_bytes_tuple_list(headers) for headers in headers_list]
        #
        def serialize(payload, key_bool):
            type_str = self.key_type_str if key_bool else self.value_type_str
            #
            if type_str.lower() == "json":
                if isinstance(payload, dict):
                    payload_str_or_bytes = json.dumps(payload)
                else:
                    payload_str_or_bytes = payload
            else:
                payload_str_or_bytes = payload
            return payload_str_or_bytes
        #        
        message_bytes = b""
        for value, key, timestamp_int, headers_str_bytes_tuple_list in zip(value_list, key_list, timestamp_int_list, headers_str_bytes_tuple_list_list):
            value_bytes = serialize(value, False)
            key_bytes = serialize(key, True)
            #
            if timestamp_int == CURRENT_TIME:
                timestamp_int = get_millis()
            #
            message_bytes += str({"headers": headers_str_bytes_tuple_list, "timestamp": timestamp_int, "key": key_bytes, "value": value_bytes}).encode("utf-8") + b"\n"
        #
        self.write_bytes(message_bytes)
