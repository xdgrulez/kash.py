import re

from kashpy.functional import Functional

# Constants

ALL_MESSAGES = -1

#

class Shell(Functional):
    def cat(self, resource, n=ALL_MESSAGES, **kwargs):
        def map_function(message_dict):
            return message_dict
        #
        return self.map(resource, map_function, n, **kwargs)

    def head(self, resource, n=10, **kwargs):
        return self.cat(resource, n, **kwargs)

    def tail(self, resource, n=10, **kwargs):
        n_int = n
        #
        def map_function(message_dict):
            return message_dict
        #
        (offsets, offsets_arg_str) = self.get_offsets(resource, n_int)
        kwargs[offsets_arg_str] = offsets
        #
        return self.map(resource, map_function, n, **kwargs)

    #

    def cp(self, source_resource, target_storage, target_resource, map_function=lambda x: x, n=ALL_MESSAGES, **kwargs):
        return self.map_to(source_resource, target_storage, target_resource, map_function, n, **kwargs)

    #

    def wc(self, topic_str, **kwargs):
        def foldl_function(acc, message_dict):
            if message_dict["key"] is None:
                key_str = ""
            else:
                key_str = str(message_dict["key"])
            num_words_key_int = 0 if key_str == "" else len(key_str.split(" "))
            num_bytes_key_int = len(key_str)
            #
            if message_dict["value"] is None:
                value_str = ""
            else:
                value_str = str(message_dict["value"])
            num_words_value_int = len(value_str.split(" "))
            num_bytes_value_int = len(value_str)
            #
            acc_num_words_int = acc[0] + num_words_key_int + num_words_value_int
            acc_num_bytes_int = acc[1] + num_bytes_key_int + num_bytes_value_int
            return (acc_num_words_int, acc_num_bytes_int)
        #
        ((acc_num_words_int, acc_num_bytes_int), num_messages_int) = self.foldl(topic_str, foldl_function, (0, 0), **kwargs)
        return (num_messages_int, acc_num_words_int, acc_num_bytes_int)

    #

    def diff_fun(self, resource1, storage2, resource2, diff_function, n=ALL_MESSAGES, **kwargs):
        def zip_foldl_function(acc, message_dict1, message_dict2):
            if diff_function(message_dict1, message_dict2):
                acc += [(message_dict1, message_dict2)]
                #
                if self.verbose() > 0:
                    partition_int1 = message_dict1["partition"]
                    offset_int1 = message_dict1["offset"]
                    partition_int2 = message_dict2["partition"]
                    offset_int2 = message_dict2["offset"]
                    print(f"Found differing messages on 1) partition {partition_int1}, offset {offset_int1} and 2) partition {partition_int2}, offset {offset_int2}.")
                #
            return acc
        #
        return self.zip_foldl(resource1, storage2, resource2, zip_foldl_function, [], n=n, **kwargs)
    
    def diff(self, resource1, storage2, resource2, n=ALL_MESSAGES, **kwargs):
        def diff_function(message_dict1, message_dict2):
            return message_dict1["key"] != message_dict2["key"] or message_dict1["value"] != message_dict2["value"]
        return self.diff_fun(resource1, storage2, resource2, diff_function, n=n, **kwargs)

    #

    def grep_fun(self, topic_str, match_function, n=ALL_MESSAGES, **kwargs):
        def flatmap_function(message_dict):
            if match_function(message_dict):
                if self.verbose() > 0:
                    partition_int = message_dict["partition"]
                    offset_int = message_dict["offset"]
                    print(f"Found matching message on partition {partition_int}, offset {offset_int}.")
                return [message_dict]
            else:
                return []
        #
        (matching_message_dict_list, message_counter_int) = self.flatmap(topic_str, flatmap_function, n=n, **kwargs)
        #
        return matching_message_dict_list, len(matching_message_dict_list), message_counter_int

    def grep(self, topic_str, re_pattern_str, n=ALL_MESSAGES, **kwargs):
        def match_function(message_dict):
            pattern = re.compile(re_pattern_str)
            key_str = str(message_dict["key"])
            value_str = str(message_dict["value"])
            return pattern.match(key_str) is not None or pattern.match(value_str) is not None
        #
        return self.grep_fun(topic_str, match_function, n=n, **kwargs)
