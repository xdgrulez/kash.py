from kashpy.helpers import get_millis

from kashpy.storage_reader import StorageReader

# Constants

ALL_MESSAGES = -1

#

class KafkaReader(StorageReader):
    def __init__(self, kafka_obj, *topics, **kwargs):
        super().__init__(kafka_obj, *topics, **kwargs)
        #
        self.topic_str_list = self.resource_str_list
        #
        self.topic_str_offsets_dict_dict = self.resource_str_offsets_dict_dict
        #
        self.group_str = self.get_group_str(**kwargs)

    #

    def foldl(self, foldl_function, initial_acc, n=ALL_MESSAGES, **kwargs):
        n_int = n
        #
        read_batch_size_int = kwargs["read_batch_size"] if "read_batch_size" in kwargs else self.storage_obj.read_batch_size()
        if n != ALL_MESSAGES and read_batch_size_int > n_int:
            read_batch_size_int = n_int
        #
        break_function = kwargs["break_function"] if "break_function" in kwargs else lambda _, _1: False
        #
        message_counter_int = 0
        #
        acc = initial_acc
        break_bool = False
        while True:
            message_dict_list = self.consume(n=read_batch_size_int, **kwargs)
            if not message_dict_list:
                break
            #
            for message_dict in message_dict_list:
                if break_function(acc, message_dict):
                    break_bool = True
                    break
                acc = foldl_function(acc, message_dict)
                message_counter_int += 1
            #
            if break_bool:
                break
            #
            if n_int != ALL_MESSAGES:
                if message_counter_int >= n_int:
                    break
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

    # Helpers

    def get_group_str(self, **kwargs):
        if "group" in kwargs:
            return kwargs["group"]
        else:
            prefix_str = self.storage_obj.consumer_group_prefix()
            return prefix_str + str(get_millis())
