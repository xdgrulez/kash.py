from kashpy.helpers import get_millis

# Constants

ALL_MESSAGES = -1

#

class KafkaConsumer:
    def __init__(self, kafka_obj, *topics, **kwargs):
        self.schema_registry_config_dict = kafka_obj.schema_registry_config_dict
        self.kash_config_dict = kafka_obj.kash_config_dict
        #
        # Topics
        #
        self.topic_str_list = list(topics)
        #
        # Group
        #
        self.group_str = self.get_group_str(**kwargs)
        #
        # Offsets
        #
        self.offsets_dict = self.get_offsets_dict(self.topic_str_list, **kwargs)
        #
        # Key and Value Types
        #
        (self.key_type, self.value_type) = kafka_obj.get_key_value_type_tuple(**kwargs)
        #
        (self.key_type_dict, self.value_type_dict) = self.get_key_value_type_dict_tuple(self.key_type, self.value_type, self.topic_str_list)

    #

    def foldl(self, foldl_function, initial_acc, n=ALL_MESSAGES, **kwargs):
        n_int = n
        read_batch_size_int = kwargs["read_batch_size"] if "read_batch_size" in kwargs else 1
        #
        break_function = kwargs["break_function"] if "break_function" in kwargs else lambda _, _1: False
        #
        acc = initial_acc
        message_counter_int = 0
        break_bool = False
        verbose_int = self.kash_config_dict["verbose"]
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
            #
            message_counter_int += len(message_dict_list)
            #
            if break_bool:
                break
            #
            if verbose_int > 0 and message_counter_int % self.kash_config_dict["progress.num.messages"] == 0:
                print(f"Consumed: {message_counter_int}")
            if n_int != ALL_MESSAGES:
                if message_counter_int >= n_int:
                    break
        #
        return (acc, message_counter_int)

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
            if "consumer.group.prefix" in self.kash_config_dict:
                prefix_str = self.kash_config_dict["consumer.group.prefix"]
            else:
                prefix_str = ""
            return prefix_str + str(get_millis())

    def get_offsets_dict(self, topic_str_list, **kwargs):
        if "offsets" in kwargs:
            offsets_dict = kwargs["offsets"]
            str_or_int = list(offsets_dict.keys())[0]
            if isinstance(str_or_int, int):
                offsets_dict = {topic_str: offsets_dict for topic_str in topic_str_list}
            else:
                offsets_dict = offsets_dict
        else:
            offsets_dict = None
        #
        return offsets_dict

    def get_key_value_type_dict_tuple(self, key_type, value_type, topic_str_list):
        if isinstance(key_type, dict):
            key_type_dict = key_type
        else:
            key_type_dict = {topic_str: key_type for topic_str in topic_str_list}
        if isinstance(value_type, dict):
            value_type_dict = value_type
        else:
            value_type_dict = {topic_str: value_type for topic_str in topic_str_list}
        #
        return (key_type_dict, value_type_dict)
