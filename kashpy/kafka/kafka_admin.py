class KafkaAdmin:
    def __init__(self, kafka_obj, **kwargs):
        self.kafka_obj = kafka_obj

    #

    def topics(self, pattern=None, size=False, **kwargs):
        pattern_str_or_str_list = pattern
        size_bool = size
        partitions_bool = "partitions" in kwargs and kwargs["partitions"]
        #
        timeout_int = kwargs["timeout"] if "timeout" in kwargs else -1.0
        #
        def size(pattern_str_or_str_list, timeout_int):
            topic_str_partition_int_tuple_dict_dict = self.watermarks(pattern_str_or_str_list, timeout=timeout_int)
            #
            topic_str_total_size_int_size_dict_tuple_dict = {}
            for topic_str, partition_int_tuple_dict in topic_str_partition_int_tuple_dict_dict.items():
                size_dict = {partition_int: partition_int_tuple_dict[partition_int][1]-partition_int_tuple_dict[partition_int][0] for partition_int in partition_int_tuple_dict.keys()}
                #
                total_size_int = 0
                for offset_int_tuple in partition_int_tuple_dict.values():
                    partition_size_int = offset_int_tuple[1] - offset_int_tuple[0]
                    total_size_int += partition_size_int
                #
                topic_str_total_size_int_size_dict_tuple_dict[topic_str] = (total_size_int, size_dict)
            return topic_str_total_size_int_size_dict_tuple_dict
        #

        if size_bool:
            topic_str_total_size_int_size_dict_tuple_dict = size(pattern_str_or_str_list, timeout_int)
            if partitions_bool:
                return topic_str_total_size_int_size_dict_tuple_dict
            else:
                topic_str_size_int_dict = {topic_str: topic_str_total_size_int_size_dict_tuple_dict[topic_str][0] for topic_str in topic_str_total_size_int_size_dict_tuple_dict}
                return topic_str_size_int_dict
        else:
            if partitions_bool:
                topic_str_total_size_int_size_dict_tuple_dict = size(pattern_str_or_str_list, timeout_int)
                topic_str_size_dict_dict = {topic_str: topic_str_total_size_int_size_dict_tuple_dict[topic_str][1] for topic_str in topic_str_total_size_int_size_dict_tuple_dict}
                return topic_str_size_dict_dict
            else:
                topic_str_list = self.list_topics(pattern_str_or_str_list)
                return topic_str_list
