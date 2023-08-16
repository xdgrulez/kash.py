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
            topic_str_size_int_partitions_dict_tuple_dict = {}
            for topic_str, partition_int_tuple_dict in topic_str_partition_int_tuple_dict_dict.items():
                partitions_dict = {partition_int: partition_int_tuple_dict[partition_int][1]-partition_int_tuple_dict[partition_int][0] for partition_int in partition_int_tuple_dict.keys()}
                #
                size_int = 0
                for offset_int_tuple in partition_int_tuple_dict.values():
                    partition_size_int = offset_int_tuple[1] - offset_int_tuple[0]
                    size_int += partition_size_int
                #
                topic_str_size_int_partitions_dict_tuple_dict[topic_str] = (size_int, partitions_dict)
            return topic_str_size_int_partitions_dict_tuple_dict
        #

        if size_bool:
            topic_str_size_int_partitions_dict_tuple_dict = size(pattern_str_or_str_list, timeout_int)
            if partitions_bool:
                # e.g. {"topic": {"size": 42, "partitions": {0: 23, 1: 4711}}}
                topic_str_size_int_partitions_dict_dict = {topic_str: {"size": size_int_partitions_dict_tuple[0], "partitions": size_int_partitions_dict_tuple[1]} for topic_str, size_int_partitions_dict_tuple in topic_str_size_int_partitions_dict_tuple_dict.items()}
                return topic_str_size_int_partitions_dict_dict
            else:
                # e.g. {"topic": 42}
                topic_str_size_int_dict = {topic_str: size_int_partitions_dict_tuple[0] for topic_str, size_int_partitions_dict_tuple in topic_str_size_int_partitions_dict_tuple_dict.items()}
                return topic_str_size_int_dict
        else:
            if partitions_bool:
                # e.g. {"topic": {0: 23, 1: 4711}}
                topic_str_size_int_partitions_dict_tuple_dict = size(pattern_str_or_str_list, timeout_int)
                topic_str_partitions_dict_dict = {topic_str: size_int_partitions_dict_tuple[1] for topic_str, size_int_partitions_dict_tuple in topic_str_size_int_partitions_dict_tuple_dict.items()}
                return topic_str_partitions_dict_dict
            else:
                # e.g. ["topic"]
                topic_str_list = self.list_topics(pattern_str_or_str_list)
                return topic_str_list
