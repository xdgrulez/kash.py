from kashpy.storage import Storage

from fnmatch import fnmatch

class Kafka(Storage):
    def __init__(self, dir_str, config_str, mandatory_section_str_list, optional_section_str_list):
        super().__init__(dir_str, config_str, mandatory_section_str_list, optional_section_str_list)

    def size(self, pattern, timeout=-1.0):
        pattern_str_or_str_list = pattern
        #
        topic_str_partition_int_tuple_dict_dict = self.watermarks(pattern_str_or_str_list, timeout=timeout)
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

    def topics(self, pattern=None, size=False, partitions=False):
        pattern_str_or_str_list = pattern
        size_bool = size
        partitions_bool = partitions
        #
        if size_bool:
            topic_str_total_size_int_size_dict_tuple_dict = self.size(pattern_str_or_str_list)
            if partitions_bool:
                return topic_str_total_size_int_size_dict_tuple_dict
            else:
                topic_str_size_int_dict = {topic_str: topic_str_total_size_int_size_dict_tuple_dict[topic_str][0] for topic_str in topic_str_total_size_int_size_dict_tuple_dict}
                return topic_str_size_int_dict
        else:
            if partitions_bool:
                topic_str_total_size_int_size_dict_tuple_dict = self.size(pattern_str_or_str_list)
                topic_str_size_dict_dict = {topic_str: topic_str_total_size_int_size_dict_tuple_dict[topic_str][1] for topic_str in topic_str_total_size_int_size_dict_tuple_dict}
                return topic_str_size_dict_dict
            else:
                topic_str_list = self.list_topics()
                if pattern_str_or_str_list is not None:
                    if isinstance(pattern_str_or_str_list, str):
                        pattern_str_or_str_list = [pattern_str_or_str_list]
                    topic_str_list = [topic_str for topic_str in topic_str_list if any(fnmatch(topic_str, pattern_str) for pattern_str in pattern_str_or_str_list)]
                topic_str_list.sort()
                return topic_str_list

    ls = topics

    def l(self, pattern=None, size=True, partitions=False):
        return self.topics(pattern=pattern, size=size, partitions=partitions)

    ll = l

    def exists(self, topic_str):
        return self.topics(topic_str) != []
