from kashpy.cluster.admin import Admin
from kashpy.cluster.consumer import Consumer
from kashpy.cluster.producer import Producer
from kashpy.schemaregistry import SchemaRegistry
from kashpy.helpers import is_interactive
from kashpy.storage import Storage

class Kafka(Storage):
    def __init__(self, dir_str, config_str, mandatory_section_str_list, optional_section_str_list):
        super().__init__(dir_str, config_str, mandatory_section_str_list, optional_section_str_list)
        #
        self.kafka_config_dict = self.config_dict["kafka"]
        self.schema_registry_config_dict = self.config_dict["schema_registry"]
        self.kash_config_dict = self.config_dict["kash"]
        #
        self.verbose_int = 1 if is_interactive() else 0
        #
        self.schemaRegistry = self.get_schemaRegistry()
        #
        self.admin = self.get_admin()
        #
        self.producer = self.get_producer()
        #
        self.consumer = self.get_consumer()

    #

    def get_schemaRegistry(self):
        return SchemaRegistry(self.schema_registry_config_dict, self.kash_config_dict)

    def get_admin(self):
        return Admin(self.kafka_config_dict, self.kash_config_dict)

    def get_producer(self):
        return Producer(self.kafka_config_dict, self.schema_registry_config_dict, self.kash_config_dict, self.config_str)

    def get_consumer(self):
        return Consumer(self.kafka_config_dict, self.schema_registry_config_dict, self.kash_config_dict)

    #

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
                topic_str_list = self.list_topics(pattern_str_or_str_list)
                return topic_str_list

    ls = topics

    def l(self, pattern=None, size=True, partitions=False):
        return self.topics(pattern=pattern, size=size, partitions=partitions)

    ll = l

    def exists(self, topic):
        topic_str = topic
        #
        return self.topics(topic_str) != []

    #

    def open(self, topics, mode, group=None, offsets=None, config={}, key_type="str", value_type="str", key_schema=None, value_schema=None, on_delivery=None):
        if "r" in mode:
            self.consumer = self.get_consumer()
            #
            self.consumer.subscribe(topics, group, offsets, config, key_type, value_type)
            return self.consumer
        elif "a" in mode:
            self.key_type_str = key_type
            self.value_type_str = value_type
            self.key_schema_str = key_schema
            self.value_schema_str = value_schema
            self.on_delivery_function = on_delivery
            #
            self.producer = self.get_producer()
            #
            return self.producer

    def read(size): # Optional. The number of bytes to return. Default -1, which means the whole file.
        #not supported

    def readline(size): # Optional. The number of bytes from the line to return. Default -1, which means the whole line.
    def consume(self, n=1):
        return self.consumer.consume(n)

    def readlines(hint): # Optional. If the number of bytes returned exceed the hint number, no more lines will be returned. Default value is  -1, which means all lines will be returned.
        return self.consumer.consume(n)
