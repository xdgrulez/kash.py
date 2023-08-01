from kashpy.kafka.schemaregistry import SchemaRegistry
from kashpy.storage import Storage
from kashpy.helpers import is_interactive, get_millis

#

class Kafka(Storage):
    def __init__(self, dir_str, config_str, mandatory_section_str_list, optional_section_str_list):
        super().__init__(dir_str, config_str, mandatory_section_str_list, optional_section_str_list)
        #
        self.dir_str = dir_str
        self.config_str = config_str
        #
        if "kafka" in mandatory_section_str_list:
            self.kafka_config_dict = self.config_dict["kafka"]
        else:
            self.kafka_config_dict = None
        #
        if "rest_proxy" in mandatory_section_str_list:
            self.rest_proxy_config_dict = self.config_dict["rest_proxy"]
        else:
            self.rest_proxy_config_dict = None
        #
        self.schema_registry_config_dict = self.config_dict["schema_registry"]
        #
        self.kash_config_dict = self.config_dict["kash"]
        #
        self.schemaRegistry = None
        self.admin = None
        #
        # cluster config kash section
        #
        if "flush.num.messages" not in self.kash_config_dict:
            self.flush_num_messages(10000)
        else:
            self.flush_num_messages(int(self.kash_config_dict["flush.num.messages"]))
        #
        if "flush.timeout" not in self.kash_config_dict:
            self.flush_timeout(-1.0)
        else:
            self.flush_timeout(float(self.kash_config_dict["flush.timeout"]))
        #
        if "retention.ms" not in self.kash_config_dict:
            self.retention_ms(604800000)
        else:
            self.retention_ms(int(self.kash_config_dict["retention.ms"]))
        #
        if "consume.timeout" not in self.kash_config_dict:
            self.consume_timeout(3.0)
        else:
            self.consume_timeout(float(self.kash_config_dict["consume.timeout"]))
        #
        if "consume.num.messages" not in self.kash_config_dict:
            self.consume_num_messages(1)
        else:
            self.consume_num_messages(int(self.kash_config_dict["consume.num.messages"]))
        #
        if "enable.auto.commit" not in self.kash_config_dict:
            self.enable_auto_commit(True)
        else:
            self.enable_auto_commit(bool(self.kash_config_dict["enable.auto.commit"]))
        #
        if "session.timeout.ms" not in self.kash_config_dict:
            self.session_timeout_ms(45000)
        else:
            self.session_timeout_ms(int(self.kash_config_dict["session.timeout.ms"]))
        #
        if "block.num.retries" not in self.kash_config_dict:
            self.block_num_retries(50)
        else:
            self.block_num_retries(int(self.kash_config_dict["block.num.retries"]))
        #
        if "block.interval" not in self.kash_config_dict:
            self.block_interval(0.1)
        else:
            self.block_interval(float(self.kash_config_dict["block.interval"]))
        #
        # both cluster and restproxy kash section
        #
        if "auto.offset.reset" not in self.kash_config_dict:
            self.auto_offset_reset("earliest")
        else:
            self.auto_offset_reset(str(self.kash_config_dict["auto.offset.reset"]))
        #
        if "consumer.group.prefix" not in self.kash_config_dict:
            self.consumer_group_prefix("")
        else:
            self.consumer_group_prefix(str(self.kash_config_dict["consumer.group.prefix"]))
        #
        # restproxy config kash section
        #
        if "auto.commit.enable" not in self.kash_config_dict:
            self.auto_commit_enable(True)
        else:
            self.auto_commit_enable(bool(self.kash_config_dict["auto.commit.enable"]))
        #
        if "fetch.min.bytes" not in self.kash_config_dict:
            self.fetch_min_bytes(-1)
        else:
            self.fetch_min_bytes(int(self.kash_config_dict["fetch.min.bytes"]))
        #
        if "consumer.request.timeout.ms" not in self.kash_config_dict:
            self.consumer_request_timeout_ms(1000)
        else:
            self.consumer_request_timeout_ms(int(self.kash_config_dict["consumer.request.timeout.ms"]))
        #
        if "consume.num.attempts" not in self.kash_config_dict:
            self.consume_num_attempts(5)
        else:
            self.consume_num_attempts(int(self.kash_config_dict["consume.num.attempts"]))
        #
        if "requests.num.retries" not in self.kash_config_dict:
            self.requests_num_retries(10)
        else:
            self.requests_num_retries(int(self.kash_config_dict["requests.num.retries"]))
        #
        if "schema.registry.url" in self.schema_registry_config_dict:
            self.schemaRegistry = self.get_schemaRegistry()
        else:
            self.schemaRegistry = None
        #

    #

    def flush_num_messages(self, new_value=None): # int
        return self.get_set_config("flush.num.messages", new_value)

    def flush_timeout(self, new_value=None): # float
        return self.get_set_config("flush.timeout", new_value)

    def retention_ms(self, new_value=None): # int
        return self.get_set_config("retention.ms", new_value)

    def consume_timeout(self, new_value=None): # float
        return self.get_set_config("consume.timeout", new_value)

    def consume_num_messages(self, new_value=None): # int
        return self.get_set_config("consume.num.messages", new_value)

    def enable_auto_commit(self, new_value=None): # bool
        return self.get_set_config("enable.auto.commit", new_value)

    def session_timeout_ms(self, new_value=None): # int
        return self.get_set_config("session.timeout.ms", new_value)

    def block_num_retries(self, new_value=None): # int
        return self.get_set_config("block.num.retries", new_value)

    def block_interval(self, new_value=None): # float
        return self.get_set_config("block.interval", new_value)

    #

    def auto_offset_reset(self, new_value=None): # str
        return self.get_set_config("auto.offset.reset", new_value)

    def consumer_group_prefix(self, new_value=None): # str
        return self.get_set_config("consumer.group.prefix", new_value)

    #

    def auto_commit_enable(self, new_value=None): # bool
        return self.get_set_config("auto.commit.enable", new_value)

    def fetch_min_bytes(self, new_value=None): # int
        return self.get_set_config("fetch.min.bytes", new_value)

    def consumer_request_timeout_ms(self, new_value=None): # int
        return self.get_set_config("consumer.request.timeout.ms", new_value)

    def consume_num_attempts(self, new_value=None): # int
        return self.get_set_config("consume.num.attempts", new_value)

    def requests_num_retries(self, new_value=None): # int
        return self.get_set_config("requests.num.retries", new_value)

    #

    def get_schemaRegistry(self):
        schemaRegistry = SchemaRegistry(self.schema_registry_config_dict, self.kash_config_dict)
        #
        return schemaRegistry

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

    # Topics

    def watermarks(self, pattern, timeout=-1.0):
        return self.admin.watermarks(pattern, timeout)

    def config(self, pattern):
        return self.admin.config(pattern)

    def set_config(self, pattern, config, test=False):
        return self.admin.set_config(pattern, config, test)
    
    def create(self, topic, partitions=1, config={}, block=True):
        return self.admin.create(topic, partitions, config, block)
    
    touch = create

    def delete(self, pattern, block=True):
        return self.admin.delete(pattern, block)

    rm = delete

    def offsets_for_times(self, pattern, partitions_timestamps, timeout=-1.0):
        return self.admin.offsets_for_times(pattern, partitions_timestamps, timeout)
    
    def partitions(self, pattern=None, verbose=False):
        return self.admin.partitions(pattern, verbose)

    def set_partitions(self, pattern, num_partitions, test=False):
        return self.admin.set_partitions(pattern, num_partitions, test)
    
    def list_topics(self, pattern):
        return self.admin.list_topics(pattern)

    # Groups

    def groups(self, pattern="*", state_pattern="*", state=False):
        return self.admin.groups(pattern, state_pattern, state)
    
    def describe_groups(self, pattern="*", state_pattern="*"):
        return self.admin.describe_groups(pattern, state_pattern)
    
    def delete_groups(self, pattern, state_pattern="*"):
        return self.admin.delete_groups(pattern, state_pattern)
    
    def group_offsets(self, pattern, state_pattern="*"):
        return self.admin.group_offsets(pattern, state_pattern)

    def set_group_offsets(self, group_offsets):
        return self.admin.set_group_offsets(group_offsets)

    # Brokers

    def brokers(self, pattern=None):
        return self.admin.brokers(pattern)
    
    def broker_config(self, pattern):
        return self.admin.broker_config(pattern)
    
    def set_broker_config(self, pattern, config, test=False):
        return self.admin.set_broker_config(pattern, config, test)

    # ACLs

    def acls(self, restype="any", name=None, resource_pattern_type="any", principal=None, host=None, operation="any", permission_type="any"):
        return self.admin.acls(restype, name, resource_pattern_type, principal, host, operation, permission_type)

    def create_acl(self, restype="any", name=None, resource_pattern_type="any", principal=None, host=None, operation="any", permission_type="any"):
        return self.admin.create_acl(restype, name, resource_pattern_type, principal, host, operation, permission_type)
    
    def delete_acl(self, restype="any", name=None, resource_pattern_type="any", principal=None, host=None, operation="any", permission_type="any"):
        return self.admin.delete_acl(restype, name, resource_pattern_type, principal, host, operation, permission_type)

    # Shared

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
            str_or_int = list(offsets_dict.keys())[0]
            if isinstance(str_or_int, int):
                offsets_dict = {topic_str: offsets_dict for topic_str in topic_str_list}
            else:
                offsets_dict = offsets_dict
        else:
            return None

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

    def get_key_value_schema_tuple(self, **kwargs):
        key_schema_str = kwargs["key_schema"] if "key_schema" in kwargs else None
        value_schema_str = kwargs["value_schema"] if "value_schema" in kwargs else None
        #
        key_schema_id_int = kwargs["key_schema_id"] if "key_schema_id" in kwargs else None
        value_schema_id_int = kwargs["value_schema_id"] if "value_schema_id" in kwargs else None
        #
        return (key_schema_str, value_schema_str, key_schema_id_int, value_schema_id_int)

    # Open
    def openr(self, topics, **kwargs):
        consumer = self.get_consumer(topics, **kwargs)
        #
        return consumer
        
    def openw(self, topic, **kwargs):
        producer = self.get_producer(topic, **kwargs)
        #
        return producer
