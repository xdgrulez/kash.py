from kashpy.kafka import Kafka

# Constants

RD_KAFKA_PARTITION_UA = -1
CURRENT_TIME = 0

# Cluster class

class Cluster(Kafka):
    def __init__(self, cluster_str):
        super().__init__("clusters", cluster_str, ["kafka"], ["schema_registry", "kash"])
        #
        self.cluster_str = cluster_str
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
        if "auto.offset.reset" not in self.kash_config_dict:
            self.auto_offset_reset("earliest")
        else:
            self.auto_offset_reset(str(self.kash_config_dict["auto.offset.reset"]))
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
        if "progress.num.messages" not in self.kash_config_dict:
            self.progress_num_messages(1000)
        else:
            self.progress_num_messages(int(self.kash_config_dict["progress.num.messages"]))
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

    def get_set_config(self, config_key_str, new_value=None):
        if new_value is not None:
            self.kash_config_dict[config_key_str] = new_value
        #
        if self.schemaRegistry is not None:
            self.schemaRegistry.kash_config_dict[config_key_str] = new_value
        #
        if self.admin is not None:
            self.admin.kash_config_dict[config_key_str] = new_value
        #
        if self.producer is not None:
            self.producer.kash_config_dict[config_key_str] = new_value
        #
        return new_value

    #

    def flush_num_messages(self, new_value=None): # int
        return self.get_set_config("flush.num.messages", new_value)

    def flush_timeout(self, new_value=None): # float
        return self.get_set_config("flush.timeout", new_value)

    def retention_ms(self, new_value=None): # int
        return self.get_set_config("retention.ms", new_value)

    def consume_timeout(self, new_value=None): # float
        return self.get_set_config("consume.timeout", new_value)

    def auto_offset_reset(self, new_value=None): # str
        return self.get_set_config("auto.offset.reset", new_value)

    def enable_auto_commit(self, new_value=None): # bool
        return self.get_set_config("enable.auto.commit", new_value)

    def session_timeout_ms(self, new_value=None): # int
        return self.get_set_config("session.timeout.ms", new_value)

    def progress_num_messages(self, new_value=None): # int
        return self.get_set_config("progress.num.messages", new_value)

    def block_num_retries(self, new_value=None): # int
        return self.get_set_config("block.num.retries", new_value)

    def block_interval(self, new_value=None): # float
        return self.get_set_config("block.interval", new_value)

    #

    def verbose(self, new_value=None): # int
        if new_value is not None:
            self.verbose_int = new_value
        return self.verbose_int

    #
    # Admin
    #

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

    #
    # Producer
    #

    def produce(self, topic, value, key=None, key_type="str", value_type="str", key_schema=None, value_schema=None, partition=RD_KAFKA_PARTITION_UA, timestamp=CURRENT_TIME, headers=None, on_delivery=None):
        return self.producer.produce(topic, value, key, key_type, value_type, key_schema, value_schema, partition, timestamp, headers, on_delivery)

    def flush(self):
        return self.producer.flush()

    #
    # Consumer
    #

    def subscribe(self, topics, group=None, offsets=None, config={}, key_type="str", value_type="str"):
        return self.consumer.subscribe(topics, group, offsets, config, key_type, value_type)

    def unsubscribe(self):
        return self.consumer.unsubscribe()

    def close(self):
        return self.consumer.close()

    def consume(self, n=1):
        return self.consumer.consume(n)

    def commit(self, offsets=None, asynchronous=False):
        return self.consumer.commit(offsets, asynchronous)

    def offsets(self, timeout=-1.0):
        return self.consumer.offsets(timeout)

    def memberid(self):
        return self.consumer.memberid()
