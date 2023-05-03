from kashpy.kafka import Kafka
from kashpy.cluster.admin import Admin
from kashpy.cluster.consumer import Consumer
from kashpy.cluster.producer import Producer
from kashpy.cluster.schemaregistry import SchemaRegistry
from kashpy.helpers import is_interactive

# Cluster class

class Cluster(Kafka):
    def __init__(self, cluster_str):
        super().__init__("clusters", cluster_str, ["kafka"], ["schema_registry", "kash"])
        #
        self.kafka_config_dict = self.config_dict["kafka"]
        self.schema_registry_config_dict = self.config_dict["schema_registry"]
        self.kash_config_dict = self.config_dict["kash"]
        #
        self.schemaRegistry = None
        #
        self.admin = None
        #
        self.producer = None
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
        self.verbose_int = 1 if is_interactive() else 0
        #
        self.schemaRegistry = SchemaRegistry(self.schema_registry_config_dict, self.kash_config_dict)
        #
        self.admin = Admin(self.kafka_config_dict, self.kash_config_dict)
        #
        self.producer = Producer(self.kafka_config_dict, self.kash_config_dict)

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
    
    def list_topics(self):
        return self.admin.list_topics()

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
        return self.admin.alter_group_offsets(group_offsets)

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
