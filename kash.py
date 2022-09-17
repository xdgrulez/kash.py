from confluent_kafka import Consumer, KafkaError, OFFSET_BEGINNING, OFFSET_END, OFFSET_INVALID, OFFSET_STORED, Producer, TIMESTAMP_CREATE_TIME, TopicPartition
from confluent_kafka.admin import AclBinding, AclBindingFilter, AclOperation, AclPermissionType, AdminClient, ConfigResource, NewPartitions, NewTopic, ResourceType, ResourcePatternType
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer, JSONSerializer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer, ProtobufSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from google.protobuf.json_format import MessageToDict, ParseDict
import configparser
import importlib
import json
import os
import requests
import sys
import time

# Constants

ALL_MESSAGES=-1

# Helpers

def is_file(str):
    return str.startswith("./")


def get_millis():
    return int(time.time()*1000)


def create_unique_group_id():
    return str(get_millis())


def count_lines(path_str):
    def count_generator(reader):
        bytes = reader(1024 * 1024)
        while bytes:
            yield bytes
            bytes = reader(1024 * 1024)
    #
    with open(path_str, "rb") as bufferedReader:
        c_generator = count_generator(bufferedReader.raw.read)
        # count each \n
        count_int = sum(buffer.count(b'\n') for buffer in c_generator)
    #
    return count_int


def foreach_line(path_str, proc_function, delimiter='\n', bufsize=4096):
    """Summary line.

    Extended description of the function.

    Args:
        arg1 (str): Description of arg1
        arg2 (int): Description of arg2

    Returns:
        bool: Description of return value
    """
    delimiter_str = delimiter
    bufsize_int = bufsize
    #
    buf_str = ""
    #
    line_counter_int = 0
    #
    file_line_count_int = count_lines(path_str)
    #
    with open(path_str) as textIOWrapper:
        while True:
            newbuf_str = textIOWrapper.read(bufsize_int)
            if not newbuf_str:
                if buf_str:
                    proc_function(buf_str)
                    line_counter_int += 1
                    if line_counter_int % 1000 == 0:
                        print(f"{line_counter_int}/{file_line_count_int}")
                break
            buf_str += newbuf_str
            line_str_list = buf_str.split(delimiter_str)
            for line_str in line_str_list[:-1]:
                proc_function(line_str)
                line_counter_int += 1
                if line_counter_int % 1000 == 0:
                    print(f"{line_counter_int}/{file_line_count_int}")
            buf_str = line_str_list[-1]


# Get cluster configurations

def get_config_dict(cluster_str):
    rawConfigParser = configparser.RawConfigParser()
    if os.path.exists(f"./clusters_secured/{cluster_str}.conf"):
        rawConfigParser.read(f"./clusters_secured/{cluster_str}.conf")
        cluster_dir_str = "clusters_secured"
    elif os.path.exists(f"./clusters_unsecured/{cluster_str}.conf"):
        rawConfigParser.read(f"./clusters_unsecured/{cluster_str}.conf")
        cluster_dir_str = "clusters_unsecured"
    else:
        raise Exception(f"No cluster configuration file \"{cluster_str}.conf\" found in \"clusters_secured\" and \"clusters_unsecured\".")
    #
    config_dict = dict(rawConfigParser.items("kafka"))
    #
    schema_registry_config_dict = dict(rawConfigParser.items("schema_registry"))
    #
    return config_dict, schema_registry_config_dict, cluster_dir_str


# Get AdminClient, Producer and Consumer objects from a configuration dictionary

def get_adminClient(config_dict):
    adminClient = AdminClient(config_dict)
    return adminClient


def get_producer(config_dict):
    producer = Producer(config_dict)
    return producer


def get_consumer(config_dict):
    consumer = Consumer(config_dict)
    return consumer


def get_schemaRegistryClient(config_dict):
    dict = {"url": config_dict["schema.registry.url"]}
    schemaRegistryClient = SchemaRegistryClient(dict)
    return schemaRegistryClient 


# Conversion functions from confluent_kafka objects to kash.py basic Python datatypes like strings and dictionaries

def offset_int_to_int_or_str(offset_int):
    if offset_int >= 0:
        return offset_int
    else:
        if offset_int == OFFSET_BEGINNING:
            return "OFFSET_BEGINNING"
        elif offset_int == OFFSET_END:
            return "OFFSET_END"
        elif offset_int == OFFSET_INVALID:
            return "OFFSET_INVALID"
        elif offset_int == OFFSET_STORED:
            return "OFFSET_STORED"
        else:
            return offset_int


def groupMetadata_to_group_dict(groupMetadata):
    group_dict = {"id": groupMetadata.id, "error": kafkaError_to_error_dict(groupMetadata.error), "state": groupMetadata.state, "protocol_type": groupMetadata.protocol_type, "protocol": groupMetadata.protocol, "members": [groupMember_to_dict(groupMember) for groupMember in groupMetadata.members]}
    return group_dict


def partitionMetadata_to_partition_dict(partitionMetadata):
    partition_dict = {"id": partitionMetadata.id, "leader": partitionMetadata.leader, "replicas": partitionMetadata.replicas, "isrs": partitionMetadata.isrs, "error": kafkaError_to_error_dict(partitionMetadata.error)}
    return partition_dict


def topicMetadata_to_topic_dict(topicMetadata):
    partitions_dict = {partition_int: partitionMetadata_to_partition_dict(partitionMetadata) for partition_int, partitionMetadata in topicMetadata.partitions.items()}
    topic_dict = {"topic": topicMetadata.topic, "partitions": partitions_dict, "error": kafkaError_to_error_dict(topicMetadata.error)}
    return topic_dict


def kafkaError_to_error_dict(kafkaError):
    error_dict = None
    if kafkaError:
        error_dict = {"code": kafkaError.code(), "fatal": kafkaError.fatal(), "name": kafkaError.name(), "retriable": kafkaError.retriable(), "str": kafkaError.str(), "txn_requires_abort": kafkaError.txn_requires_abort()}
    return error_dict


def str_to_resourceType(restype_str):
    restype_str1 = restype_str.lower()
    if restype_str1 == "unknown":
        return ResourceType.UNKNOWN
    elif restype_str1 == "any":
        return ResourceType.ANY
    elif restype_str1 == "topic":
        return ResourceType.TOPIC
    elif restype_str1 == "group":
        return ResourceType.GROUP
    elif restype_str1 == "broker":
        return ResourceType.BROKER


def resourceType_to_str(resourceType):
    if resourceType == ResourceType.UNKNOWN:
        return "unknown"
    elif resourceType == ResourceType.ANY:
        return "any"
    elif resourceType == ResourceType.TOPIC:
        return "topic"
    elif resourceType == ResourceType.GROUP:
        return "group"
    elif resourceType == ResourceType.BROKER:
        return "broker"


def str_to_resourcePatternType(resource_pattern_type_str):
    resource_pattern_type_str1 = resource_pattern_type_str.lower()
    if resource_pattern_type_str1 == "unknown":
        return ResourcePatternType.UNKNOWN
    elif resource_pattern_type_str1 == "any":
        return ResourcePatternType.ANY
    elif resource_pattern_type_str1 == "match":
        return ResourcePatternType.MATCH
    elif resource_pattern_type_str1 == "literal":
        return ResourcePatternType.LITERAL
    elif resource_pattern_type_str1 == "prefixed":
        return ResourcePatternType.PREFIXED


def resourcePatternType_to_str(resourcePatternType):
    if resourcePatternType == ResourcePatternType.UNKNOWN:
        return "unknown"
    elif resourcePatternType == ResourcePatternType.ANY:
        return "any"
    elif resourcePatternType == ResourcePatternType.MATCH:
        return "match"
    elif resourcePatternType == ResourcePatternType.LITERAL:
        return "literal"
    elif resourcePatternType == ResourcePatternType.PREFIXED:
        return "prefixed"


def str_to_aclOperation(operation_str):
    operation_str1 = operation_str.lower()
    if operation_str1 == "unknown":
        return AclOperation.UNKNOWN
    elif operation_str1 == "any":
        return AclOperation.ANY
    elif operation_str1 == "all":
        return AclOperation.ALL
    elif operation_str1 == "read":
        return AclOperation.READ
    elif operation_str1 == "write":
        return AclOperation.WRITE
    elif operation_str1 == "create":
        return AclOperation.CREATE
    elif operation_str1 == "delete":
        return AclOperation.DELETE
    elif operation_str1 == "alter":
        return AclOperation.ALTER
    elif operation_str1 == "describe":
        return AclOperation.DESCRIBE
    elif operation_str1 == "cluster_action":
        return AclOperation.CLUSTER_ACTION
    elif operation_str1 == "describe_configs":
        return AclOperation.DESCRIBE_CONFIGS
    elif operation_str1 == "alter_configs":
        return AclOperation.ALTER_CONFIGS
    elif operation_str1 == "itempotent_write":
        return AclOperation.IDEMPOTENT_WRITE

    
def aclOperation_to_str(aclOperation):
    if aclOperation == AclOperation.UNKNOWN:
        return "unknown"
    elif aclOperation == AclOperation.ANY:
        return "any"
    elif aclOperation == AclOperation.ALL:
        return "all"
    elif aclOperation == AclOperation.READ:
        return "read"
    elif aclOperation == AclOperation.WRITE:
        return "write"
    elif aclOperation == AclOperation.CREATE:
        return "create"
    elif aclOperation == AclOperation.DELETE:
        return "delete"
    elif aclOperation == AclOperation.ALTER:
        return "alter"
    elif aclOperation == AclOperation.DESCRIBE:
        return "describe"
    elif aclOperation == AclOperation.CLUSTER_ACTION:
        return "cluster_action"
    elif aclOperation == AclOperation.DESCRIBE_CONFIGS:
        return "describe_configs"
    elif aclOperation == AclOperation.ALTER_CONFIGS:
        return "alter_configs"
    elif aclOperation == AclOperation.IDEMPOTENT_WRITE:
        return "itempotent_write"
    

def str_to_aclPermissionType(permission_type_str):
    permission_type_str1 = permission_type_str.lower()
    if permission_type_str1 == "unknown":
        return AclPermissionType.UNKNOWN
    elif permission_type_str1 == "any":
        return AclPermissionType.ANY
    elif permission_type_str1 == "deny":
        return AclPermissionType.DENY
    elif permission_type_str1 == "allow":
        return AclPermissionType.ALLOW

    
def aclPermissionType_to_str(aclPermissionType):
    if aclPermissionType == AclPermissionType.UNKNOWN:
        return "unknown"
    elif aclPermissionType == AclPermissionType.ANY:
        return "any"
    elif aclPermissionType == AclPermissionType.DENY:
        return "deny"
    elif aclPermissionType == AclPermissionType.ALLOW:
        return "allow"


def aclBinding_to_dict(aclBinding):
    dict = {"restype": resourceType_to_str(aclBinding.restype),
    "name": aclBinding.name,
    "resource_pattern_type": resourcePatternType_to_str(aclBinding.resource_pattern_type),
    "principal": aclBinding.principal,
    "host": aclBinding.host,
    "operation": aclOperation_to_str(aclBinding.operation),
    "permission_type": aclPermissionType_to_str(aclBinding.permission_type)}
    return dict


def groupMember_to_dict(groupMember):
    dict = {"id": groupMember.id,
    "client_id": groupMember.client_id,
    "client_host": groupMember.client_host,
    "metadata": groupMember.metadata,
    "assignment": groupMember.assignment}
    return dict


# Cross-cluster

def replicate(source_cluster, source_topic_str, target_cluster, target_topic_str, group=None, map=None, keep_timestamps=True, timeout=1.0):
    group_str = group
    map_function = map
    keep_timestamps_bool = keep_timestamps#
    #
    source_cluster.subscribe(source_topic_str, group=group_str)
    while True:
        message_dict_list = source_cluster.consume(key_type="bytes", value_type="bytes")
        if not message_dict_list:
            break
        for message_dict in message_dict_list:
            if map_function:
                message_dict = map_function(message_dict)
            #
            if keep_timestamps_bool:
                timestamp_int_int_tuple = message_dict["timestamp"]
                if timestamp_int_int_tuple[0] == TIMESTAMP_CREATE_TIME:
                    timestamp_int = timestamp_int_int_tuple[1]
                    #
                    target_cluster.producer.produce(target_topic_str, key=message_dict["key"], value=message_dict["value"], partition=message_dict["partition"], timestamp=timestamp_int, headers=message_dict["headers"])
                    #
                    continue
            target_cluster.producer.produce(target_topic_str, key=message_dict["key"], value=message_dict["value"], partition=message_dict["partition"], headers=message_dict["headers"])
        target_cluster.flush()
    source_cluster.unsubscribe()

# Shell alias
def cp(source_cluster, source_topic_str, target_cluster, target_topic_str, group=None, map=None, keep_timestamps=True):
    replicate(source_cluster, source_topic_str, target_cluster, target_topic_str, group, map, keep_timestamps)


# Main kash.py class

class Cluster:
    def __init__(self, cluster_str):
        self.cluster_str = cluster_str
        self.config_dict, self.schema_registry_config_dict, self.cluster_dir_str = get_config_dict(cluster_str)
        #
        self.adminClient = get_adminClient(self.config_dict)
        #
        self.producer = get_producer(self.config_dict)
        self.produced_messages_int = 0
        #
        if self.schema_registry_config_dict:
            self.schemaRegistryClient = get_schemaRegistryClient(self.schema_registry_config_dict)
        else:
            self.schemaRegistryClient = None
        #
        self.subscribed_topic_str = None
        self.subscribed_key_type_str = None
        self.subscribed_value_type_str = None
        self.last_consumed_message = None
        #
        self.schema_id_int_generalizedProtocolMessageType_dict = {}
        self.schema_id_int_avro_schema_str_dict = {}
        self.schema_id_int_jsonschema_str_dict = {}
        # all kinds of timeouts
        self.timeout_float = 1.0
        # Consumer
        # auto.offset.reset (earliest or latest (confluent_kafka default: latest))
        self.auto_offset_reset_str = "earliest"
        # enable.auto.commit (True or False (confluent_kafka default: True))
        self.auto_commit_bool = True
        # session.timeout.ms (6000-300000 (confluent_kafka default: 30000))
        self.session_timeout_ms_int = 10000

    def timeout(self):
        return self.timeout_float

    def set_timeout(self, timeout_float):
        self.timeout_float = timeout_float

    #

    def auto_commit(self):
        return self.auto_commit_bool

    def set_auto_commit(self, auto_commit_bool):
        self.auto_commit_bool = auto_commit_bool

    #

    def session_timeout_ms(self):
        return self.session_timeout_ms_int

    def set_session_timeout_ms(self, session_timeout_ms_int):
        self.session_timeout_ms_int = session_timeout_ms_int

    #

    def auto_offset_reset(self):
        return self.auto_offset_reset_str

    def set_auto_offset_reset(self, auto_offset_reset_str):
        self.auto_offset_reset_str = auto_offset_reset_str

    # Schema Registry helper methods (inside the Cluster class to do caching etc.)

    def post_schema(self, schema_str, schema_type_str, topic_str, key_bool):
        key_or_value_str = "key" if key_bool else "value"
        #
        schema_registry_url_str = self.schema_registry_config_dict["schema.registry.url"]
        url_str = f"{schema_registry_url_str}/subjects/{topic_str}-{key_or_value_str}/versions?normalize=true"
        headers_dict = {"Accept": "application/vnd.schemaregistry.v1+json", "Content-Type": "application/vnd.schemaregistry.v1+json"}
        schema_dict = {"schema": schema_str, "schemaType": schema_type_str}
        response = requests.post(url_str, headers=headers_dict, json=schema_dict)
        response_dict = response.json()
        schema_id_int = response_dict["id"]
        return schema_id_int

    def schema_str_to_generalizedProtocolMessageType(self, schema_str, topic_str, key_bool):
        schema_id_int = self.post_schema(schema_str, "PROTOBUF", topic_str, key_bool)
        #
        generalizedProtocolMessageType = self.schema_id_int_and_schema_str_to_generalizedProtocolMessageType(schema_id_int, schema_str)
        return generalizedProtocolMessageType

    def schema_id_int_to_generalizedProtocolMessageType(self, schema_id_int):
        schema = self.schemaRegistryClient.get_schema(schema_id_int)
        schema_str = schema.schema_str
        #
        generalizedProtocolMessageType = self.schema_id_int_and_schema_str_to_generalizedProtocolMessageType(schema_id_int, schema_str)
        return generalizedProtocolMessageType

    def schema_id_int_to_avro_schema_str(self, schema_id_int):
        schema = self.schemaRegistryClient.get_schema(schema_id_int)
        avro_schema_str = schema.schema_str
        #
        return avro_schema_str

    def schema_id_int_to_jsonschema_str(self, schema_id_int):
        schema = self.schemaRegistryClient.get_schema(schema_id_int)
        jsonschema_str = schema.schema_str
        #
        return jsonschema_str

    def schema_id_int_and_schema_str_to_generalizedProtocolMessageType(self, schema_id_int, schema_str):
        path_str = f"/tmp/kash.py/{self.cluster_dir_str}/{self.cluster_str}"
        os.makedirs(path_str, exist_ok=True)
        file_str = f"schema_{schema_id_int}.proto"
        file_path_str = f"{path_str}/{file_str}"
        with open(file_path_str, "w") as textIOWrapper:
            textIOWrapper.write(schema_str)
        #
        import grpc_tools.protoc
        grpc_tools.protoc.main(["protoc", f"-I{path_str}", f"--python_out={path_str}", f"{file_str}"])
        #
        sys.path.insert(1, path_str)
        schema_module = importlib.import_module(f"schema_{schema_id_int}_pb2")
        schema_name_str = list(schema_module.DESCRIPTOR.message_types_by_name.keys())[0]
        generalizedProtocolMessageType = getattr(schema_module, schema_name_str)
        return generalizedProtocolMessageType

    def bytes_protobuf_to_dict(self, bytes):
        schema_id_int = int.from_bytes(bytes[1:5], "big")
        if schema_id_int in self.schema_id_int_generalizedProtocolMessageType_dict:
            generalizedProtocolMessageType = self.schema_id_int_generalizedProtocolMessageType_dict[schema_id_int]
        else:
            generalizedProtocolMessageType = self.schema_id_int_to_generalizedProtocolMessageType(schema_id_int)
            self.schema_id_int_generalizedProtocolMessageType_dict[schema_id_int] = generalizedProtocolMessageType
        #
        protobufDeserializer = ProtobufDeserializer(generalizedProtocolMessageType, {"use.deprecated.format": False}) 
        protobuf_message = protobufDeserializer(bytes, None)
        dict = MessageToDict(protobuf_message)
        return dict

    def bytes_avro_to_dict(self, bytes):
        schema_id_int = int.from_bytes(bytes[1:5], "big")
        if schema_id_int in self.schema_id_int_avro_schema_str_dict:
            avro_schema_str = self.schema_id_int_avro_schema_str_dict[schema_id_int]
        else:
            avro_schema_str = self.schema_id_int_to_avro_schema_str(schema_id_int)
            self.schema_id_int_avro_schema_str_dict[schema_id_int] = avro_schema_str
        #
        avroDeserializer = AvroDeserializer(self.schemaRegistryClient, avro_schema_str)
        dict = avroDeserializer(bytes, None)
        return dict

    def bytes_jsonschema_to_dict(self, bytes):
        schema_id_int = int.from_bytes(bytes[1:5], "big")
        if schema_id_int in self.schema_id_int_jsonschema_str_dict:
            jsonschema_str = self.schema_id_int_jsonschema_str_dict[schema_id_int]
        else:
            jsonschema_str = self.schema_id_int_to_jsonschema_str(schema_id_int)
            self.schema_id_int_jsonschema_str_dict[schema_id_int] = jsonschema_str
        #
        jsonDeserializer = JSONDeserializer(jsonschema_str)
        dict = jsonDeserializer(bytes, None)
        return dict

    # Deserialize a message to a message dictionary

    def message_to_message_dict(self, message, key_type="str", value_type="str"):
        key_type_str = key_type
        value_type_str = value_type
        #
        def bytes_to_str(bytes):
            if bytes:
                return bytes.decode("utf-8")
            else:
                return bytes
        #
        def bytes_to_bytes(bytes):
            return bytes
        #
        if key_type_str == "str":
            decode_key = bytes_to_str
        elif key_type_str == "bytes":
            decode_key = bytes_to_bytes
        elif key_type_str in ["pb", "protobuf"]:
            decode_key = self.bytes_protobuf_to_dict
        elif key_type_str == "avro":
            decode_key = self.bytes_avro_to_dict
        elif key_type_str in ["json", "jsonschema"]:
            decode_key = self.bytes_jsonschema_to_dict
        #
        if value_type_str == "str":
            decode_value = bytes_to_str
        elif value_type_str == "bytes":
            decode_value = bytes_to_bytes
        elif value_type_str in ["pb", "protobuf"]:
            decode_value = self.bytes_protobuf_to_dict
        elif value_type_str == "avro":
            decode_value = self.bytes_avro_to_dict
        elif value_type_str in ["json", "jsonschema"]:
            decode_value = self.bytes_jsonschema_to_dict
        #
        message_dict = {"headers": message.headers(), "partition": message.partition(), "offset": message.offset(), "timestamp": message.timestamp(), "key": decode_key(message.key()), "value": decode_value(message.value())}
        return message_dict

    # Configuration helpers

    def get_config_dict(self, resourceType, resource_str):
        configResource = ConfigResource(resourceType, resource_str)
        # configEntry_dict: ConfigResource -> ConfigEntry
        configEntry_dict = self.adminClient.describe_configs([configResource])[configResource].result()
        # config_dict: str -> str
        config_dict = {config_key_str: configEntry.value for config_key_str, configEntry in configEntry_dict.items()}
        return config_dict

    def set_config_dict(self, resourceType, resource_str, new_config_dict, test=False):
        test_bool = test
        #
        old_config_dict = self.get_config_dict(resourceType, resource_str)
        #
        if resourceType == ResourceType.BROKER:
            # https://docs.confluent.io/platform/current/installation/configuration/broker-configs.html#cp-config-brokers
            white_list_key_str_list = ["advertised.listeners", "background.threads", "compression.type", "confluent.balancer.enable", "confluent.balancer.heal.uneven.load.trigger", "confluent.balancer.throttle.bytes.per.second", "confluent.tier.local.hotset.bytes", "confluent.tier.local.hotset.ms", "listeners", "log.flush.interval.messages", "log.flush.interval.ms", "log.retention.bytes", "log.retention.ms", "log.roll.jitter.ms", "log.roll.ms", "log.segment.bytes", "log.segment.delete.delay.ms", "message.max.bytes", "min.insync.replicas", "num.io.threads", "num.network.threads", "num.recovery.threads.per.data.dir", "num.replica.fetchers", "unclean.leader.election.enable", "confluent.balancer.exclude.topic.names", "confluent.balancer.exclude.topic.prefixes", "confluent.clm.enabled", "confluent.clm.frequency.in.hours", "confluent.clm.max.backup.days", "confluent.clm.min.delay.in.minutes", "confluent.clm.topic.retention.days.to.backup.days", "confluent.cluster.link.fetch.response.min.bytes", "confluent.cluster.link.fetch.response.total.bytes", "confluent.cluster.link.io.max.bytes.per.second", "confluent.tier.enable", "confluent.tier.max.partition.fetch.bytes.override", "log.cleaner.backoff.ms", "log.cleaner.dedupe.buffer.size", "log.cleaner.delete.retention.ms", "log.cleaner.io.buffer.load.factor", "log.cleaner.io.buffer.size", "log.cleaner.io.max.bytes.per.second", "log.cleaner.max.compaction.lag.ms", "log.cleaner.min.cleanable.ratio", "log.cleaner.min.compaction.lag.ms", "log.cleaner.threads", "log.cleanup.policy", "log.deletion.max.segments.per.run", "log.index.interval.bytes", "log.index.size.max.bytes", "log.message.timestamp.difference.max.ms", "log.message.timestamp.type", "log.preallocate", "max.connection.creation.rate", "max.connections", "max.connections.per.ip", "max.connections.per.ip.overrides", "principal.builder.class", "sasl.enabled.mechanisms", "sasl.jaas.config", "sasl.kerberos.kinit.cmd", "sasl.kerberos.min.time.before.relogin", "sasl.kerberos.principal.to.local.rules", "sasl.kerberos.service.name", "sasl.kerberos.ticket.renew.jitter", "sasl.kerberos.ticket.renew.window.factor", "sasl.login.refresh.buffer.seconds", "sasl.login.refresh.min.period.seconds", "sasl.login.refresh.window.factor", "sasl.login.refresh.window.jitter", "sasl.mechanism.inter.broker.protocol", "ssl.cipher.suites", "ssl.client.auth", "ssl.enabled.protocols", "ssl.keymanager.algorithm", "ssl.protocol", "ssl.provider", "ssl.trustmanager.algorithm", "confluent.cluster.link.replication.quota.mode", "confluent.metadata.server.cluster.registry.clusters", "confluent.reporters.telemetry.auto.enable", "confluent.security.event.router.config", "confluent.telemetry.enabled", "confluent.tier.topic.delete.backoff.ms", "confluent.tier.topic.delete.check.interval.ms", "confluent.tier.topic.delete.max.inprogress.partitions", "follower.replication.throttled.rate", "follower.replication.throttled.replicas", "leader.replication.throttled.rate", "leader.replication.throttled.replicas", "listener.security.protocol.map", "log.message.downconversion.enable", "metric.reporters", "ssl.endpoint.identification.algorithm", "ssl.engine.factory.class", "ssl.secure.random.implementation"]
        #
        alter_config_dict = {}
        for key_str, value_str in old_config_dict.items():
            if resourceType == ResourceType.BROKER:
                if key_str not in white_list_key_str_list:
                    continue
            if key_str in new_config_dict:
                value_str = new_config_dict[key_str]
            if value_str:
                alter_config_dict[key_str] = value_str
        #
        alter_configResource = ConfigResource(resourceType, resource_str, set_config=alter_config_dict)
        #
        future = self.adminClient.alter_configs([alter_configResource], validate_only=test_bool)[alter_configResource]
        #
        future.result()

    # AdminClient - topics

    def topics(self, topic=None, size=False):
        size_bool = size
        topic_str = topic
        #
        if topic_str != None:
            if self.exists(topic_str):
                topic_str_list = [topic_str]
            else:
                topic_str_list = []
        else:
            topic_str_list = list(self.adminClient.list_topics().topics.keys())
            topic_str_list.sort()
        #
        if size_bool:
            topic_str_size_int_tuple_list = [(topic_str, self.size(topic_str)[1]) for topic_str in topic_str_list]
            return topic_str_size_int_tuple_list
        else:
            return topic_str_list

    def ls(self, topic=None):
        return self.topics(topic=topic, size=False)

    # Shell alias
    def l(self, topic=None):
        return self.topics(topic=topic, size=True)

    # Shell alias
    def ll(self, topic=None):
        return self.topics(topic=topic, size=True)

    def config(self, topic_str):
        return self.get_config_dict(ResourceType.TOPIC, topic_str)

    def set_config(self, topic_str, key_str, value_str, test=False):
        self.set_config_dict(ResourceType.TOPIC, topic_str, {key_str: value_str}, test)

    def create(self, topic_str, partitions=1, retention_ms=-1, operation_timeout=0):
        partitions_int = partitions
        retention_ms_int = retention_ms
        operation_timeout_float = operation_timeout
        #
        newTopic = NewTopic(topic_str, partitions_int, config={"retention.ms": retention_ms_int})
        self.adminClient.create_topics([newTopic], operation_timeout=operation_timeout_float)

    # Shell alias
    def mk(self, topic_str, partitions=1, retention_ms=-1):
        self.create(topic_str, partitions, retention_ms)

    def delete(self, topic_str, operation_timeout=0):
        operation_timeout_float = operation_timeout
        #
        self.adminClient.delete_topics([topic_str], operation_timeout=operation_timeout_float)

    # Shell alias
    def rm(self, topic_str):
        self.delete(topic_str)

    def describe(self, topic_str):
        topicMetadata = self.adminClient.list_topics(topic=topic_str).topics[topic_str]
        topic_dict = {}
        if topicMetadata:
            topic_dict = topicMetadata_to_topic_dict(topicMetadata)
        return topic_dict

    def exists(self, topic_str):
        topic_dict = self.describe(topic_str)
        if topic_dict["error"] != None:
            if topic_dict["error"]["code"] == KafkaError.UNKNOWN_TOPIC_OR_PART:
                return False
        return True

    def partitions(self, topic_str):
        partitions_int = len(self.adminClient.list_topics(topic=topic_str).topics[topic_str].partitions)
        return partitions_int

    def set_partitions(self, topic_str, new_partitions_int, test=False):
        test_bool = test
        #
        newPartitions = NewPartitions(topic_str, new_partitions_int)
        self.adminClient.create_partitions([newPartitions], validate_only=test_bool)

    def size(self, topic_str, verbose=True):
        verbose_bool = verbose
        #
        watermarks_dict = self.watermarks(topic_str)
        #
        size_dict = {partition_int: watermarks_dict[partition_int][1]-watermarks_dict[partition_int][0] for partition_int in watermarks_dict.keys()}
        #
        total_size_int = 0
        for offset_int_tuple in watermarks_dict.values():
            partition_size_int = offset_int_tuple[1] - offset_int_tuple[0]               
            total_size_int += partition_size_int
        #
        return size_dict, total_size_int

    def watermarks(self, topic_str):
        config_dict = self.config_dict
        config_dict["group.id"] = "dummy_group_id"
        consumer = get_consumer(config_dict)
        #
        partitions_int = self.partitions(topic_str)
        offset_int_tuple_dict = {partition_int: consumer.get_watermark_offsets(TopicPartition(topic_str, partition=partition_int)) for partition_int in range(partitions_int)}
        return offset_int_tuple_dict
    
    # AdminClient - groups

    def groups(self):
        groupMetadata_list = self.adminClient.list_groups()
        group_str_list = [groupMetadata.id for groupMetadata in groupMetadata_list]
        group_str_list.sort()
        return group_str_list

    def describe_group(self, group_str):
        groupMetadata_list = self.adminClient.list_groups(group=group_str)
        group_dict = {}
        if groupMetadata_list:            
            groupMetadata = groupMetadata_list[0]
            group_dict = groupMetadata_to_group_dict(groupMetadata)
        return group_dict

    # AdminClient - brokers

    def brokers(self):
        broker_dict = {broker_int: brokerMetadata.host + ":" + str(brokerMetadata.port) for broker_int, brokerMetadata in self.adminClient.list_topics().brokers.items()}
        return broker_dict

    def broker_config(self, broker_int):
        return self.get_config_dict(ResourceType.BROKER, str(broker_int))

    def set_broker_config(self, broker_int, key_str, value_str, test=False):
        self.set_config_dict(ResourceType.BROKER, str(broker_int), {key_str: value_str}, test)

    # AdminClient - ACLs

    def acls(self, restype="any", name=None, resource_pattern_type="any", principal=None, host=None, operation="any", permission_type="any"):
        resourceType = str_to_resourceType(restype)
        name_str = name
        resourcePatternType = str_to_resourcePatternType(resource_pattern_type)
        principal_str = principal
        host_str = host
        aclOperation = str_to_aclOperation(operation)
        aclPermissionType = str_to_aclPermissionType(permission_type)
        #
        aclBindingFilter = AclBindingFilter(resourceType, name_str, resourcePatternType, principal_str, host_str, aclOperation, aclPermissionType)
        aclBinding_list = self.adminClient.describe_acls(aclBindingFilter).result()
        return [aclBinding_to_dict(aclBinding) for aclBinding in aclBinding_list]

    def create_acl(self, restype="any", name=None, resource_pattern_type="any", principal=None, host=None, operation="any", permission_type="any"):
        resourceType = str_to_resourceType(restype)
        name_str = name
        resourcePatternType = str_to_resourcePatternType(resource_pattern_type)
        principal_str = principal
        host_str = host
        aclOperation = str_to_aclOperation(operation)
        aclPermissionType = str_to_aclPermissionType(permission_type)
        #
        aclBinding = AclBinding(resourceType, name_str, resourcePatternType, principal_str, host_str, aclOperation, aclPermissionType)
        self.adminClient.create_acls([aclBinding])[aclBinding].result()

    def delete_acl(self, restype=ResourceType.ANY, name=None, resource_pattern_type=ResourcePatternType.ANY, principal=None, host=None, operation=AclOperation.ANY, permission_type=AclPermissionType.ANY):
        resourceType = str_to_resourceType(restype)
        name_str = name
        resourcePatternType = str_to_resourcePatternType(resource_pattern_type)
        principal_str = principal
        host_str = host
        aclOperation = str_to_aclOperation(operation)
        aclPermissionType = str_to_aclPermissionType(permission_type)
        #
        aclBindingFilter = AclBindingFilter(resourceType, name_str, resourcePatternType, principal_str, host_str, aclOperation, aclPermissionType)
        aclBinding_list = self.adminClient.delete_acls([aclBindingFilter])[aclBindingFilter].result()
        return [aclBinding_to_dict(aclBinding) for aclBinding in aclBinding_list]

    # Producer

    def produce(self, topic_str, value, key=None, key_type="str", value_type="str", key_schema=None, value_schema=None):
        key_type_str = key_type
        value_type_str = value_type
        key_schema_str = key_schema
        value_schema_str = value_schema
        #
        def payload_to_payload_dict(payload):
            if isinstance(payload, str) or isinstance(payload, bytes):
                return json.loads(payload)
            else:
                return payload
        #
        def serialize(key_bool):
            type_str = key_type_str if key_bool else value_type_str
            schema_str = key_schema_str if key_bool else value_schema_str
            payload = key if key_bool else value
            messageField = MessageField.KEY if key_bool else MessageField.VALUE
            #
            if type_str in ["pb", "protobuf"]:
                generalizedProtocolMessageType = self.schema_str_to_generalizedProtocolMessageType(schema_str, topic_str, key_bool)
                protobufSerializer = ProtobufSerializer(generalizedProtocolMessageType, self.schemaRegistryClient, {"use.deprecated.format": False})
                payload_dict = payload_to_payload_dict(payload)
                protobuf_message = generalizedProtocolMessageType()
                ParseDict(payload_dict, protobuf_message)
                payload_bytes = protobufSerializer(protobuf_message, SerializationContext(topic_str, messageField)) 
            elif type_str == "avro":
                avroSerializer = AvroSerializer(self.schemaRegistryClient, schema_str)
                payload_dict = payload_to_payload_dict(payload)
                payload_bytes = avroSerializer(payload_dict, SerializationContext(topic_str, messageField))
            elif type_str in ["json", "jsonschema"]:
                jSONSerializer = JSONSerializer(schema_str, self.schemaRegistryClient)
                payload_dict = payload_to_payload_dict(payload)
                payload_bytes = jSONSerializer(payload_dict, SerializationContext(topic_str, messageField))
            else:
                payload_bytes = payload
            return payload_bytes
        #
        key_bytes = serialize(key_bool=True)
        value_bytes = serialize(key_bool=False)
        #
        self.producer.produce(topic_str, value_bytes, key_bytes)
        #
        self.produced_messages_int += 1
        if self.produced_messages_int % 10000 == 0:
            self.producer.flush(self.timeout_float)

    def upload(self, path_str, topic_str, key_type="str", value_type="str", key_schema=None, value_schema=None, key_value_separator=None, message_separator="\n"):  
        key_value_separator_str = key_value_separator
        message_separator_str = message_separator
        #
        def proc(line_str):
            line_str1 = line_str.strip()
            if key_value_separator_str != None:
                split_str_list = line_str1.split(key_value_separator_str)
                if len(split_str_list) == 2:
                    key_str = split_str_list[0]
                    value_str = split_str_list[1]
                else:
                    key_str = None
                    value_str = line_str1
            else:
                key_str = None
                value_str = line_str1
            #
            self.produce(topic_str, value_str, key=key_str, key_type=key_type, value_type=value_type, key_schema=key_schema, value_schema=value_schema)
        #
        foreach_line(path_str, proc, delimiter=message_separator_str)
        self.flush()

    def flush(self):
        self.producer.flush(self.timeout_float)

    # Consumer

    def subscribe(self, topic_str, group=None, offsets=None, config={}, key_type="str", value_type="str"):
        offsets_dict = offsets
        config_dict = config
        #
        if group == None:
            group_str = create_unique_group_id()
        else:
            group_str = group
        #
        self.config_dict["group.id"] = group_str
        self.config_dict["auto.offset.reset"] = self.auto_offset_reset_str
        self.config_dict["enable.auto.commit"] = self.auto_commit_bool
        self.config_dict["session.timeout.ms"] = self.session_timeout_ms_int
        for key_str, value in config_dict.items():
            self.config_dict[key_str] = value
        self.consumer = get_consumer(self.config_dict)
        #
        clusterMetaData = self.consumer.list_topics(topic=topic_str)
        self.topicPartition_list = [TopicPartition(topic_str, partition_int) for partition_int in clusterMetaData.topics[topic_str].partitions.keys()]
        #
        def on_assign(consumer, partitions):
            topicPartition_list = partitions
            #
            if offsets_dict != None:
                for index_int, offset_int in offsets_dict.items():
                    topicPartition_list[index_int].offset = offset_int
                consumer.assign(topicPartition_list)
        self.consumer.subscribe([topic_str], on_assign=on_assign)
        self.subscribed_topic_str = topic_str
        self.subscribed_key_type_str = key_type
        self.subscribed_value_type_str = value_type

    def unsubscribe(self):
        self.consumer.unsubscribe()
        self.subscribed_topic_str = None
        self.subscribed_key_type_str = None
        self.subscribed_value_type_str = None

    def consume(self, n=1):
        if self.subscribed_topic_str == None:
            print("Please subscribe before you consume.")
            return
        #
        num_messages_int = n
        #
        message_list = self.consumer.consume(num_messages_int, self.timeout_float)
        message_dict_list = [self.message_to_message_dict(message, key_type=self.subscribed_key_type_str, value_type=self.subscribed_value_type_str) for message in message_list]
        #
        return message_dict_list

    def commit(self):
        self.consumer.commit(self.last_consumed_message)

    def offsets(self):
        topicPartition_list = self.consumer.committed(self.topicPartition_list, timeout=1.0)
        if self.subscribed_topic_str:
            offsets_dict = {topicPartition.partition: offset_int_to_int_or_str(topicPartition.offset) for topicPartition in topicPartition_list if topicPartition.topic == self.subscribed_topic_str}
            return offsets_dict

    def foreach(self, topic_str, group=None, foreach=print, key_type="str", value_type="str", n=ALL_MESSAGES):
        group_str = group
        foreach_function = foreach
        key_type_str = key_type
        value_type_str = value_type
        num_messages_int = n
        #
        self.subscribe(topic_str, group=group_str, key_type=key_type_str, value_type=value_type_str)
        message_counter_int = 0
        while True:
            message_dict_list = self.consume()
            if not message_dict_list:
                break
            foreach_function(message_dict_list[0])
            if num_messages_int != ALL_MESSAGES:
                message_counter_int += 1
                if message_counter_int >= num_messages_int:
                    break
        self.unsubscribe()

    # Shell alias
    def cat(self, topic_str, group=None, foreach=print, key_type="str", value_type="str", n=ALL_MESSAGES):
        self.foreach(topic_str, group, foreach, key_type, value_type, n)

    def download(self, topic_str, path_str, group=None, key_type="str", value_type="str", key_value_separator=None, message_separator="\n", overwrite=True):
        group_str = group
        key_type_str = key_type
        value_type_str = value_type
        key_value_separator_str = key_value_separator
        message_separator_str = message_separator
        overwrite_bool = overwrite
        #
        mode_str = "w" if overwrite_bool else "a"
        #
        self.subscribe(topic_str, group=group_str, key_type=key_type_str, value_type=value_type_str)
        with open(path_str, mode_str) as textIOWrapper:
            while True:
                message_dict_list = self.consume()
                output_str_list = []
                if not message_dict_list:
                    break
                for message_dict in message_dict_list:
                    value = message_dict["value"]
                    if key_value_separator_str == None:
                        output = value
                    else:
                        key = message_dict["key"]
                        output = f"{key}{key_value_separator_str}{value}"
                    output_str_list += [f"{output}{message_separator_str}"]
                textIOWrapper.writelines(output_str_list)
        self.unsubscribe()

    # Shell alias for upload and download
    def cp(self, source_str, target_str, group=None, key_type="str", value_type="str", key_schema=None, value_schema=None, key_value_separator=None, message_separator="\n", overwrite=True, ):
        if is_file(source_str) and not is_file(target_str):
            self.upload(source_str, target_str, key_type=key_type, value_type=value_type, key_schema=key_schema, value_schema=value_schema, key_value_separator=key_value_separator, message_separator=message_separator)
        elif not is_file(source_str) and is_file(target_str):
            self.download(source_str, target_str, group=group, key_type=key_type, value_type=value_type, key_value_separator=key_value_separator, message_separator=message_separator, overwrite=overwrite)
        elif not is_file(source_str) and not is_file(target_str):
            print("Please prefix files with \"./\"; use the global replicate()/cp() function to copy topics.")
        elif is_file(source_str) and is_file(target_str):
            print("Please use your shell or file manager to copy files.")
