from confluent_kafka import Consumer, OFFSET_BEGINNING, OFFSET_END, OFFSET_INVALID, OFFSET_STORED, Producer, TIMESTAMP_CREATE_TIME, TopicPartition
from confluent_kafka.admin import AclBinding, AclBindingFilter, AclOperation, AclPermissionType, AdminClient, ConfigResource, NewPartitions, NewTopic, ResourceType, ResourcePatternType
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer, JSONSerializer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer, ProtobufSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from google.protobuf.json_format import MessageToDict, ParseDict
import configparser
from fnmatch import fnmatch
import importlib
import json
import os
import requests
import sys
import tempfile
import time

# Constants

ALL_MESSAGES = -1
RD_KAFKA_PARTITION_UA = -1

# Helpers


def is_file(str):
    return "/" in str


def get_millis():
    return int(time.time()*1000)


def create_unique_group_id():
    return str(get_millis())


def foreach_line(path_str, proc_function, delimiter='\n', bufsize=4096, n=ALL_MESSAGES):
    delimiter_str = delimiter
    bufsize_int = bufsize
    num_lines_int = n
    #
    buf_str = ""
    #
    line_counter_int = 0
    #
    with open(path_str) as textIOWrapper:
        while True:
            if num_lines_int != ALL_MESSAGES:
                if line_counter_int >= num_lines_int:
                    break
            newbuf_str = textIOWrapper.read(bufsize_int)
            if not newbuf_str:
                if buf_str:
                    proc_function(buf_str)
                    line_counter_int += 1
                break
            buf_str += newbuf_str
            line_str_list = buf_str.split(delimiter_str)
            for line_str in line_str_list[:-1]:
                proc_function(line_str)
                line_counter_int += 1
            buf_str = line_str_list[-1]


# Get cluster configurations

def get_config_dict(cluster_str):
    rawConfigParser = configparser.RawConfigParser()
    home_str = os.environ.get("KASHPY_HOME")
    if not home_str:
        home_str = "."
    if os.path.exists(f"{home_str}/clusters_secured/{cluster_str}.conf"):
        rawConfigParser.read(f"{home_str}/clusters_secured/{cluster_str}.conf")
        cluster_dir_str = "clusters_secured"
    elif os.path.exists(f"{home_str}/clusters_unsecured/{cluster_str}.conf"):
        rawConfigParser.read(f"{home_str}/clusters_unsecured/{cluster_str}.conf")
        cluster_dir_str = "clusters_unsecured"
    else:
        raise Exception(f"No cluster configuration file \"{cluster_str}.conf\" found in \"clusters_secured\" and \"clusters_unsecured\" (from: {home_str}; use KASHPY_HOME environment variable to set kash.py home directory).")
    #
    config_dict = dict(rawConfigParser.items("kafka"))
    #
    if "schema_registry" in rawConfigParser.sections():
        schema_registry_config_dict = dict(rawConfigParser.items("schema_registry"))
    else:
        schema_registry_config_dict = {}
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

def replicate(source_cluster, source_topic_str, target_cluster, target_topic_str, group=None, offsets=None, transform=None, source_key_type="bytes", source_value_type="bytes", target_key_type=None, target_value_type=None, target_key_schema=None, target_value_schema=None, keep_timestamps=True, n=ALL_MESSAGES, batch_size=1, verbose=1, progress_num_messages=1000):
    """Replicate a topic.

    Replicate (parts of) a topic (source_topic_str) on one cluster (source_cluster) to another topic (target_topic_str) on another (or the same) cluster (target_cluster). Each replicated message can be transformed into another message using a single message transform (transform). The source and target topics can have different message key and value types; e.g. the source topic can have value type Avro whereas the target topic will be written with value type Protobuf.

    Args:
        arg1 (str): Description of arg1
        arg2 (int): Description of arg2

    Returns:
        bool: Description of return value
    """
    group_str = group
    offsets_dict = offsets
    transform_function = transform
    source_key_type_str = source_key_type
    source_value_type_str = source_value_type
    target_key_type_str = target_key_type
    target_value_type_str = target_value_type
    target_key_schema_str = target_key_schema
    target_value_schema_str = target_value_schema
    keep_timestamps_bool = keep_timestamps
    num_messages_int = n
    batch_size_int = batch_size
    verbose_int = verbose
    progress_num_messages_int = progress_num_messages
    #
    source_cluster.subscribe(source_topic_str, group=group_str, offsets=offsets_dict, key_type=source_key_type_str, value_type=source_value_type_str)
    message_counter_int = 0
    while True:
        message_dict_list = source_cluster.consume(n=batch_size_int)
        if not message_dict_list:
            break
        for message_dict in message_dict_list:
            if transform_function:
                message_dict = transform_function(message_dict)
            #
            if keep_timestamps_bool:
                timestamp_int_int_tuple = message_dict["timestamp"]
                if timestamp_int_int_tuple[0] == TIMESTAMP_CREATE_TIME:
                    timestamp_int = timestamp_int_int_tuple[1]
            else:
                timestamp_int = 0
            #
            key_type_str = target_key_type_str if target_key_type_str else source_key_type_str
            value_type_str = target_value_type_str if target_value_type_str else source_value_type_str
            key_schema_str = target_key_schema_str if target_key_schema_str else source_cluster.last_consumed_message_key_schema_str
            value_schema_str = target_value_schema_str if target_value_schema_str else source_cluster.last_consumed_message_value_schema_str
            #
            target_cluster.produce(target_topic_str, message_dict["value"], message_dict["key"], key_type=key_type_str, value_type=value_type_str, key_schema=key_schema_str, value_schema=value_schema_str, partition=message_dict["partition"], timestamp=timestamp_int, headers=message_dict["headers"])
        #
        message_counter_int += len(message_dict_list)
        if verbose_int > 0 and message_counter_int % progress_num_messages_int == 0:
            print(message_counter_int)
        if num_messages_int != ALL_MESSAGES:
            if message_counter_int >= num_messages_int:
                break
    #
    target_cluster.flush()
    source_cluster.unsubscribe()
    return message_counter_int

# Shell alias
cp = replicate

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
        self.last_consumed_message_key_schema_str = None
        self.last_consumed_message_value_schema_str = None
        #
        self.schema_id_int_generalizedProtocolMessageType_protobuf_schema_str_tuple_dict = {}
        self.schema_id_int_avro_schema_str_dict = {}
        self.schema_id_int_jsonschema_str_dict = {}
        # Producer
        self.flush_num_messages_int = 10000
        self.flush_timeout_float = 2.0
        # Consumer
        self.consume_timeout_float = 1.0
        # auto.offset.reset (earliest or latest (confluent_kafka default: latest))
        self.auto_offset_reset_str = "earliest"
        # enable.auto.commit (True or False (confluent_kafka default: True))
        self.auto_commit_bool = True
        # session.timeout.ms (6000-300000 (confluent_kafka default: 30000))
        self.session_timeout_ms_int = 10000
        # Standard output
        self.verbose_int = 1
        self.progress_num_messages_int = 1000

    def flush_num_messages(self):
        return self.flush_num_messages_int

    def set_flush_num_messages(self, flush_num_messages_int):
        self.flush_num_messages_int = flush_num_messages_int

    #

    def flush_timeout(self):
        return self.flush_timeout_float

    def set_flush_timeout(self, flush_timeout_float):
        self.flush_timeout_float = flush_timeout_float

    #

    def consume_timeout(self):
        return self.consume_timeout_float

    def set_consume_timeout(self, consume_timeout_float):
        self.consume_timeout_float = consume_timeout_float

    #

    def auto_offset_reset(self):
        return self.auto_offset_reset_str

    def set_auto_offset_reset(self, auto_offset_reset_str):
        self.auto_offset_reset_str = auto_offset_reset_str

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

    def verbose(self):
        return self.verbose_int

    def set_verbose(self, verbose_int):
        self.verbose_int = verbose_int

    #

    def progress_num_messages(self):
        return self.progress_num_messages_int

    def set_progress_num_messages(self, progress_num_messages_int):
        self.progress_num_messages_int = progress_num_messages_int

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

    def schema_id_int_to_generalizedProtocolMessageType_protobuf_schema_str_tuple(self, schema_id_int):
        schema = self.schemaRegistryClient.get_schema(schema_id_int)
        schema_str = schema.schema_str
        #
        generalizedProtocolMessageType = self.schema_id_int_and_schema_str_to_generalizedProtocolMessageType(schema_id_int, schema_str)
        return generalizedProtocolMessageType, schema_str

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
        path_str = f"/{tempfile.gettempdir()}/kash.py/{self.cluster_dir_str}/{self.cluster_str}"
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

    def bytes_protobuf_to_dict(self, bytes, key_bool):
        schema_id_int = int.from_bytes(bytes[1:5], "big")
        if schema_id_int in self.schema_id_int_generalizedProtocolMessageType_protobuf_schema_str_tuple_dict:
            generalizedProtocolMessageType, protobuf_schema_str = self.schema_id_int_generalizedProtocolMessageType_protobuf_schema_str_tuple_dict[schema_id_int]
        else:
            generalizedProtocolMessageType, protobuf_schema_str = self.schema_id_int_to_generalizedProtocolMessageType_protobuf_schema_str_tuple(schema_id_int)
            self.schema_id_int_generalizedProtocolMessageType_protobuf_schema_str_tuple_dict[schema_id_int] = (generalizedProtocolMessageType, protobuf_schema_str)
        #
        if key_bool:
            self.last_consumed_message_key_schema_str = protobuf_schema_str
        else:
            self.last_consumed_message_value_schema_str = protobuf_schema_str
        #
        protobufDeserializer = ProtobufDeserializer(generalizedProtocolMessageType, {"use.deprecated.format": False})
        protobuf_message = protobufDeserializer(bytes, None)
        dict = MessageToDict(protobuf_message)
        return dict

    def bytes_avro_to_dict(self, bytes, key_bool):
        schema_id_int = int.from_bytes(bytes[1:5], "big")
        if schema_id_int in self.schema_id_int_avro_schema_str_dict:
            avro_schema_str = self.schema_id_int_avro_schema_str_dict[schema_id_int]
        else:
            avro_schema_str = self.schema_id_int_to_avro_schema_str(schema_id_int)
            self.schema_id_int_avro_schema_str_dict[schema_id_int] = avro_schema_str
        #
        if key_bool:
            self.last_consumed_message_key_schema_str = avro_schema_str
        else:
            self.last_consumed_message_value_schema_str = avro_schema_str
        #
        avroDeserializer = AvroDeserializer(self.schemaRegistryClient, avro_schema_str)
        dict = avroDeserializer(bytes, None)
        return dict

    def bytes_jsonschema_to_dict(self, bytes, key_bool):
        schema_id_int = int.from_bytes(bytes[1:5], "big")
        if schema_id_int in self.schema_id_int_jsonschema_str_dict:
            jsonschema_str = self.schema_id_int_jsonschema_str_dict[schema_id_int]
        else:
            jsonschema_str = self.schema_id_int_to_jsonschema_str(schema_id_int)
            self.schema_id_int_jsonschema_str_dict[schema_id_int] = jsonschema_str
        #
        if key_bool:
            self.last_consumed_message_key_schema_str = jsonschema_str
        else:
            self.last_consumed_message_value_schema_str = jsonschema_str
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
        elif key_type_str == "json":
            decode_key = json.loads
        elif key_type_str in ["pb", "protobuf"]:
            def decode_key(bytes):
                return self.bytes_protobuf_to_dict(bytes, key_bool=True)
        elif key_type_str == "avro":
            def decode_key(bytes):
                return self.bytes_avro_to_dict(bytes, key_bool=True)
        elif key_type_str == "jsonschema":
            def decode_key(bytes):
                return self.bytes_jsonschema_to_dict(bytes, key_bool=True)
        #
        if value_type_str == "str":
            decode_value = bytes_to_str
        elif value_type_str == "bytes":
            decode_value = bytes_to_bytes
        elif value_type_str == "json":
            decode_value = json.loads
        elif value_type_str in ["pb", "protobuf"]:
            def decode_value(bytes):
                return self.bytes_protobuf_to_dict(bytes, key_bool=False)
        elif value_type_str == "avro":
            def decode_value(bytes):
                return self.bytes_avro_to_dict(bytes, key_bool=False)
        elif value_type_str == "jsonschema":
            def decode_value(bytes):
                return self.bytes_jsonschema_to_dict(bytes, key_bool=False)
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

    def topics(self, pattern=None, size=False):
        size_bool = size
        pattern_str = pattern
        #
        if size_bool:
            topic_str_size_dict_total_size_int_tuple_dict = self.size(pattern_str)
            topic_str_size_int_dict = {topic_str: topic_str_size_dict_total_size_int_tuple_dict[topic_str][1] for topic_str in topic_str_size_dict_total_size_int_tuple_dict}
            return topic_str_size_int_dict
        else:
            topic_str_list = list(self.adminClient.list_topics().topics.keys())
            if pattern_str is not None:
                topic_str_list = [topic_str for topic_str in topic_str_list if fnmatch(topic_str, pattern_str)]
            topic_str_list.sort()
            return topic_str_list

    # Shell alias
    ls = topics

    def l(self, pattern=None, size=True):
        return self.topics(pattern=pattern, size=size)

    # Shell alias
    ll = l

    def config(self, pattern_str):
        topic_str_list = self.topics(pattern_str)
        #
        topic_str_config_dict_dict = {topic_str: self.get_config_dict(ResourceType.TOPIC, topic_str) for topic_str in topic_str_list}
        #
        return topic_str_config_dict_dict

    def set_config(self, pattern_str, key_str, value_str, test=False):
        test_bool = test
        #
        topic_str_list = self.topics(pattern_str)
        #
        for topic_str in topic_str_list:
            self.set_config_dict(ResourceType.TOPIC, topic_str, {key_str: value_str}, test_bool)
        #
        topic_str_key_str_value_str_tuple_dict = {topic_str: (key_str, value_str) for topic_str in topic_str_list}
        return topic_str_key_str_value_str_tuple_dict
        
    def create(self, topic_str, partitions=1, retention_ms=-1):
        partitions_int = partitions
        retention_ms_int = retention_ms
        #
        newTopic = NewTopic(topic_str, partitions_int, config={"retention.ms": retention_ms_int})
        self.adminClient.create_topics([newTopic])
        #
        return topic_str

    # Shell aliases
    mk = create

    def delete(self, pattern_str):
        topic_str_list = self.topics(pattern_str)
        if topic_str_list:
            self.adminClient.delete_topics(topic_str_list)
            return topic_str_list
        else:
            return []

    # Shell alias
    rm = delete

    def offsets_for_times(self, topic_str, partition_int_timestamp_int_dict, timeout=-1):
        partition_int_offset_int_dict = {}
        timeout_float = timeout
        #
        topicPartition_list = [TopicPartition(topic_str, partition_int, timestamp_int) for partition_int, timestamp_int in partition_int_timestamp_int_dict.items()]
        if topicPartition_list:
            config_dict = self.config_dict
            config_dict["group.id"] = "dummy_group_id"
            consumer = get_consumer(config_dict)
            topicPartition_list1 = consumer.offsets_for_times(topicPartition_list, timeout=timeout_float)
            #
            for topicPartition in topicPartition_list1:
                partition_int_offset_int_dict[topicPartition.partition] = topicPartition.offset
        #
        return partition_int_offset_int_dict

    def describe(self, pattern_str):
        topic_str_topicMetadata_dict = self.adminClient.list_topics().topics
        #
        topic_str_topic_dict_dict = {topic_str: topicMetadata_to_topic_dict(topic_str_topicMetadata_dict[topic_str]) for topic_str in topic_str_topicMetadata_dict if fnmatch(topic_str, pattern_str)}
        #
        return topic_str_topic_dict_dict

    def exists(self, topic_str):
        if self.topics(topic_str):
            return True
        else:
            return False

    def partitions(self, pattern_str):
        topic_str_topicMetadata_dict = self.adminClient.list_topics().topics
        #
        topic_str_num_partitions_int_dict = {topic_str: len(topic_str_topicMetadata_dict[topic_str].partitions) for topic_str in topic_str_topicMetadata_dict if fnmatch(topic_str, pattern_str)}
        #
        return topic_str_num_partitions_int_dict

    def set_partitions(self, pattern_str, num_partitions_int, test=False):
        test_bool = test
        #
        topic_str_list = self.topics(pattern_str)
        #
        newPartitions_list = [NewPartitions(topic_str, num_partitions_int) for topic_str in topic_str_list]
        topic_str_future_dict = self.adminClient.create_partitions(newPartitions_list, validate_only=test_bool)
        #
        for future in topic_str_future_dict.values():
            future.result()
        #
        topic_str_num_partitions_int_dict = {topic_str: num_partitions_int for topic_str in topic_str_list}
        return topic_str_num_partitions_int_dict

    def size(self, pattern_str):
        topic_str_partition_int_tuple_dict_dict = self.watermarks(pattern_str)
        #
        topic_str_size_dict_total_size_int_tuple_dict = {}
        for topic_str, partition_int_tuple_dict in topic_str_partition_int_tuple_dict_dict.items():
            size_dict = {partition_int: partition_int_tuple_dict[partition_int][1]-partition_int_tuple_dict[partition_int][0] for partition_int in partition_int_tuple_dict.keys()}
            #
            total_size_int = 0
            for offset_int_tuple in partition_int_tuple_dict.values():
                partition_size_int = offset_int_tuple[1] - offset_int_tuple[0]
                total_size_int += partition_size_int
            #
            topic_str_size_dict_total_size_int_tuple_dict[topic_str] = (size_dict, total_size_int)
        return topic_str_size_dict_total_size_int_tuple_dict

    def watermarks(self, pattern_str, timeout=-1):
        timeout_float = timeout
        #
        config_dict = self.config_dict
        config_dict["group.id"] = "dummy_group_id"
        consumer = get_consumer(config_dict)
        #
        topic_str_list = self.topics(pattern_str)
        topic_str_partition_int_tuple_dict_dict = {}
        for topic_str in topic_str_list:
            partitions_int = self.partitions(topic_str)[topic_str]
            partition_int_tuple_dict = {partition_int: consumer.get_watermark_offsets(TopicPartition(topic_str, partition=partition_int), timeout_float) for partition_int in range(partitions_int)}
            topic_str_partition_int_tuple_dict_dict[topic_str] = partition_int_tuple_dict
        return topic_str_partition_int_tuple_dict_dict

    # AdminClient - groups

    def groups(self):
        groupMetadata_list = self.adminClient.list_groups()
        group_str_list = [groupMetadata.id for groupMetadata in groupMetadata_list]
        group_str_list.sort()
        return group_str_list

    def describe_groups(self, pattern_str):
        groupMetadata_list = self.adminClient.list_groups()
        #
        group_str_group_dict_dict = {groupMetadata.id: groupMetadata_to_group_dict(groupMetadata) for groupMetadata in groupMetadata_list if fnmatch(groupMetadata.id, pattern_str)}
        #
        return group_str_group_dict_dict

    # AdminClient - brokers

    def brokers(self, pattern=None):
        pattern_int_or_str = pattern
        #
        if pattern_int_or_str is None:
            pattern_int_or_str = "*"
        else:
            pattern_int_or_str = str(pattern_int_or_str)
        #
        broker_dict = {broker_int: brokerMetadata.host + ":" + str(brokerMetadata.port) for broker_int, brokerMetadata in self.adminClient.list_topics().brokers.items() if fnmatch(str(broker_int), pattern_int_or_str)}
        #
        return broker_dict

    def broker_config(self, pattern_int_or_str):
        broker_dict = self.brokers(pattern=pattern_int_or_str)
        #
        broker_int_broker_config_dict = {broker_int: self.get_config_dict(ResourceType.BROKER, str(broker_int)) for broker_int in broker_dict}
        #
        return broker_int_broker_config_dict

    def set_broker_config(self, pattern_int_or_str, key_str, value_str, test=False):
        test_bool = test
        #
        broker_dict = self.brokers(pattern=pattern_int_or_str)
        #
        for broker_int in broker_dict:
            self.set_config_dict(ResourceType.BROKER, str(broker_int), {key_str: value_str}, test_bool)
        #
        broker_int_key_str_value_str_tuple_dict = {broker_int: (key_str, value_str) for broker_int in broker_dict}
        return broker_int_key_str_value_str_tuple_dict

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
        #
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
        #
        return aclBinding_to_dict(aclBinding)

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
        #
        return [aclBinding_to_dict(aclBinding) for aclBinding in aclBinding_list]

    # Producer

    def produce(self, topic_str, value, key=None, key_type="str", value_type="str", key_schema=None, value_schema=None, partition=RD_KAFKA_PARTITION_UA, timestamp=0, headers=None):
        key_type_str = key_type
        value_type_str = value_type
        key_schema_str = key_schema
        value_schema_str = value_schema
        partition_int = partition
        timestamp_int = timestamp
        headers_dict_or_list = headers
        #

        def serialize(key_bool):
            type_str = key_type_str if key_bool else value_type_str
            schema_str = key_schema_str if key_bool else value_schema_str
            payload = key if key_bool else value
            messageField = MessageField.KEY if key_bool else MessageField.VALUE
            #

            def payload_to_payload_dict(payload):
                if isinstance(payload, str) or isinstance(payload, bytes):
                    payload_dict = json.loads(payload)
                else:
                    payload_dict = payload
                return payload_dict
            #
            if type_str == "json":
                if isinstance(payload, dict):
                    payload_str_or_bytes = json.dumps(payload)
                else:
                    payload_str_or_bytes = payload
            elif type_str in ["pb", "protobuf"]:
                generalizedProtocolMessageType = self.schema_str_to_generalizedProtocolMessageType(schema_str, topic_str, key_bool)
                protobufSerializer = ProtobufSerializer(generalizedProtocolMessageType, self.schemaRegistryClient, {"use.deprecated.format": False})
                payload_dict = payload_to_payload_dict(payload)
                protobuf_message = generalizedProtocolMessageType()
                ParseDict(payload_dict, protobuf_message)
                payload_str_or_bytes = protobufSerializer(protobuf_message, SerializationContext(topic_str, messageField))
            elif type_str == "avro":
                avroSerializer = AvroSerializer(self.schemaRegistryClient, schema_str)
                payload_dict = payload_to_payload_dict(payload)
                payload_str_or_bytes = avroSerializer(payload_dict, SerializationContext(topic_str, messageField))
            elif type_str == "jsonschema":
                jSONSerializer = JSONSerializer(schema_str, self.schemaRegistryClient)
                payload_dict = payload_to_payload_dict(payload)
                payload_str_or_bytes = jSONSerializer(payload_dict, SerializationContext(topic_str, messageField))
            else:
                payload_str_or_bytes = payload
            return payload_str_or_bytes
        #
        key_str_or_bytes = serialize(key_bool=True)
        value_str_or_bytes = serialize(key_bool=False)
        #
        self.producer.produce(topic_str, value_str_or_bytes, key_str_or_bytes, partition=partition_int, timestamp=timestamp_int, headers=headers_dict_or_list)
        #
        self.produced_messages_int += 1
        if self.produced_messages_int % self.flush_num_messages_int == 0:
            self.producer.flush(self.flush_timeout_float)
        #
        return key_str_or_bytes, value_str_or_bytes 

    def upload(self, path_str, topic_str, key_type="str", value_type="str", key_schema=None, value_schema=None, key_value_separator=None, message_separator="\n", n=ALL_MESSAGES):
        key_value_separator_str = key_value_separator
        message_separator_str = message_separator
        num_messages_int = n
        #

        def proc(line_str):
            line_str1 = line_str.strip()
            if line_str1:
                if key_value_separator_str is not None:
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
                if self.verbose_int > 0 and self.produced_messages_int % self.progress_num_messages_int == 0:
                    print(self.produced_messages_int)
        #
        self.produced_messages_int = 0
        #
        foreach_line(path_str, proc, delimiter=message_separator_str, n=num_messages_int)
        self.flush()
        #
        return self.produced_messages_int

    def flush(self):
        self.producer.flush(self.flush_timeout_float)

    # Consumer

    def subscribe(self, topic_str, group=None, offsets=None, config={}, key_type="str", value_type="str"):
        offsets_dict = offsets
        config_dict = config
        #
        if group is None:
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
            if offsets_dict is not None:
                for index_int, offset_int in offsets_dict.items():
                    topicPartition_list[index_int].offset = offset_int
                consumer.assign(topicPartition_list)
        self.consumer.subscribe([topic_str], on_assign=on_assign)
        self.subscribed_topic_str = topic_str
        self.subscribed_key_type_str = key_type
        self.subscribed_value_type_str = value_type
        #
        return topic_str, group_str

    def unsubscribe(self):
        self.consumer.unsubscribe()
        self.subscribed_topic_str = None
        self.subscribed_key_type_str = None
        self.subscribed_value_type_str = None

    def consume(self, n=1):
        num_messages_int = n
        #
        if self.subscribed_topic_str is None:
            print("Please subscribe to a topic before you consume.")
            return
        #
        message_list = self.consumer.consume(num_messages_int, self.consume_timeout_float)
        if message_list:
            self.last_consumed_message = message_list[-1]
        message_dict_list = [self.message_to_message_dict(message, key_type=self.subscribed_key_type_str, value_type=self.subscribed_value_type_str) for message in message_list]
        #
        return message_dict_list

    def commit(self):
        self.consumer.commit(self.last_consumed_message)
        #
        return self.last_consumed_message

    def offsets(self, timeout=-1):
        timeout_float = timeout
        #
        topicPartition_list = self.consumer.committed(self.topicPartition_list, timeout=timeout_float)
        if self.subscribed_topic_str:
            offsets_dict = {topicPartition.partition: offset_int_to_int_or_str(topicPartition.offset) for topicPartition in topicPartition_list if topicPartition.topic == self.subscribed_topic_str}
            return offsets_dict
        else:
            return {}

    def fold(self, topic_str, fold_function, group=None, offsets=None, key_type="str", value_type="str", n=ALL_MESSAGES, batch_size=1):
        group_str = group
        offsets_dict = offsets
        key_type_str = key_type
        value_type_str = value_type
        num_messages_int = n
        batch_size_int = batch_size
        #
        acc_list = []
        self.subscribe(topic_str, group=group_str, offsets=offsets_dict, key_type=key_type_str, value_type=value_type_str)
        message_counter_int = 0
        while True:
            message_dict_list = self.consume(n=batch_size_int)
            if not message_dict_list:
                break
            acc_list1 = []
            [acc_list1 := acc_list1 + fold_function(message_dict) for message_dict in message_dict_list]
            acc_list += acc_list1
            message_counter_int += len(message_dict_list)
            if self.verbose_int > 0 and message_counter_int % self.progress_num_messages_int == 0:
                print(message_counter_int)
            if num_messages_int != ALL_MESSAGES:
                if message_counter_int >= num_messages_int:
                    break
        self.unsubscribe()
        return acc_list

    def foreach(self, topic_str, foreach=print, group=None, offsets=None, key_type="str", value_type="str", n=ALL_MESSAGES, batch_size=1):
        foreach_function = foreach
        group_str = group
        offsets_dict = offsets
        key_type_str = key_type
        value_type_str = value_type
        num_messages_int = n
        batch_size_int = batch_size
        #

        def fold_function(message_dict):
            foreach_function(message_dict)
            return []
        #
        self.fold(topic_str, fold_function, group=group_str, offsets=offsets_dict, key_type=key_type_str, value_type=value_type_str, n=num_messages_int, batch_size=batch_size_int)

    # Shell alias
    cat = foreach

    def grep(self, topic_str, match_function, group=None, offsets=None, key_type="str", value_type="str", n=ALL_MESSAGES, batch_size=1):
        group_str = group
        offsets_dict = offsets
        key_type_str = key_type
        value_type_str = value_type
        num_messages_int = n
        batch_size_int = batch_size
        #

        def fold_function(message_dict):
            if match_function(message_dict):
                if self.verbose_int > 0:
                    partition_int = message_dict["partition"]
                    offset_int = message_dict["offset"]
                    print(f"Found matching message on partition {partition_int} with offset {offset_int}.")
                return [message_dict]
            else:
                return []
        #
        return self.fold(topic_str, fold_function, group=group_str, offsets=offsets_dict, key_type=key_type_str, value_type=value_type_str, n=num_messages_int, batch_size=batch_size_int)

    def download(self, topic_str, path_str, group=None, offsets=None, key_type="str", value_type="str", key_value_separator=None, message_separator="\n", overwrite=True, n=ALL_MESSAGES, batch_size=1):
        group_str = group
        offsets_dict = offsets
        key_type_str = key_type
        value_type_str = value_type
        key_value_separator_str = key_value_separator
        message_separator_str = message_separator
        overwrite_bool = overwrite
        num_messages_int = n
        batch_size_int = batch_size
        #
        mode_str = "w" if overwrite_bool else "a"
        #
        self.subscribe(topic_str, group=group_str, offsets=offsets_dict, key_type=key_type_str, value_type=value_type_str)
        with open(path_str, mode_str) as textIOWrapper:
            message_counter_int = 0
            while True:
                message_dict_list = self.consume(n=batch_size_int)
                if not message_dict_list:
                    break
                output_str_list = []
                for message_dict in message_dict_list:
                    value = message_dict["value"]
                    if isinstance(value, dict):
                        value = json.dumps(value)
                    if key_value_separator_str is None:
                        output = value
                    else:
                        key = message_dict["key"]
                        if isinstance(key, dict):
                            key = json.dumps(key)
                        output = f"{key}{key_value_separator_str}{value}"
                    #
                    output_str = f"{output}{message_separator_str}"
                    output_str_list += [output_str]
                textIOWrapper.writelines(output_str_list)
                #
                message_counter_int += len(message_dict_list)
                if self.verbose_int > 0 and message_counter_int % self.progress_num_messages_int == 0:
                    print(message_counter_int)
                if num_messages_int != ALL_MESSAGES:
                    if message_counter_int >= num_messages_int:
                        break
        self.unsubscribe()
        #
        return message_counter_int

    # Shell alias for upload and download
    def cp(self, source_str, target_str, group=None, offsets=None, transform=None, source_key_type="str", source_value_type="str", target_key_type="str", target_value_type="str", target_key_schema=None, target_value_schema=None, key_value_separator=None, message_separator="\n", overwrite=True, keep_timestamps=True, n=ALL_MESSAGES, batch_size=1):
        if is_file(source_str) and not is_file(target_str):
            return self.upload(source_str, target_str, key_type=target_key_type, value_type=target_value_type, key_schema=target_key_schema, value_schema=target_value_schema, key_value_separator=key_value_separator, message_separator=message_separator, n=n)
        elif not is_file(source_str) and is_file(target_str):
            return self.download(source_str, target_str, group=group, offsets=offsets, key_type=source_key_type, value_type=source_value_type, key_value_separator=key_value_separator, message_separator=message_separator, overwrite=overwrite, n=n, batch_size=batch_size)
        elif (not is_file(source_str)) and (not is_file(target_str)):
            return replicate(self, source_str, self, target_str, group=group, offsets=offsets, transform=transform, source_key_type=source_key_type, source_value_type=source_value_type, target_key_type=target_key_type, target_value_type=target_value_type, target_key_schema=target_key_schema, target_value_schema=target_value_schema, keep_timestamps=keep_timestamps, n=n, batch_size=batch_size, verbose=self.verbose_int, progress_num_messages=self.progress_num_messages_int)
        elif is_file(source_str) and is_file(target_str):
            print("Please use your shell or file manager to copy files.")
