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
import re
import requests
import sys
import tempfile
import time

# Constants

ALL_MESSAGES = -1
RD_KAFKA_PARTITION_UA = -1

# Helpers


def str_to_bool(str):
    if str.lower() == "false":
        return False
    else:
        return True


def is_interactive():
    return hasattr(sys, 'ps1')


def is_file(str):
    return "/" in str


def get_millis():
    return int(time.time()*1000)


def create_unique_group_id():
    return str(get_millis())


def foldl_file(path_str, foldl_function, initial_acc, delimiter='\n', n=ALL_MESSAGES, bufsize=4096, verbose=0, progress_num_lines=1000):
    delimiter_str = delimiter
    bufsize_int = bufsize
    num_lines_int = n
    verbose_int = verbose
    progress_num_lines_int = progress_num_lines
    #
    buf_str = ""
    #
    line_counter_int = 0
    #
    acc = initial_acc
    with open(path_str) as textIOWrapper:
        while True:
            newbuf_str = textIOWrapper.read(bufsize_int)
            if not newbuf_str:
                if buf_str:
                    last_line_str = buf_str
                    line_counter_int += 1
                    if verbose_int > 0 and line_counter_int % progress_num_lines_int == 0:
                        print(f"Read: {line_counter_int}")
                    #
                    acc = foldl_function(acc, last_line_str)
                break
            buf_str += newbuf_str
            line_str_list = buf_str.split(delimiter_str)
            for line_str in line_str_list[:-1]:
                line_counter_int += 1
                if verbose_int > 0 and line_counter_int % progress_num_lines_int == 0:
                    print(f"Read: {line_counter_int}")
                #
                acc = foldl_function(acc, line_str)
                #
                if num_lines_int != ALL_MESSAGES:
                    if line_counter_int >= num_lines_int:
                        break
            #
            buf_str = line_str_list[-1]
    #
    return (acc, line_counter_int)


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
    if "kash" in rawConfigParser.sections():
        kash_dict = dict(rawConfigParser.items("kash"))
    else:
        kash_dict = {}
    #
    return config_dict, schema_registry_config_dict, kash_dict, cluster_dir_str


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
    dict = {}
    #
    dict["url"] = config_dict["schema.registry.url"]
    if "basic.auth.user.info" in config_dict:
        dict["basic.auth.user.info"] = config_dict["basic.auth.user.info"]
    #
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

def flatmap(source_cluster, source_topic_str, target_cluster, target_topic_str, flatmap_function, group=None, offsets=None, source_key_type="bytes", source_value_type="bytes", target_key_type=None, target_value_type=None, target_key_schema=None, target_value_schema=None, keep_timestamps=True, n=ALL_MESSAGES, batch_size=1):
    """Replicate a topic and transform the messages in a flatmap-like manner.

    Replicate (parts of) a topic (source_topic_str) on one cluster (source_cluster) to another topic (target_topic_str) on another (or the same) cluster (target_cluster). Each replicated message is transformed into a list of other messages in a flatmap-like manner. The source and target topics can have different message key and value types; e.g. the source topic can have value type Avro whereas the target topic will be written with value type Protobuf.

    Args:
        source_cluster (Cluster): Source cluster
        source_topic_str (str): Source topic
        target_cluster (Cluster): Target cluster
        target_topic_str (str): Target topic
        flatmap_function (function): Flatmap function (takes a message dictionary and returns a list of message dictionaries)
        group (str, optional): Consumer group name used for consuming from the source topic. If set to None, creates a new unique consumer group name. Defaults to None.
        offsets (dict, optional): Dictionary of offsets (keys: partitions (int), values: offsets for the partitions (int)) for consuming from the source topic. If set to None, consume the topic using the offsets from the consumer group for the topic. Defaults to None.
        source_key_type (str, optional): Source topic message key type ("bytes", "str", "json", "avro", "protobuf"/"pb" or "jsonschema"). Defaults to "bytes".
        source_value_type (str, optional): Source topic message value type ("bytes", "str", "json", "avro", "protobuf"/"pb" or "jsonschema"). Defaults to "bytes".
        target_key_type (str, optional): Target topic message key type ("bytes", "str", "json", "avro", "protobuf"/"pb" or "jsonschema"). If set to None, target_key_type = source_key_type. Defaults to None.
        target_value_type (str, optional): Target topic message value type ("bytes", "str", "json", "avro", "protobuf"/"pb" or "jsonschema"). If set to None, target_value_type = source_value_type. Defaults to None.
        target_key_schema (str, optional): Target key message type schema (for "avro", "protobuf"/"pb" or "jsonschema"). Defaults to None.
        target_value_schema (str, optional): Target value message type schema (for "avro", "protobuf"/"pb" or "jsonschema"). Defaults to None.
        keep_timestamps (bool, optional): Replicate the timestamps of the source messages in the target messages. Defaults to True.
        n (int, optional): Number of messages to consume from the source topic. Defaults to ALL_MESSAGES.
        batch_size (int, optional): Maximum number of messages to consume from the source topic at a time. Defaults to 1.

    Returns:
        tuple(int, int): Pair of the number of messages consumed from the source topic and the number of messages produced to the target topic.

    Examples:
        flatmap(cluster1, "topic1", cluster2, "topic2", lambda message_dict: [message_dict])
            Replicate "topic1" on cluster1 to "topic2" on cluster2.

        flatmap(cluster1, "topic1", cluster2, "topic2", lambda message_dict: [message_dict, message_dict])
            Replicate "topic1" on cluster1 to "topic2" on cluster2, while duplicating each message from topic1 in topic2.

        flatmap(cluster1, "topic1", cluster2, "topic2", lambda message_dict: [message_dict], source_value_type="avro", target_value_type="protobuf", target_value_schema="message Snack { required string name = 1; required float calories = 2; optional string colour = 3; }", n=100)
            Replicate the first 100 messages from "topic1" on cluster1 to "topic2" on cluster2, changing the value schema from avro to protobuf.
        
        flatmap(cluster1, "topic1", cluster2, "topic2", lambda message_dict: [message_dict], offsets={0:100}, keep_timestamps=False, n=500)
            Replicate the messages 100-600 from "topic1" on cluster1 to "topic2" on cluster2. Create new timestamps for the messages produced to the target topic.     
    """
    source_key_type_str = source_key_type
    source_value_type_str = source_value_type
    target_key_type_str = target_key_type
    target_value_type_str = target_value_type
    target_key_schema_str = target_key_schema
    target_value_schema_str = target_value_schema
    keep_timestamps_bool = keep_timestamps
    #
    if not target_cluster.exists(target_topic_str):
        source_num_partitions_int = source_cluster.partitions(source_topic_str)[source_topic_str]
        source_config_dict = source_cluster.config(source_topic_str)[source_topic_str]
        target_cluster.create(target_topic_str, partitions=source_num_partitions_int, config=source_config_dict)
    #
    target_cluster.produced_messages_counter_int = 0
    #
    def foreach_function(message_dict):
        message_dict_list = flatmap_function(message_dict)
        #
        for message_dict in message_dict_list:
            if keep_timestamps_bool:
                timestamp_int_int_tuple = message_dict["timestamp"]
                if timestamp_int_int_tuple[0] == TIMESTAMP_CREATE_TIME:
                    timestamp_int = timestamp_int_int_tuple[1]
                else:
                    timestamp_int = 0
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
            if target_cluster.verbose_int > 0 and target_cluster.produced_messages_counter_int % target_cluster.kash_dict["progress.num.messages"] == 0:
                print(f"Produced: {target_cluster.produced_messages_counter_int}")
            #
            if target_cluster.produced_messages_counter_int % target_cluster.kash_dict["flush.num.messages"] == 0:
                target_cluster.flush()
    #
    num_messages_int = source_cluster.foreach(source_topic_str, foreach_function, group=group, offsets=offsets, key_type=source_key_type_str, value_type=source_value_type_str, n=n, batch_size=batch_size)
    #
    target_cluster.flush()
    #
    return (num_messages_int, target_cluster.produced_messages_counter_int)

def map(source_cluster, source_topic_str, target_cluster, target_topic_str, map_function, group=None, offsets=None, source_key_type="bytes", source_value_type="bytes", target_key_type=None, target_value_type=None, target_key_schema=None, target_value_schema=None, keep_timestamps=True, n=ALL_MESSAGES, batch_size=1):
    def flatmap_function(message_dict):
        return [map_function(message_dict)]
    #
    return flatmap(source_cluster, source_topic_str, target_cluster, target_topic_str, flatmap_function, group=group, offsets=offsets, source_key_type=source_key_type, source_value_type=source_value_type, target_key_type=target_key_type, target_value_type=target_value_type, target_key_schema=target_key_schema, target_value_schema=target_value_schema, keep_timestamps=keep_timestamps, n=n, batch_size=batch_size)

def cp(source_cluster, source_topic_str, target_cluster, target_topic_str, flatmap_function=lambda x: [x], group=None, offsets=None, source_key_type="bytes", source_value_type="bytes", target_key_type=None, target_value_type=None, target_key_schema=None, target_value_schema=None, keep_timestamps=True, n=ALL_MESSAGES, batch_size=1):
    """Replicate a topic and optionally transform the messages in a flatmap-like manner.

    Replicate (parts of) a topic (source_topic_str) on one cluster (source_cluster) to another topic (target_topic_str) on another (or the same) cluster (target_cluster). Each replicated message can be transformed into a list of other messages in a flatmap-like manner. The source and target topics can have different message key and value types; e.g. the source topic can have value type Avro whereas the target topic will be written with value type Protobuf.

    Args:
        source_cluster (Cluster): Source cluster
        source_topic_str (str): Source topic
        target_cluster (Cluster): Target cluster
        target_topic_str (str): Target topic
        flatmap_function (function, optional): Flatmap function (takes a message dictionary and returns a list of message dictionaries). Defaults to lambda x: [x].
        group (str, optional): Consumer group name used for consuming from the source topic. If set to None, creates a new unique consumer group name. Defaults to None.
        offsets (dict, optional): Dictionary of offsets (keys: partitions (int), values: offsets for the partitions (int)) for consuming from the source topic. If set to None, consume the topic using the offsets from the consumer group for the topic. Defaults to None.
        source_key_type (str, optional): Source topic message key type ("bytes", "str", "json", "avro", "protobuf"/"pb" or "jsonschema"). Defaults to "bytes".
        source_value_type (str, optional): Source topic message value type ("bytes", "str", "json", "avro", "protobuf"/"pb" or "jsonschema"). Defaults to "bytes".
        target_key_type (str, optional): Target topic message key type ("bytes", "str", "json", "avro", "protobuf"/"pb" or "jsonschema"). If set to None, target_key_type = source_key_type. Defaults to None.
        target_value_type (str, optional): Target topic message value type ("bytes", "str", "json", "avro", "protobuf"/"pb" or "jsonschema"). If set to None, target_value_type = source_value_type. Defaults to None.
        target_key_schema (str, optional): Target key message type schema (for "avro", "protobuf"/"pb" or "jsonschema"). Defaults to None.
        target_value_schema (str, optional): Target value message type schema (for "avro", "protobuf"/"pb" or "jsonschema"). Defaults to None.
        keep_timestamps (bool, optional): Replicate the timestamps of the source messages in the target messages. Defaults to True.
        n (int, optional): Number of messages to consume from the source topic. Defaults to ALL_MESSAGES.
        batch_size (int, optional): Maximum number of messages to consume from the source topic at a time. Defaults to 1.

    Returns:
        tuple(int, int): Pair of the number of messages consumed from the source topic and the number of messages produced to the target topic.

    Examples:
        cp(cluster1, "topic1", cluster2, "topic2")
            Replicate "topic1" on cluster1 to "topic2" on cluster2.

        cp(cluster1, "topic1", cluster2, "topic2", flatmap_function=lambda message_dict: [message_dict, message_dict])
            Replicate "topic1" on cluster1 to "topic2" on cluster2, while duplicating each message from topic1 in topic2.

        cp(cluster1, "topic1", cluster2, "topic2", source_value_type="avro", target_value_type="protobuf", target_value_schema="message Snack { required string name = 1; required float calories = 2; optional string colour = 3; }", n=100)
            Replicate the first 100 messages from "topic1" on cluster1 to "topic2" on cluster2, changing the value schema from avro to protobuf.
        
        cp(cluster1, "topic1", cluster2, "topic2", offsets={0:100}, keep_timestamps=False, n=500)
            Replicate the messages 100-600 from "topic1" on cluster1 to "topic2" on cluster2. Create new timestamps for the messages produced to the target topic.     
    """
    return flatmap(source_cluster, source_topic_str, target_cluster, target_topic_str, flatmap_function, group=group, offsets=offsets, source_key_type=source_key_type, source_value_type=source_value_type, target_key_type=target_key_type, target_value_type=target_value_type, target_key_schema=target_key_schema, target_value_schema=target_value_schema, keep_timestamps=keep_timestamps, n=n, batch_size=batch_size)

def zip_foldl(cluster1, topic_str1, cluster2, topic_str2, zip_foldl_function, initial_acc, group1=None, group2=None, offsets1=None, offsets2=None, key_type1="bytes", value_type1="bytes", key_type2="bytes", value_type2="bytes", n=ALL_MESSAGES, batch_size=1):
    """Consume topic 1 on cluster 1 and topic 2 on cluster 2 and combine the messages using a foldl function.

    Consume (parts of) a topic (topic_str1) on one cluster (cluster1) and another topic (topic_str2) on another (or the same) cluster (cluster2) and combine them using a foldl function.

    Args:
        cluster1 (Cluster): Cluster 1
        topic_str1 (str): Topic 1
        cluster2 (Cluster): Cluster 2
        topic_str2 (str): Topic 2
        zip_foldl_function (function): Foldl function (takes an accumulator (any type) and a message dictionary and returns the updated accumulator)
        initial_acc: Initial value of the accumulator (any type).
        group1 (str, optional): Consumer group name used for consuming from topic 1. If set to None, creates a new unique consumer group name. Defaults to None.
        group2 (str, optional): Consumer group name used for consuming from topic 2. If set to None, creates a new unique consumer group name. Defaults to None.
        offsets1 (dict, optional): Dictionary of offsets (keys: partitions (int), values: offsets for the partitions (int)) for consuming from topic 1. If set to None, consume topic 1 using the offsets from the consumer group for topic 1. Defaults to None.
        offsets2 (dict, optional): Dictionary of offsets (keys: partitions (int), values: offsets for the partitions (int)) for consuming from topic 2. If set to None, consume topic 2 using the offsets from the consumer group for topic 2. Defaults to None.
        key_type1 (str, optional): Topic 1 message key type ("bytes", "str", "json", "avro", "protobuf"/"pb" or "jsonschema"). Defaults to "bytes".
        value_type1 (str, optional): Topic 1 message value type ("bytes", "str", "json", "avro", "protobuf"/"pb" or "jsonschema"). Defaults to "bytes".
        key_type2 (str, optional): Topic 2 message key type ("bytes", "str", "json", "avro", "protobuf"/"pb" or "jsonschema"). If set to None, target_key_type = source_key_type. Defaults to None.
        value_type2 (str, optional): Topic 2 message value type ("bytes", "str", "json", "avro", "protobuf"/"pb" or "jsonschema"). If set to None, target_value_type = source_value_type. Defaults to None.
        n (int, optional): Number of messages to consume from the topic 1 and topic 2. Defaults to ALL_MESSAGES.
        batch_size (int, optional): Maximum number of messages to consume from topic 1 and topic 2 at a time. Defaults to 1.

    Returns:
        tuple(acc, int, int): Tuple of the accumulator (any type), the number of messages consumed from topic 1 and the number of messages consumed from topic 2.

    Examples:
        zip_foldl(cluster1, "topic1", cluster2, "topic2", lambda acc, message_dict1, message_dict2: acc += [(message_dict1, message_dict2)], [])
            Consume "topic1" on cluster1 and "topic2" on cluster2 and return a list of pairs of message dictionaries from topic 1 and topic 2, respectively.
    """
    group_str1 = group1
    group_str2 = group2
    offsets_dict1 = offsets1
    offsets_dict2 = offsets2
    key_type_str1 = key_type1
    value_type_str1 = value_type1
    key_type_str2 = key_type2
    value_type_str2 = value_type2
    num_messages_int = n
    batch_size_int = batch_size
    #
    cluster1.subscribe(topic_str1, group=group_str1, offsets=offsets_dict1, key_type=key_type_str1, value_type=value_type_str1)
    cluster2.subscribe(topic_str2, group=group_str2, offsets=offsets_dict2, key_type=key_type_str2, value_type=value_type_str2)
    #
    message_counter_int1 = 0
    message_counter_int2 = 0
    acc = initial_acc
    while True:
        message_dict_list1 = []
        while True:
            message_dict_list1 += cluster1.consume(n=batch_size_int)
            if not message_dict_list1 or len(message_dict_list1) == batch_size_int:
                break
        if not message_dict_list1:
            break
        num_messages_int1 = len(message_dict_list1)
        message_counter_int1 += num_messages_int1
        if cluster1.verbose_int > 0 and message_counter_int1 % cluster1.kash_dict["progress.num.messages"] == 0:
            print(f"Consumed (topic 1): {message_counter_int1}")
        #
        batch_size_int2 = num_messages_int1 if num_messages_int1 < batch_size_int else batch_size_int
        message_dict_list2 = []
        while True:
            message_dict_list2 += cluster2.consume(n=batch_size_int2)
            if not message_dict_list2 or len(message_dict_list2) == batch_size_int2:
                break
        if not message_dict_list2:
            break
        num_messages_int2 = len(message_dict_list2)
        message_counter_int2 += num_messages_int2
        if cluster2.verbose_int > 0 and message_counter_int2 % cluster2.kash_dict["progress.num.messages"] == 0:
            print(f"Consumed (topic 2): {message_counter_int2}")
        #
        if num_messages_int1 != num_messages_int2:
            break
        #
        for message_dict1, message_dict2 in zip(message_dict_list1, message_dict_list2):
            acc = zip_foldl_function(acc, message_dict1, message_dict2)
        #
        if num_messages_int != ALL_MESSAGES:
            if message_counter_int1 >= num_messages_int:
                break
    #
    cluster1.unsubscribe()
    cluster2.unsubscribe()
    return acc, message_counter_int1, message_counter_int2


def diff_fun(cluster1, topic_str1, cluster2, topic_str2, diff_function, group1=None, group2=None, offsets1=None, offsets2=None, key_type1="bytes", value_type1="bytes", key_type2="bytes", value_type2="bytes", n=ALL_MESSAGES, batch_size=1):
    """Create a diff of topic 1 on cluster 1 and topic 2 on cluster 2 using a diff function.

    Create a diff of (parts of) a topic (topic_str1) on one cluster (cluster1) and another topic (topic_str2) on another (or the same) cluster (cluster2) using a diff function (diff_function).

    Args:
        cluster1 (Cluster): Cluster 1
        topic_str1 (str): Topic 1
        cluster2 (Cluster): Cluster 2
        topic_str2 (str): Topic 2
        diff_function (function): Diff function (takes a message dictionary from topic 1 and a message dictionary from topic 2 and returns the updated accumulator)
        group1 (str, optional): Consumer group name used for consuming from topic 1. If set to None, creates a new unique consumer group name. Defaults to None.
        group2 (str, optional): Consumer group name used for consuming from topic 2. If set to None, creates a new unique consumer group name. Defaults to None.
        offsets1 (dict, optional): Dictionary of offsets (keys: partitions (int), values: offsets for the partitions (int)) for consuming from topic 1. If set to None, consume topic 1 using the offsets from the consumer group for topic 1. Defaults to None.
        offsets2 (dict, optional): Dictionary of offsets (keys: partitions (int), values: offsets for the partitions (int)) for consuming from topic 2. If set to None, consume topic 2 using the offsets from the consumer group for topic 2. Defaults to None.
        key_type1 (str, optional): Topic 1 message key type ("bytes", "str", "json", "avro", "protobuf"/"pb" or "jsonschema"). Defaults to "bytes".
        value_type1 (str, optional): Topic 1 message value type ("bytes", "str", "json", "avro", "protobuf"/"pb" or "jsonschema"). Defaults to "bytes".
        key_type2 (str, optional): Topic 2 message key type ("bytes", "str", "json", "avro", "protobuf"/"pb" or "jsonschema"). If set to None, target_key_type = source_key_type. Defaults to None.
        value_type2 (str, optional): Topic 2 message value type ("bytes", "str", "json", "avro", "protobuf"/"pb" or "jsonschema"). If set to None, target_value_type = source_value_type. Defaults to None.
        n (int, optional): Number of messages to consume from the topic 1 and topic 2. Defaults to ALL_MESSAGES.
        batch_size (int, optional): Maximum number of messages to consume from topic 1 and topic 2 at a time. Defaults to 1.

    Returns:
        list(tuple(message_dict, message_dict)): Tuple of message dictionaries from topic 1 and topic 2 which are different according to the diff_function (=where diff_function(message_dict1, message_dict2) returned True).

    Examples:
        diff_fun(cluster1, "topic1", cluster2, "topic2", lambda message_dict1, message_dict2: message_dict1["value"] != message_dict2["value"])
            Create a diff of "topic1" on cluster1 and "topic2" on cluster2 by comparing the message values.
    """
    def zip_foldl_function(acc, message_dict1, message_dict2):
        if diff_function(message_dict1, message_dict2):
            acc += [(message_dict1, message_dict2)]
            #
            if cluster1.verbose_int > 0:
                partition_int1 = message_dict1["partition"]
                offset_int1 = message_dict1["offset"]
                partition_int2 = message_dict2["partition"]
                offset_int2 = message_dict2["offset"]
                print(f"Found differing messages on 1) partition {partition_int1}, offset {offset_int1} and 2) partition {partition_int2}, offset {offset_int2}.")
            #
        return acc
    #
    return zip_foldl(cluster1, topic_str1, cluster2, topic_str2, zip_foldl_function, [], group1=group1, group2=group2, offsets1=offsets1, offsets2=offsets2, key_type1=key_type1, value_type1=value_type1, key_type2=key_type2, value_type2=value_type2, n=n, batch_size=batch_size)


def diff(cluster1, topic_str1, cluster2, topic_str2, group1=None, group2=None, offsets1=None, offsets2=None, key_type1="bytes", value_type1="bytes", key_type2="bytes", value_type2="bytes", n=ALL_MESSAGES, batch_size=1):
    """Create a diff of topic 1 on cluster 1 and topic 2 on cluster 2 using a diff function.

    Create a diff of (parts of) a topic (topic_str1) on one cluster (cluster1) and another topic (topic_str2) on another (or the same) cluster (cluster2) with respect to their keys and values.

    Args:
        cluster1 (Cluster): Cluster 1
        topic_str1 (str): Topic 1
        cluster2 (Cluster): Cluster 2
        topic_str2 (str): Topic 2
        group1 (str, optional): Consumer group name used for consuming from topic 1. If set to None, creates a new unique consumer group name. Defaults to None.
        group2 (str, optional): Consumer group name used for consuming from topic 2. If set to None, creates a new unique consumer group name. Defaults to None.
        offsets1 (dict, optional): Dictionary of offsets (keys: partitions (int), values: offsets for the partitions (int)) for consuming from topic 1. If set to None, consume topic 1 using the offsets from the consumer group for topic 1. Defaults to None.
        offsets2 (dict, optional): Dictionary of offsets (keys: partitions (int), values: offsets for the partitions (int)) for consuming from topic 2. If set to None, consume topic 2 using the offsets from the consumer group for topic 2. Defaults to None.
        key_type1 (str, optional): Topic 1 message key type ("bytes", "str", "json", "avro", "protobuf"/"pb" or "jsonschema"). Defaults to "bytes".
        value_type1 (str, optional): Topic 1 message value type ("bytes", "str", "json", "avro", "protobuf"/"pb" or "jsonschema"). Defaults to "bytes".
        key_type2 (str, optional): Topic 2 message key type ("bytes", "str", "json", "avro", "protobuf"/"pb" or "jsonschema"). If set to None, target_key_type = source_key_type. Defaults to None.
        value_type2 (str, optional): Topic 2 message value type ("bytes", "str", "json", "avro", "protobuf"/"pb" or "jsonschema"). If set to None, target_value_type = source_value_type. Defaults to None.
        n (int, optional): Number of messages to consume from the topic 1 and topic 2. Defaults to ALL_MESSAGES.
        batch_size (int, optional): Maximum number of messages to consume from topic 1 and topic 2 at a time. Defaults to 1.

    Returns:
        list(tuple(message_dict, message_dict)): Tuple of message dictionaries from topic 1 and topic 2 which are different with respect to their keys and values.

    Examples:
        diff(cluster1, "topic1", cluster2, "topic2")
            Create a diff of "topic1" on cluster1 and "topic2" on cluster2 with respect to their keys and values.
    """
    def diff_function(message_dict1, message_dict2):
        return message_dict1["key"] != message_dict2["key"] or message_dict1["value"] != message_dict2["value"]
    return diff_fun(cluster1, topic_str1, cluster2, topic_str2, diff_function, group1=group1, group2=group2, offsets1=offsets1, offsets2=offsets2, key_type1=key_type1, value_type1=value_type1, key_type2=key_type2, value_type2=value_type2, n=n, batch_size=batch_size)


# Main kash.py class

class Cluster:
    def __init__(self, cluster_str):
        self.cluster_str = cluster_str
        self.config_dict, self.schema_registry_config_dict, self.kash_dict, self.cluster_dir_str = get_config_dict(cluster_str)
        #
        self.adminClient = get_adminClient(self.config_dict)
        #
        self.producer = get_producer(self.config_dict)
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
        #
        self.produced_messages_counter_int = 0
        #
        self.verbose_int = 1 if is_interactive() else 0
        #
        # Kash cluster config
        # Producer
        if "flush.num.messages" not in self.kash_dict:
            self.kash_dict["flush.num.messages"] = 10000
        else:
            self.kash_dict["flush.num.messages"] = int(self.kash_dict["flush.num.messages"])
        if "flush.timeout" not in self.kash_dict:
            self.kash_dict["flush.timeout"] = -1
        else:
            self.kash_dict["flush.timeout"] = float(self.kash_dict["flush.timeout"])
        # Consumer
        if "consume.timeout" not in self.kash_dict:
            self.kash_dict["consume.timeout"] = 3.0
        else:
            self.kash_dict["consume.timeout"] = float(self.kash_dict["consume.timeout"])
        #
        if "auto.offset.reset" not in self.kash_dict:
            self.kash_dict["auto.offset.reset"] = "earliest"
        else:
            self.kash_dict["auto.offset.reset"] = str(self.kash_dict["auto.offset.reset"])
        if "enable.auto.commit" not in self.kash_dict:
            self.kash_dict["enable.auto.commit"] = True
        else:
            self.kash_dict["enable.auto.commit"] = str_to_bool(self.kash_dict["enable.auto.commit"])
        if "session.timeout.ms" not in self.kash_dict:
            self.kash_dict["session.timeout.ms"] = 10000
        else:
            self.kash_dict["session.timeout.ms"] = int(self.kash_dict["session.timeout.ms"])
        # Standard output
        if "progress.num.messages" not in self.kash_dict:
            self.kash_dict["progress.num.messages"] = 1000
        else:
            self.kash_dict["progress.num.messages"] = int(self.kash_dict["progress.num.messages"])
        # Await
        if "await.num.retries" not in self.kash_dict:
            self.kash_dict["await.num.retries"] = 50
        else:
            self.kash_dict["await.num.retries"] = int(self.kash_dict["await.num.retries"])
        if "await.interval" not in self.kash_dict:
            self.kash_dict["await.interval"] = 0.1
        else:
            self.kash_dict["await.interval"] = float(self.kash_dict["await.interval"])

    #

    def set_verbose(self, verbose_int):
        self.verbose_int = verbose_int

    def verbose(self):
        return self.verbose_int

    # Schema Registry helper methods (inside the Cluster class to do caching etc.)

    def post_schema(self, schema_str, schema_type_str, topic_str, key_bool):
        key_or_value_str = "key" if key_bool else "value"
        #
        schema_registry_url_str = self.schema_registry_config_dict["schema.registry.url"]
        url_str = f"{schema_registry_url_str}/subjects/{topic_str}-{key_or_value_str}/versions?normalize=true"
        headers_dict = {"Accept": "application/vnd.schemaregistry.v1+json", "Content-Type": "application/vnd.schemaregistry.v1+json"}
        if "basic.auth.user.info" in self.schema_registry_config_dict:
            user_password_str = self.schema_registry_config_dict["basic.auth.user.info"]
            user_str_password_str_tuple = tuple(user_password_str.split(":"))
        else:
            user_str_password_str_tuple = None
        schema_dict = {"schema": schema_str, "schemaType": schema_type_str}
        response = requests.post(url_str, headers=headers_dict, json=schema_dict, auth=user_str_password_str_tuple)
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

    def size(self, pattern_str, timeout=-1.0):
        """List topics, their total sizes and the sizes of their partitions.

        List topics on the cluster whose names match the pattern pattern_str, their total sizes and the sizes of their partitions.

        Args:
            pattern (str): The pattern for selecting those topics which shall be listed.
            timeout (float, optional): The timeout for the internally used get_watermark_offsets() method calls from confluent_kafka.Consumer. Defaults to -1.0 (infinite=no timeout).

        Returns:
            dict(str, tuple(int, dict(int, int))): Dictionary of strings (topic name) and pairs of integers (total size of the topic) and dictionaries of integers (partition) and integers (size of the partition).

        Examples:
            size("*")
                List all topics of the cluster, their total sizes and the sizes of their partitions.

            size("*test", timeout=1.0)
                List those topics whose name ends with "test", their total sizes and the sizes of their partitions and time out the internally used get_watermark_offsets() method after one second.
        """
        topic_str_partition_int_tuple_dict_dict = self.watermarks(pattern_str, timeout=timeout)
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

    def watermarks(self, pattern_str, timeout=-1.0):
        """Get low and high offsets (=so called "watermarks") of topics on the cluster.

        Returns a dictionary of the topics whose names match the bash-like pattern pattern_str and the low and high offsets of their partitions.

        Args:
            pattern_str (str): The pattern for selecting the topics.
            timeout (float, optional): The timeout for the individual get_watermark_offsets() method calls from confluent_kafka.Consumer. Defaults to -1.0 (infinite=no timeout).

        Returns:
            dict(str, dict(int, tuple(int, int))): Dictionary of strings (topic names) and dictionaries of integers (partition) and pairs of integers (low and high offsets of the respective partition of the respective topic)

        Examples:
            watermarks("test*")
                Return the watermarks for all topics whose name starts with "test" and their partitions.

            watermarks("test", timeout=1.0)
                Return the watermarks for the topic "test" and time out the internally used get_watermark_offsets() method after one second.
        """
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

    def topics(self, pattern=None, size=False, partitions=False):
        """List topics on the cluster.

        List topics on the cluster. Optionally return only those topics whose names match a bash-like pattern. Optionally return the total sizes of the topics and the sizes of their individual partitions.

        Args:
            pattern (str, optional): The pattern for selecting those topics which shall be listed. Defaults to None.
            size (bool, optional): Return the total sizes of the topics if set to True. Defaults to False.
            partitions (bool, optional): Return the sizes of the individual partitions of the topics if set to True. Defaults to False.

        Returns:
            list(str) | dict(str, int) | dict(str, dict(int, int)) | dict(str, tuple(int, dict(int, int))): List of strings if size=False and partitions=False; dictionary of strings (topic name) and integers (total size of the topic) if size=True and partitions=False; dictionary of strings (topic name) and dictionaries of integers (partition) and integers (size of the partition) if size=False and partitions=True; dictionary of strings (topic name) and pairs of integers (total size of the topic) and dictionaries of integers (partition) and integers (size of the partition) if size=True and partitions=True.

        Examples:
            topics()
                List all topics of the cluster.

            topics(size=True)
                List all the topics of the cluster and their total sizes.
            
            topics(partitions=True)
                List all the topics of the cluster and the sizes of their individual partitions.

            topics(size=True, partitions=True)
                List all the topics of the cluster, their total sizes and the sizes of their individual partitions.

            topics("test*")
                List all those topics of the cluster whose name starts with "test". 
        """
        pattern_str = pattern
        size_bool = size
        partitions_bool = partitions
        #
        if size_bool:
            topic_str_total_size_int_size_dict_tuple_dict = self.size(pattern_str)
            if partitions_bool:
                return topic_str_total_size_int_size_dict_tuple_dict
            else:
                topic_str_size_int_dict = {topic_str: topic_str_total_size_int_size_dict_tuple_dict[topic_str][0] for topic_str in topic_str_total_size_int_size_dict_tuple_dict}
                return topic_str_size_int_dict
        else:
            if partitions_bool:
                topic_str_total_size_int_size_dict_tuple_dict = self.size(pattern_str)
                topic_str_size_dict_dict = {topic_str: topic_str_total_size_int_size_dict_tuple_dict[topic_str][1] for topic_str in topic_str_total_size_int_size_dict_tuple_dict}
                return topic_str_size_dict_dict
            else:
                topic_str_list = list(self.adminClient.list_topics().topics.keys())
                if pattern_str is not None:
                    topic_str_list = [topic_str for topic_str in topic_str_list if fnmatch(topic_str, pattern_str)]
                topic_str_list.sort()
                return topic_str_list

    ls = topics
    """List topics on the cluster (shortcut for topics()).

    List topics on the cluster. Optionally return only those topics whose names match a bash-like pattern. Optionally return the total sizes of the topics and the sizes of their individual partitions.

    Args:
        pattern (str, optional): The pattern for selecting those topics which shall be listed. Defaults to None.
        size (bool, optional): Return the total sizes of the topics if set to True. Defaults to False.
        partitions (bool, optional): Return the sizes of the individual partitions of the topics if set to True. Defaults to False.

    Returns:
        list(str) | dict(str, int) | dict(str, dict(int, int)) | dict(str, tuple(int, dict(int, int))): List of strings if size=False and partitions=False; dictionary of strings (topic name) and integers (total size of the topic) if size=True and partitions=False; dictionary of strings (topic name) and dictionaries of integers (partition) and integers (size of the partition) if size=False and partitions=True; dictionary of strings (topic name) and pairs of integers (total size of the topic) and dictionaries of integers (partition) and integers (size of the partition) if size=True and partitions=True.

    Examples:
        ls()
            List all topics of the cluster.

        ls(size=True)
            List all the topics of the cluster and their total sizes.
        
        ls(partitions=True)
            List all the topics of the cluster and the sizes of their individual partitions.

        ls(size=True, partitions=True)
            List all the topics of the cluster, their total sizes and the sizes of their individual partitions.

        ls  ("test*")
            List all those topics of the cluster whose name starts with "test". 
    """

    def l(self, pattern=None, size=True, partitions=False):
        """List topics on the cluster (shortcut for topics(size=True), a la the "l" alias in bash).

        List topics on the cluster. Optionally return only those topics whose names match a bash-like pattern. Optionally return the total sizes of the topics and the sizes of their individual partitions.

        Args:
            pattern (str, optional): The pattern for selecting those topics which shall be listed. Defaults to None.
            size (bool, optional): Return the total sizes of the topics if set to True. Defaults to True.
            partitions (bool, optional): Return the sizes of the individual partitions of the topics if set to True. Defaults to False.

        Returns:
            list(str) | dict(str, int) | dict(str, dict(int, int)) | dict(str, tuple(int, dict(int, int))): List of strings if size=False and partitions=False; dictionary of strings (topic name) and integers (total size of the topic) if size=True and partitions=False; dictionary of strings (topic name) and dictionaries of integers (partition) and integers (size of the partition) if size=False and partitions=True; dictionary of strings (topic name) and pairs of integers (total size of the topic) and dictionaries of integers (partition) and integers (size of the partition) if size=True and partitions=True.

        Examples:
            l()
                List all the topics of the cluster and their total sizes.
            
            l(size=False)
                List all the topics of the cluster.
            
            l(partitions=True)
                List all the topics of the cluster, their total sizes and the sizes of their individual partitions.

            l(size=False, partitions=True)
                List all the topics of the cluster and the sizes of their individual partitions.
            
            l("test*")
                List all those topics of the cluster whose name starts with "test" and their total sizes. 
        """
        return self.topics(pattern=pattern, size=size, partitions=partitions)

    ll = l
    """List topics on the cluster (shortcut for topics(size=True), a la the "ll" alias in bash).

    List topics on the cluster. Optionally return only those topics whose names match a bash-like pattern. Optionally return the total sizes of the topics and the sizes of their individual partitions.

    Args:
        pattern (str, optional): The pattern for selecting those topics which shall be listed. Defaults to None.
        size (bool, optional): Return the total sizes of the topics if set to True. Defaults to True.
        partitions (bool, optional): Return the sizes of the individual partitions of the topics if set to True. Defaults to False.

    Returns:
        list(str | dict(str, int) | dict(str, dict(int, int)) | dict(str, tuple(int, dict(int, int)))): List of strings if size=False and partitions=False; dictionary of strings (topic name) and integers (total size of the topic) if size=True and partitions=False; dictionary of strings (topic name) and dictionaries of integers (partition) and integers (size of the partition) if size=False and partitions=True; dictionary of strings (topic name) and pairs of integers (total size of the topic) and dictionaries of integers (partition) and integers (size of the partition) if size=True and partitions=True.

    Examples:
        ll()
            List all the topics of the cluster and their total sizes.
        
        ll(size=False)
            List all the topics of the cluster.
        
        ll(partitions=True)
            List all the topics of the cluster, their total sizes and the sizes of their individual partitions.

        ll(size=False, partitions=True)
            List all the topics of the cluster and the sizes of their individual partitions.
        
        ll("test*")
            List all those topics of the cluster whose name starts with "test" and their total sizes. 
    """

    def config(self, pattern_str):
        """Return the configuration of topics.

        Return the configuration of those topics whose names match the bash-like pattern pattern_str.

        Args:
            pattern_str (str): The pattern for selecting the topics for which the configuration shall be returned.

        Returns:
            dict(str, dict(str, str)): Dictionary of strings (topic names) and dictionaries of strings (configuration keys) and strings (configuration values).
        
        Examples:
            config("test")
                Return the configuration of the topic "test".
            
            config("test?")
                Return the configuration of all topics matching the pattern "test?"
        """
        topic_str_list = self.topics(pattern_str)
        #
        topic_str_config_dict_dict = {topic_str: self.get_config_dict(ResourceType.TOPIC, topic_str) for topic_str in topic_str_list}
        #
        return topic_str_config_dict_dict

    def set_config(self, pattern_str, key_str, value_str, test=False):
        """Set a configuration item of topics.

        Set the configuration item with key key_str and value value_str of those topics whose names match the bash-like pattern pattern_str.

        Args:
            pattern_str (str): The pattern for selecting those topics whose configuration shall be changed.

        Returns:
            dict(str, tuple(str, str)): Dictionary of strings (topic names) and tuples of strings (configuration keys) and strings (configuration values)
        
        Examples:
            set_config("test", "retention.ms", "4711")
                Sets the configuration key "retention.ms" to configuration value "4711" for topic "test".
            
            set_config("test*", "42")
                Sets the configuration key "retention.ms" to configuration value "42" for all topics whose names start with "test".
        """
        test_bool = test
        #
        topic_str_list = self.topics(pattern_str)
        #
        for topic_str in topic_str_list:
            self.set_config_dict(ResourceType.TOPIC, topic_str, {key_str: value_str}, test_bool)
        #
        topic_str_key_str_value_str_tuple_dict = {topic_str: (key_str, value_str) for topic_str in topic_str_list}
        return topic_str_key_str_value_str_tuple_dict
    
    def await_topic(self, topic_str, exists=True):
        exists_bool = exists
        #
        num_retries_int = 0
        while True:
            if exists_bool:
                if self.exists(topic_str):
                    return True
            else:
                if not self.exists(topic_str):
                    return True
            #
            num_retries_int += 1
            if num_retries_int >= self.kash_dict["await.num.retries"]:
                break
            time.sleep(self.kash_dict["await.interval"])
        return False

    def create(self, topic_str, partitions=1, config={"retention.ms": "-1"}, block=True):
        """Create a topic.

        Create a topic.

        Args:
            topic_str (str): The name of the topic to be created.
            partitions (int, optional): The number of partitions for the topic to be created. Defaults to 1.
            config (dict(str, str), optional): Configuration overrides for the topic to be created. Defaults to {"retention.ms": "-1"} (unlimited retention).
            block (bool, optional): Block until the topic is created. Defaults to True.

        Returns:
            str: Name of the created topic.
        
        Examples:
            create("test")
                Create the topic "test" with one partition, unlimited retention and block until it is created.
            
            create("test", partitions=2)
                Create the topic "test" with two partitions, unlimited retention and block until it is created.

            create("test", config={"retention.ms": "4711"})
                Create the topic "test" with one partition, a retention time of 4711ms and block until it is created.
            
            create("test", block=False)
                Create the topic "test" with one partition, unlimited retention time and *do not* block until it is created.
        """
        partitions_int = partitions
        config_dict = config
        block_bool = block
        #
        newTopic = NewTopic(topic_str, partitions_int, config=config_dict)
        self.adminClient.create_topics([newTopic])
        #
        if block_bool:
            self.await_topic(topic_str, exists=True)
        #
        return topic_str

    def delete(self, pattern_str, block=True):
        """Delete topics.

        Delete those topics whose names match the bash-like pattern pattern_str.

        Args:
            pattern_str (str): The pattern for selecting the topics to be deleted.
            block (bool, optional): Block until the topic is deleted. Defaults to True.

        Returns:
            list(str): List of strings of names of the deleted topics.
        
        Examples:
            delete("test")
                Delete the topic "test" and block until it is deleted.
            
            delete("test", block=False)
                Delete the topic "test" and *do not* block until it is deleted.

            delete("test*")
                Delete all topics starting with "test".
        """
        block_bool = block
        #
        topic_str_list = self.topics(pattern_str)
        #
        if topic_str_list:
            self.adminClient.delete_topics(topic_str_list)
            if block_bool:
                for topic_str in topic_str_list:
                    self.await_topic(topic_str, exists=False)
        #    
        return topic_str_list

    rm = delete
    """Delete topics (shortcut for delete(), a la bash "rm")

    Delete those topics whose names match the bash-like pattern pattern_str.

    Args:
        pattern_str (str): The pattern for selecting the topics to be deleted.
        block (bool, optional): Block until the topic is deleted. Defaults to True.

    Returns:
        list(str): List of strings of names of the deleted topics.
    
    Examples:
        rm("test")
            Delete the topic "test" and block until it is deleted.
        
        rm("test", block=False)
            Delete the topic "test" and *do not* block until it is deleted.

        rm("test*")
            Delete all topics starting with "test".
    """

    def offsets_for_times(self, pattern_str, partition_int_timestamp_int_dict, timeout=-1.0):
        """Look up offsets corresponding to message timestamps in the partitions of topics.

        Look up those offsets in the individual partitions of all topics matching the bash-like pattern pattern_str which correspond to the timestamps provided in partition_int_timestamp_int_dict (for the individual partitions).

        Args:
            pattern_str (str): The pattern for selecting the topics.
            partition_int_timestamp_int_dict (dict(int, int)): Dictionary of integers (partitions) and integers (timestamps).
            timeout (float, optional): The timeout for the individual offsets_for_times() method calls from confluent_kafka.Consumer. Defaults to -1.0 (infinite=no timeout).

        Returns:
            dict(str, dict(int, int)): Dictionary of strings (topic names) and dictionaries of integers (partitions) and integers (offsets).
        
        Examples:
            offsets_for_times("test", {0: 1664644769886})
                Look up the offset of the first message in the first partition of the topic "test" which has a timestamp greater or equal to 1664644769886 milliseconds from epoch. If the provided timestamp exceeds that of the last message in the partition, a value of -1 will be returned.

            offsets_for_times("te*st", {0: 1664644769886, 1: 1664645155987}, timeout=1.0)
                Look up the offset of the first message in the first partition of those topics starting with "te" and ending with "st" with a timestamp greater or equal to 1664644769886 milliseconds from epoch; and look up the offset of the first message in the second partition of those topics with a timestamp greater or equal to 1664645155987 milliseconds from epoch. Time out the internally used offsets_for_times() calls after one second.
        """
        topic_str_list = self.topics(pattern_str)
        #
        topic_str_partition_int_offsets_int_dict_dict = {}
        for topic_str in topic_str_list:
            partition_int_offset_int_dict = {}
            #
            topicPartition_list = [TopicPartition(topic_str, partition_int, timestamp_int) for partition_int, timestamp_int in partition_int_timestamp_int_dict.items()]
            if topicPartition_list:
                config_dict = self.config_dict
                config_dict["group.id"] = "dummy_group_id"
                consumer = get_consumer(config_dict)
                topicPartition_list1 = consumer.offsets_for_times(topicPartition_list, timeout=timeout)
                #
                for topicPartition in topicPartition_list1:
                    partition_int_offset_int_dict[topicPartition.partition] = topicPartition.offset
                #
                topic_str_partition_int_offsets_int_dict_dict[topic_str] = partition_int_offset_int_dict
        #
        return topic_str_partition_int_offsets_int_dict_dict

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
        self.produced_messages_counter_int += 1
        #
        return key_str_or_bytes, value_str_or_bytes 

    def flatmap_from_file(self, path_str, topic_str, flatmap_function, key_type="str", value_type="str", key_schema=None, value_schema=None, key_value_separator=None, message_separator="\n", n=ALL_MESSAGES, bufsize=4096):
        key_value_separator_str = key_value_separator
        message_separator_str = message_separator
        num_messages_int = n
        bufsize_int = bufsize
        #

        def foldl_function(_, line_str):
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
                key_str_value_str_tuple_list = flatmap_function((key_str, value_str))
                #
                for (key_str, value_str) in key_str_value_str_tuple_list:
                    self.produce(topic_str, value_str, key=key_str, key_type=key_type, value_type=value_type, key_schema=key_schema, value_schema=value_schema)
                    #
                    if self.produced_messages_counter_int % self.kash_dict["flush.num.messages"] == 0:
                        self.flush()
                    #
                    if self.verbose_int > 0 and self.produced_messages_counter_int % self.kash_dict["progress.num.messages"] == 0:
                        print(f"Produced: {self.produced_messages_counter_int}")
        #
        self.produced_messages_counter_int = 0
        #
        progress_num_messages_int = self.kash_dict["progress.num.messages"]
        (_, lines_counter_int) = foldl_file(path_str, foldl_function, None, delimiter=message_separator_str, n=num_messages_int, bufsize=bufsize_int, verbose=self.verbose_int, progress_num_lines=progress_num_messages_int)
        self.flush()
        #
        return (lines_counter_int, self.produced_messages_counter_int)

    def upload(self, path_str, topic_str, flatmap_function=lambda x: [x], key_type="str", value_type="str", key_schema=None, value_schema=None, key_value_separator=None, message_separator="\n", n=ALL_MESSAGES, bufsize=4096):
        return self.flatmap_from_file(path_str, topic_str, flatmap_function, key_type=key_type, value_type=value_type, key_schema=key_schema, value_schema=value_schema, key_value_separator=key_value_separator, message_separator=message_separator, n=n, bufsize=bufsize)

    def flush(self):
        self.producer.flush(self.kash_dict["flush.timeout"])

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
        self.config_dict["auto.offset.reset"] = self.kash_dict["auto.offset.reset"]
        self.config_dict["enable.auto.commit"] = self.kash_dict["enable.auto.commit"]
        self.config_dict["session.timeout.ms"] = self.kash_dict["session.timeout.ms"]
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
            print("Please subscribe to a topic before consuming.")
            return
        #
        message_list = self.consumer.consume(num_messages_int, self.kash_dict["consume.timeout"])
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

    #

    def foldl(self, topic_str, foldl_function, initial_acc, group=None, offsets=None, key_type="str", value_type="str", n=ALL_MESSAGES, batch_size=1):
        group_str = group
        offsets_dict = offsets
        key_type_str = key_type
        value_type_str = value_type
        num_messages_int = n
        batch_size_int = batch_size
        #
        self.subscribe(topic_str, group=group_str, offsets=offsets_dict, key_type=key_type_str, value_type=value_type_str)
        #
        acc = initial_acc
        message_counter_int = 0
        while True:
            message_dict_list = self.consume(n=batch_size_int)
            if not message_dict_list:
                break
            #
            for message_dict in message_dict_list:
                acc = foldl_function(acc, message_dict)
            #
            message_counter_int += len(message_dict_list)
            if self.verbose_int > 0 and message_counter_int % self.kash_dict["progress.num.messages"] == 0:
                print(f"Consumed: {message_counter_int}")
            if num_messages_int != ALL_MESSAGES:
                if message_counter_int >= num_messages_int:
                    break
        self.unsubscribe()
        return (acc, message_counter_int)

    #

    def flatmap(self, topic_str, flatmap_function, group=None, offsets=None, key_type="str", value_type="str", n=ALL_MESSAGES, batch_size=1):
        def foldl_function(acc, message_dict):
            acc += flatmap_function(message_dict)
            return acc
        #
        return self.foldl(topic_str, foldl_function, [], group=group, offsets=offsets, key_type=key_type, value_type=value_type, n=n, batch_size=batch_size)

    #

    def map(self, topic_str, map_function, group=None, offsets=None, key_type="str", value_type="str", n=ALL_MESSAGES, batch_size=1):
        def flatmap_function(message_dict):
            return [map_function(message_dict)]
        #
        return self.flatmap(topic_str, flatmap_function, group=group, offsets=offsets, key_type=key_type, value_type=value_type, n=n, batch_size=batch_size)

    #

    def foreach(self, topic_str, foreach_function, group=None, offsets=None, key_type="str", value_type="str", n=ALL_MESSAGES, batch_size=1):
        group_str = group
        offsets_dict = offsets
        key_type_str = key_type
        value_type_str = value_type
        num_messages_int = n
        batch_size_int = batch_size
        #

        def foldl_function(_, message_dict):
            foreach_function(message_dict)
            return None
        #
        (_, message_counter_int) = self.foldl(topic_str, foldl_function, None, group=group_str, offsets=offsets_dict, key_type=key_type_str, value_type=value_type_str, n=num_messages_int, batch_size=batch_size_int)
        #
        return message_counter_int

    #

    def cat(self, topic_str, foreach_function=print, group=None, offsets=None, key_type="str", value_type="str", n=ALL_MESSAGES, batch_size=1):
        return self.foreach(topic_str, foreach_function, group=group, offsets=offsets, key_type=key_type, value_type=value_type, n=n, batch_size=batch_size)

    #

    def grep_fun(self, topic_str, match_function, group=None, offsets=None, key_type="str", value_type="str", n=ALL_MESSAGES, batch_size=1):
        group_str = group
        offsets_dict = offsets
        key_type_str = key_type
        value_type_str = value_type
        num_messages_int = n
        batch_size_int = batch_size
        #

        def flatmap_function(message_dict):
            if match_function(message_dict):
                if self.verbose_int > 0:
                    partition_int = message_dict["partition"]
                    offset_int = message_dict["offset"]
                    print(f"Found matching message on partition {partition_int}, offset {offset_int}.")
                return [message_dict]
            else:
                return []
        #
        return self.flatmap(topic_str, flatmap_function, group=group_str, offsets=offsets_dict, key_type=key_type_str, value_type=value_type_str, n=num_messages_int, batch_size=batch_size_int)

    def grep(self, topic_str, re_pattern_str, group=None, offsets=None, key_type="str", value_type="str", n=ALL_MESSAGES, batch_size=1):
        def match_function(message_dict):
            pattern = re.compile(re_pattern_str)
            key_str = str(message_dict["key"])
            value_str = str(message_dict["value"])
            return pattern.match(key_str) != None or pattern.match(value_str) != None
        #
        return self.grep_fun(topic_str, match_function, group=group, offsets=offsets, key_type=key_type, value_type=value_type, n=n, batch_size=batch_size)

    #

    def wc(self, topic_str, group=None, offsets=None, key_type="str", value_type="str", n=ALL_MESSAGES, batch_size=1):
        def foldl_function(acc, message_dict):
            if message_dict["key"] is None:
                key_str = ""
            else:
                key_str = str(message_dict["key"])
            num_words_key_int = 0 if key_str == "" else len(key_str.split(" "))
            num_bytes_key_int = len(key_str)
            #
            if message_dict["value"] is None:
                value_str = ""
            else:
                value_str = str(message_dict["value"])
            num_words_value_int = len(value_str.split(" "))
            num_bytes_value_int = len(value_str)
            #
            acc_num_words_int = acc[0] + num_words_key_int + num_words_value_int
            acc_num_bytes_int = acc[1] + num_bytes_key_int + num_bytes_value_int
            return (acc_num_words_int, acc_num_bytes_int)
    #
        ((acc_num_words_int, acc_num_bytes_int), num_messages_int) = self.foldl(topic_str, foldl_function, (0, 0), group=group, offsets=offsets, key_type=key_type, value_type=value_type, n=n, batch_size=batch_size)
        return (num_messages_int, acc_num_words_int, acc_num_bytes_int)

    #

    def flatmap_to_file(self, topic_str, path_str, flatmap_function, group=None, offsets=None, key_type="str", value_type="str", key_value_separator=None, message_separator="\n", overwrite=True, n=ALL_MESSAGES, batch_size=1):
        key_value_separator_str = key_value_separator
        message_separator_str = message_separator
        overwrite_bool = overwrite
        #
        mode_str = "w" if overwrite_bool else "a"
        #
        
        def foldl_function(acc, message_dict):
            textIOWrapper = acc
            #
            message_dict_list = flatmap_function(message_dict)
            #
            output_str_list = []
            for message_dict in message_dict_list:
                value = message_dict["value"]
                if isinstance(value, dict):
                    value = json.dumps(value)
                #
                if key_value_separator_str is None:
                    output = value
                else:
                    key = message_dict["key"]
                    if isinstance(key, dict):
                        key = json.dumps(key)
                    output = f"{key}{key_value_separator_str}{value}"
                #
                output_str = f"{output}{message_separator_str}"
                output_str_list.append(output_str)
            #
            textIOWrapper.writelines(output_str_list)
            #
            return textIOWrapper
        #
        with open(path_str, mode_str) as textIOWrapper:
            (_, message_counter_int) = self.foldl(topic_str, foldl_function, textIOWrapper, group=group, offsets=offsets, key_type=key_type, value_type=value_type, n=n, batch_size=batch_size)
        self.unsubscribe()
        #
        return message_counter_int

    def download(self, topic_str, path_str, flatmap_function=lambda message_dict: [message_dict], group=None, offsets=None, key_type="str", value_type="str", key_value_separator=None, message_separator="\n", overwrite=True, n=ALL_MESSAGES, batch_size=1):
        return self.flatmap_to_file(topic_str, path_str, flatmap_function, group=group, offsets=offsets, key_type=key_type, value_type=value_type, key_value_separator=key_value_separator, message_separator=message_separator, overwrite=overwrite, n=n, batch_size=batch_size)

    def cp(self, source_str, target_str, group=None, offsets=None, flatmap_function=lambda x: [x], source_key_type="str", source_value_type="str", target_key_type="str", target_value_type="str", target_key_schema=None, target_value_schema=None, key_value_separator=None, message_separator="\n", overwrite=True, keep_timestamps=True, n=ALL_MESSAGES, batch_size=1, bufsize=4096):
        #
        if is_file(source_str) and not is_file(target_str):
            return self.upload(source_str, target_str, flatmap_function=flatmap_function, key_type=target_key_type, value_type=target_value_type, key_schema=target_key_schema, value_schema=target_value_schema, key_value_separator=key_value_separator, message_separator=message_separator, n=n, bufsize=bufsize)
        elif not is_file(source_str) and is_file(target_str):
            return self.download(source_str, target_str, flatmap_function=flatmap_function, group=group, offsets=offsets, key_type=source_key_type, value_type=source_value_type, key_value_separator=key_value_separator, message_separator=message_separator, overwrite=overwrite, n=n, batch_size=batch_size)
        elif (not is_file(source_str)) and (not is_file(target_str)):
            return cp(self, source_str, self, target_str, flatmap_function=flatmap_function, group=group, offsets=offsets, source_key_type=source_key_type, source_value_type=source_value_type, target_key_type=target_key_type, target_value_type=target_value_type, target_key_schema=target_key_schema, target_value_schema=target_value_schema, keep_timestamps=keep_timestamps, n=n, batch_size=batch_size)
        elif is_file(source_str) and is_file(target_str):
            print("Please use a shell or file manager to copy files.")

    def recreate(self, topic_str):
        temp_topic_str = f"{topic_str}_{get_millis()}"
        cp(self, topic_str, self, temp_topic_str)
        #
        produced_messages_int = 0
        if self.size(temp_topic_str)[temp_topic_str][1] == self.size(topic_str)[topic_str][1]:
            self.delete(topic_str)
            produced_messages_int = cp(self, temp_topic_str, self, topic_str)
            if self.size(topic_str)[topic_str][1] == self.size(temp_topic_str)[temp_topic_str][1]:
                self.delete(temp_topic_str)
        #
        return produced_messages_int
