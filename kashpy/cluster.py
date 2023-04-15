from confluent_kafka import Consumer, KafkaError, Message, OFFSET_BEGINNING, OFFSET_END, OFFSET_INVALID, OFFSET_STORED, Producer, TopicPartition
from confluent_kafka.admin import AclBinding, AclBindingFilter, AclOperation, AclPermissionType, AdminClient, ConfigResource, _ConsumerGroupTopicPartitions, _ConsumerGroupState, NewPartitions, NewTopic, ResourceType, ResourcePatternType
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer, JSONSerializer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer, ProtobufSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from google.protobuf.json_format import MessageToDict, ParseDict
from piny import YamlLoader
from fnmatch import fnmatch
import glob
#import hashlib
import importlib
import json
import os
import re
import requests
import sys
import tempfile
import time
from typing import Any, Callable, Dict, List, Union

from kashpy.kafka import Kafka

# Constants

ALL_MESSAGES = -1
RD_KAFKA_PARTITION_UA = -1
CURRENT_TIME = 0

# Helpers

def is_interactive():
    return hasattr(sys, 'ps1')


def is_file(str):
    return "/" in str


def pretty(dict):
    return json.dumps(dict, indent=2)


def ppretty(dict):
    print(pretty(dict))


# Get cluster configurations

def get_config_dict(cluster_str):
    home_str = os.environ.get("KASHPY_HOME")
    if not home_str:
        home_str = "."
    #
    cluster_path_str = f"{home_str}/clusters/{cluster_str}"
    if os.path.exists(f"{cluster_path_str}.yaml"):
        config_dict = YamlLoader(f"{cluster_path_str}.yaml").load()
    elif os.path.exists(f"{cluster_path_str}.yml"):
        config_dict = YamlLoader(f"{cluster_path_str}.yml").load()
    else:
        raise Exception(f"No cluster configuration file \"{cluster_str}.yaml\" or \"{cluster_str}.yml\" found in \"clusters\" directory (from: {home_str}; use KASHPY_HOME environment variable to set the kash.py home directory).")
    #
    if "kafka" not in config_dict:
        raise Exception(f"Cluster configuration file \"{cluster_str}.yaml\" does not include a \"kafka\" section.")
    if "schema_registry" not in config_dict:
        config_dict["schema_registry"] = {}
    if "kash" not in config_dict:
        config_dict["kash"] = {}
    #
    return config_dict

# List clusters

def clusters(pattern="*", config=False):
    """List cluster configuration files.

    List cluster configuration files matching a bash-like pattern.

    Args:
        pattern (:obj:`str`, optional): Pattern to select the cluster configuration files to match. Defaults to "*".
        config (:obj:`bool`, optional): Return configurations inside the configuration files as well. Defaults to False.

    Returns:
        :obj:`list(str)` | :obj:`dict(str, dict(str, dict(str, str)))`: List of cluster configuration file names (excluding the "*.yaml" or "*.yml" suffix) if config==False, or dictionary mapping configuration file names (excluding the "*.yaml" or "*.yml" suffix) to dictionaries mapping configuration group names ("kafka", "schema_registry", "kash") to dictionaries mapping configuration keys (e.g. "bootstrap.servers") to configuration values (e.g. "localhost:9092") if config==True.

    Examples:
        List all cluster configuration files::

            clusters()

        List only those cluster configuration files whose names start with "rp"::

            clusters("rp*")

        List only those cluster configuration files whose names start with "local" and return the configurations inside the configuration files as well::

            clusters("local*", config=True)
    """
    pattern_str = pattern
    #
    home_str = os.environ.get("KASHPY_HOME")
    if not home_str:
        home_str = "."
    #
    cluster_str_list = []
    clusters_path_str = f"{home_str}/clusters"
    yaml_cluster_path_str_list = glob.glob(f"{clusters_path_str}/{pattern_str}.yaml")
    yml_cluster_path_str_list = glob.glob(f"{clusters_path_str}/{pattern_str}.yml")
    #
    yaml_cluster_str_list = [re.search(".*/(.*)\.yaml", yaml_cluster_path_str).group(1) for yaml_cluster_path_str in yaml_cluster_path_str_list if re.search(".*/(.*)\.yaml", yaml_cluster_path_str) is not None]
    yml_cluster_str_list = [re.search(".*/(.*)\.yml", yml_cluster_path_str).group(1) for yml_cluster_path_str in yml_cluster_path_str_list if re.search(".*/(.*)\.yml", yml_cluster_path_str) is not None]
    #
    cluster_str_list = yaml_cluster_str_list + yml_cluster_str_list
    #
    if config:
        cluster_str_config_dict_dict = {cluster_str: get_config_dict(cluster_str) for cluster_str in cluster_str_list}
        return cluster_str_config_dict_dict
    else:
        cluster_str_list.sort()
        return cluster_str_list

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


def consumerGroupState_to_str(consumerGroupState):
    if consumerGroupState == _ConsumerGroupState.UNKOWN:
        return "unknown"
    elif consumerGroupState == _ConsumerGroupState.PREPARING_REBALANCING:
        return "preparing_rebalancing"
    elif consumerGroupState == _ConsumerGroupState.COMPLETING_REBALANCING:
        return "completing_rebalancing"
    elif consumerGroupState == _ConsumerGroupState.STABLE:
        return "stable"
    elif consumerGroupState == _ConsumerGroupState.DEAD:
        return "dead"
    elif consumerGroupState == _ConsumerGroupState.EMPTY:
        return "empty"


all_consumerGroupState_str_list = ["unknown", "preparing_rebalancing", "completing_rebalancing", "stable", "dead", "empty"]


def str_to_consumerGroupState(consumerGroupState_str):
    if consumerGroupState_str == "unknown":
        return _ConsumerGroupState.UNKOWN
    elif consumerGroupState_str == "preparing_rebalancing":
        return _ConsumerGroupState.PREPARING_REBALANCING 
    elif consumerGroupState_str == "completing_rebalancing":
        return _ConsumerGroupState.COMPLETING_REBALANCING
    elif consumerGroupState_str == "stable":
        return _ConsumerGroupState.STABLE
    elif consumerGroupState_str == "dead":
        return _ConsumerGroupState.DEAD
    elif consumerGroupState_str == "empty":
        return _ConsumerGroupState.EMPTY


def memberAssignment_to_dict(memberAssignment):
    dict = {"topic_partitions": [topicPartition_to_dict(topicPartition) for topicPartition in memberAssignment.topic_partitions]}
    return dict


def memberDescription_to_dict(memberDescription):
    dict = {"member_id": memberDescription.member_id,
            "client_id": memberDescription.client_id,
            "host": memberDescription.host,
            "assignment": memberAssignment_to_dict(memberDescription.assignment),
            "group_instance_id": memberDescription.group_instance_id}
    return dict


def consumerGroupDescription_to_group_description_dict(consumerGroupDescription):
    group_description_dict = {"group_id": consumerGroupDescription.group_id,
                              "is_simple_consumer_group": consumerGroupDescription.is_simple_consumer_group,
                              "members": [memberDescription_to_dict(memberDescription) for memberDescription in consumerGroupDescription.members],
                              "partition_assignor": consumerGroupDescription.partition_assignor,
                              "state": consumerGroupState_to_str(consumerGroupDescription.state),
                              "coordinator": node_to_dict(consumerGroupDescription.coordinator)}
    return group_description_dict


def topicPartition_to_dict(topicPartition):
    dict = {"error": kafkaError_to_error_dict(topicPartition.error),
            "metadata": topicPartition.metadata,
            "offset": topicPartition.offset,
            "partition": topicPartition.partition,
            "topic": topicPartition.topic}
    return dict


def topicPartition_list_to_offsets_dict(topicPartition_list):
    offsets_dict = {}
    for topicPartition in topicPartition_list:
        topic_str = topicPartition.topic
        partition_int = topicPartition.partition
        offset_int = topicPartition.offset
        #
        if topic_str in offsets_dict:
            offsets = offsets_dict[topic_str]
        else:
            offsets = {}
        offsets[partition_int] = offset_int
        offsets_dict[topic_str] = offsets
    #
    return offsets_dict


def node_to_dict(node):
    dict = {"id": node.id,
            "id_string": node.id_string,
            "host": node.host,
            "port": node.port,
            "rack": node.rack}
    return dict

# group_offsets =TA group_str_topic_str_partition_int_offset_int_dict_dict_dict
# topic_offsets =TA topic_str_partition_int_offset_int_dict_dict
# (partition_)offsets =TA partition_int_offset_int_dict
def group_str_consumerGroupTopicPartitions_future_dict_to_consumerGroupTopicPartitions_list(group_str_consumerGroupTopicPartitions_future_dict):
    consumerGroupTopicPartitions_list = [consumerGroupTopicPartitions_future.result() for consumerGroupTopicPartitions_future in group_str_consumerGroupTopicPartitions_future_dict.values()]
    #
    return consumerGroupTopicPartitions_list


def consumerGroupTopicPartitions_list_to_group_offsets(consumerGroupTopicPartitions_list):
    group_str_topic_str_partition_int_offset_int_dict_dict_dict = {}
    for consumerGroupTopicPartitions in consumerGroupTopicPartitions_list:
        group_str = consumerGroupTopicPartitions.group_id
        topic_str_partition_int_offset_int_dict_dict = consumerGroupTopicPartitions_to_topic_offsets(consumerGroupTopicPartitions)
        group_str_topic_str_partition_int_offset_int_dict_dict_dict[group_str] = topic_str_partition_int_offset_int_dict_dict
    #
    return group_str_topic_str_partition_int_offset_int_dict_dict_dict


def consumerGroupTopicPartitions_to_topic_offsets(consumerGroupTopicPartitions):
    topic_str_partition_int_offset_int_tuple_list_dict = {}
    for topicPartition in consumerGroupTopicPartitions.topic_partitions:
        topic_str = topicPartition.topic
        partition_int_offset_int_tuple = (topicPartition.partition, topicPartition.offset)
        if topic_str in topic_str_partition_int_offset_int_tuple_list_dict:
            topic_str_partition_int_offset_int_tuple_list_dict[topic_str].append(partition_int_offset_int_tuple)
        else:
            topic_str_partition_int_offset_int_tuple_list_dict[topic_str] = [(topicPartition.partition, topicPartition.offset)]
    #
    topic_str_partition_int_offset_int_dict_dict = {topic_str: {partition_int_offset_int_tuple[0]: partition_int_offset_int_tuple[1] for partition_int_offset_int_tuple in partition_int_offset_int_tuple_list} for topic_str, partition_int_offset_int_tuple_list in topic_str_partition_int_offset_int_tuple_list_dict.items()}
    #
    return topic_str_partition_int_offset_int_dict_dict

def group_str_consumerGroupTopicPartitions_future_dict_to_group_offsets(group_str_consumerGroupTopicPartitions_future_dict):
    consumerGroupTopicPartitions_list = group_str_consumerGroupTopicPartitions_future_dict_to_consumerGroupTopicPartitions_list(group_str_consumerGroupTopicPartitions_future_dict)
    #
    group_offsets = consumerGroupTopicPartitions_list_to_group_offsets(consumerGroupTopicPartitions_list)
    #
    return group_offsets

def group_offsets_to_consumerGroupTopicPartitions_list(group_offsets):
    consumerGroupTopicPartitions_list = []
    for group_str, topic_offsets in group_offsets.items():
        topicPartition_list = []
        for topic_str, partition_offsets in topic_offsets.items():
            for partition_int, offset_int in partition_offsets.items():
                topicPartition_list.append(TopicPartition(topic_str, partition_int, offset_int))
        consumerGroupTopicPartitions_list.append(_ConsumerGroupTopicPartitions(group_str, topicPartition_list))
    return consumerGroupTopicPartitions_list

# Cluster class

class Cluster(Kafka):
    """Initialize a kash.py Cluster object.

    Initialize a kash.py Cluster object based on a kash.py cluster configuration file.

    kash.py cluster configuration files are searched for in the directory "cluster" starting 1) from the directory in the KASHPY_HOME environment variable, or, if that environment variable is not set, 2) from the current directory.

    kash.py cluster configuration files have up to three sections: "kafka", "schema_registry", and "kash".

    The "kafka" and "schema_registry" sections configure the Kafka cluster and Schema Registry according to the confluent_kafka/librdkafka configuration.

    The "kash" section can be used to configure the following:

    * AdminClient (currently only applies to ``create()``):

      * ``retention.ms``: Retention time (in milliseconds) for creating topics. Defaults to 604800000 (seven days). You can set it to -1 to create topics with infinite retention by default.

    * Producer (applies to all functions/methods producing messages to a cluster):

      * ``flush.num.messages``: Number of messages produced before calling ``confluent_kafka.Producer.flush()``. Defaults to 10000.
      * ``flush.timeout``: Timeout (in seconds) for calling ``confluent_kafka.Producer.flush()``. Defaults to -1 (no timeout).

    * Consumer (applies to all functions/methods consuming messages from a cluster):

      * ``consume.timeout``: Timeout (in seconds) for calling ``confluent_kafka.Consumer.consume()``. Lower (e.g. 1.0) for local or very fast Kafka clusters, higher for remote/not-so-fast Kafka clusters (e.g. 10.0). Defaults to 3.0.
      * ``auto.offset.reset``: Either "earliest" or "latest". Directly translates to the confluent_kafka/librdkafka consumer configuration. Defaults to "earliest".
      * ``enable.auto.commit``: Either "True" or "False". Directly translates to the confluent_kafka/librdkafka consumer configuration. Defaults to "True".
      * ``session.timeout.ms``: Timeout (in milliseconds) to detect client failures. Defaults to 45000.

    * Progress display:

      * ``progress.num.messages``: Number of messages to be consumed/produced until a status print out to standard out/the console is triggered (if verbosity level > 0). Defaults to 1000.

    * Blocking (applies to ``create()`` and ``delete()``/``rm()``)

      * ``block.num.retries.int``: The number of retries when blocking to wait for topics to have been created/deleted (if ``block`` is set to True). Defaults to 50.
      * ``block.interval``: Time (in seconds) between retries when blocking to wait for topics to have been created/deleted (if ``block`` is set to True). Defaults to 0.1.

    Args:
        cluster_str (:obj:`str`): Name of the cluster; kash.py searches the folder "clusters" for kash.py configuration files named "<cluster_str>.yaml".

    Examples:

        Simplest kash.py configuration file example (just "bootstrap.servers" is set)::

            kafka:
              bootstrap.servers: localhost:9092

        Simple kash.py configuration file example with additional Schema Registry URL::

            kafka:
              bootstrap.servers: localhost:9092

            schema_registry:
              schema.registry.url: http://localhost:8081

        kash.py configuration file with all bells and whistles - all that can currently be configured::

            kafka:
              bootstrap.servers: localhost:9092
            
            schema_registry:
              schema.registry.url: http://localhost:8081

            kash:
              flush.num.messages: 10000
              flush.timeout: -1.0
              retention.ms: -1
              consume.timeout: 1.0
              auto.offset.reset: earliest
              enable.auto.commit: true
              session.timeout.ms: 10000
              progress.num.messages: 1000
              block.num.retries.int: 50
              block.interval: 0.1

        kash.py configuration file for a typical Confluent Cloud cluster (including Schema Registry). Note the use of (Piny-powered) environment variable interpolation to bring in security-relevant information such as the cluster name, user name and password::

            kafka:
              bootstrap.servers: ${CLUSTER}.confluent.cloud:9092
              security.protocol: SASL_SSL
              sasl.mechanisms: PLAIN
              sasl.username: ${CLUSTER_USERNAME}
              sasl.password: ${CLUSTER_PASSWORD}

            schema_registry:
              schema.registry.url: https://${SCHEMA_REGISTRY}.confluent.cloud
              basic.auth.credentials.source: USER_INFO
              basic.auth.user.info: ${SCHEMA_REGISTRY_USERNAME}:${SCHEMA_REGISTRY_PASSWORD}

            kash:
              flush.num.messages: 10000
              flush.timeout: -1.0
              retention.ms: -1
              consume.timeout: 10.0
              auto.offset.reset: earliest
              enable.auto.commit: true
              session.timeout.ms: 10000
              progress.num.messages: 1000
              block.num.retries.int: 50
              block.interval: 0.1

        kash.py configuration file for a self-hosted Redpanda cluster (without Schema Registry)::

            kafka:
              bootstrap.servers: ${CLUSTER}:9094
              security.protocol: sasl_plaintext
              sasl.mechanisms: SCRAM-SHA-256
              sasl.username: ${CLUSTER_USERNAME}
              sasl.password: ${CLUSTER_PASSWORD}

            kash:
              flush.num.messages: 10000
              flush.timeout: -1.0
              retention.ms: -1
              consume.timeout: 5.0
              auto.offset.reset: earliest
              enable.auto.commit: true
              session.timeout.ms: 10000
              progress.num.messages: 1000
              block.num.retries.int: 50
              block.interval: 0.1
    """
    def __init__(self, cluster_str):
        self.cluster_str = cluster_str
        self.config_dict = get_config_dict(cluster_str)
        self.schema_registry_config_dict = self.config_dict["schema_registry"]
        self.kash_dict = self.config_dict["kash"]
        self.config_dict = self.config_dict["kafka"]
        #
        self.adminClient = get_adminClient(self.config_dict)
        #
        self.producer = get_producer(self.config_dict)
        #
        if self.schema_registry_config_dict:
            self.schemaRegistryClient = get_schemaRegistryClient(self.schema_registry_config_dict)
        else:
            self.schemaRegistryClient = None
        # subscription_dict: subscription_id_str -> dict(topics: list(str), group: str, consumer: Consumer, key_type: str, value_type: str, key_schema: str, value_schema: str)
        # where uuid_str identifies the subscription.
        self.subscription_dict = {}
        self.subscription_id_counter_int = 0
        #
        self.schema_id_int_generalizedProtocolMessageType_protobuf_schema_str_tuple_dict = {}
        #
        self.produced_messages_counter_int = 0
        #
        self.verbose_int = 1 if is_interactive() else 0
        #
        self.topic_str = None
        #
        # Kash cluster config
        #
        # Admin Client
        if "retention.ms" not in self.kash_dict:
            self.retention_ms(604800000)
        else:
            self.retention_ms(int(self.kash_dict["retention.ms"]))
        # Producer
        if "flush.num.messages" not in self.kash_dict:
            self.flush_num_messages(10000)
        else:
            self.flush_num_messages(int(self.kash_dict["flush.num.messages"]))
        if "flush.timeout" not in self.kash_dict:
            self.flush_timeout(-1.0)
        else:
            self.flush_timeout(float(self.kash_dict["flush.timeout"]))
        # Consumer
        if "consume.timeout" not in self.kash_dict:
            self.consume_timeout(3.0)
        else:
            self.consume_timeout(float(self.kash_dict["consume.timeout"]))
        #
        if "auto.offset.reset" not in self.kash_dict:
            self.auto_offset_reset("earliest")
        else:
            self.auto_offset_reset(str(self.kash_dict["auto.offset.reset"]))
        if "enable.auto.commit" not in self.kash_dict:
            self.enable_auto_commit(True)
        else:
            self.enable_auto_commit(bool(self.kash_dict["enable.auto.commit"]))
        if "session.timeout.ms" not in self.kash_dict:
            self.session_timeout_ms(45000)
        else:
            self.session_timeout_ms(int(self.kash_dict["session.timeout.ms"]))
        # Standard output
        if "progress.num.messages" not in self.kash_dict:
            self.progress_num_messages(1000)
        else:
            self.progress_num_messages(int(self.kash_dict["progress.num.messages"]))
        # Block
        if "block.num.retries" not in self.kash_dict:
            self.block_num_retries(50)
        else:
            self.block_num_retries(int(self.kash_dict["block.num.retries"]))
        if "block.interval" not in self.kash_dict:
            self.block_interval(0.1)
        else:
            self.block_interval(float(self.kash_dict["block.interval"]))

    #

    def retention_ms(self, new_value_int=None):
        """Get/set the retention.ms kash setting.

            Args:
                new_value_int (:obj:`int`, optional): New value. Defaults to None (=just get, do not set).

            Returns:
                :obj:`int`: The retention.ms kash setting.
        """
        if new_value_int is not None:
            self.kash_dict["retention.ms"] = new_value_int
        return self.kash_dict["retention.ms"]

    def flush_num_messages(self, new_value_int=None):
        """Get/set the flush.num.messages kash setting.

            Args:
                new_value_int (:obj:`int`, optional): New value. Defaults to None (=just get, do not set).

            Returns:
                :obj:`int`: The flush.num.messages kash setting.
        """
        if new_value_int is not None:
            self.kash_dict["flush.num.messages"] = new_value_int
        return self.kash_dict["flush.num.messages"]

    def flush_timeout(self, new_value_float=None):
        """Get/set the flush.timeout kash setting.

            Args:
                new_value_float (:obj:`float`, optional): New value. Defaults to None (=just get, do not set).

            Returns:
                :obj:`float`: The flush.timeout kash setting.
        """
        if new_value_float is not None:
            self.kash_dict["flush.timeout"] = new_value_float
        return self.kash_dict["flush.timeout"]

    def consume_timeout(self, new_value_float=None):
        """Get/set the consume.timeout kash setting.

            Args:
                new_value_float (:obj:`float`, optional): New value. Defaults to None (=just get, do not set).

            Returns:
                :obj:`float`: The consume.timeout kash setting.
        """
        if new_value_float is not None:
            self.kash_dict["consume.timeout"] = new_value_float
        return self.kash_dict["consume.timeout"]

    def auto_offset_reset(self, new_value_str=None):
        """Get/set the auto.offset.reset kash setting.

            Args:
                new_value_str (:obj:`str`, optional): New value. Defaults to None (=just get, do not set).

            Returns:
                :obj:`str`: The auto.offset.reset kash setting.
        """
        if new_value_str is not None:
            self.kash_dict["auto.offset.reset"] = new_value_str
        return self.kash_dict["auto.offset.reset"]

    def enable_auto_commit(self, new_value_bool=None):
        """Get/set the enable.auto.commit kash setting.

            Args:
                new_value_bool (:obj:`bool`, optional): New value. Defaults to None (=just get, do not set).

            Returns:
                :obj:`bool`: The enable.auto.commit kash setting.
        """
        if new_value_bool is not None:
            self.kash_dict["enable.auto.commit"] = new_value_bool
        return self.kash_dict["enable.auto.commit"]

    def session_timeout_ms(self, new_value_int=None):
        """Get/set the session.timeout.ms kash setting.

            Args:
                new_value_int (:obj:`int`, optional): New value. Defaults to None (=just get, do not set).

            Returns:
                :obj:`int`: The session.timeout.ms kash setting.
        """
        if new_value_int is not None:
            self.kash_dict["session.timeout.ms"] = new_value_int
        return self.kash_dict["session.timeout.ms"]

    def progress_num_messages(self, new_value_int=None):
        """Get/set the progress.num.messages kash setting.

            Args:
                new_value_int (:obj:`int`, optional): New value. Defaults to None (=just get, do not set).

            Returns:
                :obj:`int`: The progress.num.messages kash setting.
        """
        if new_value_int is not None:
            self.kash_dict["progress.num.messages"] = new_value_int
        return self.kash_dict["progress.num.messages"]

    def block_num_retries(self, new_value_int=None):
        """Get/set the block.num.retries kash setting.

            Args:
                new_value_int (:obj:`int`, optional): New value. Defaults to None (=just get, do not set).

            Returns:
                :obj:`int`: The block.num.retries kash setting.
        """
        if new_value_int is not None:
            self.kash_dict["block.num.retries"] = new_value_int
        return self.kash_dict["block.num.retries"]

    def block_interval(self, new_value_float=None):
        """Get/set the block.interval kash setting.

            Args:
                new_value_float (:obj:`float`, optional): New value. Defaults to None (=just get, do not set).

            Returns:
                :obj:`float`: The block.interval kash setting.
        """
        if new_value_float is not None:
            self.kash_dict["block.interval"] = new_value_float
        return self.kash_dict["block.interval"]

    #

    def verbose(self, new_value_int=None):
        """Get/set the verbosity level.

            Args:
                new_value_int (:obj:`int`, optional): New value. Defaults to None.

            Returns:
                :obj:`int`: The verbosity level.
        """
        if new_value_int is not None:
            self.verbose_int = new_value_int
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
        # No additional caching necessary here:
        # get_schema(schema_id)[source]
        # Fetches the schema associated with schema_id from the Schema Registry. The result is cached so subsequent attempts will not require an additional round-trip to the Schema Registry.
        schema = self.schemaRegistryClient.get_schema(schema_id_int)
        schema_str = schema.schema_str
        #
        generalizedProtocolMessageType = self.schema_id_int_and_schema_str_to_generalizedProtocolMessageType(schema_id_int, schema_str)
        return generalizedProtocolMessageType, schema_str

    def schema_id_int_to_schema_str(self, schema_id_int):
        # No additional caching necessary here:
        # get_schema(schema_id)[source]
        # Fetches the schema associated with schema_id from the Schema Registry. The result is cached so subsequent attempts will not require an additional round-trip to the Schema Registry.
        schema = self.schemaRegistryClient.get_schema(schema_id_int)
        schema_str = schema.schema_str
        #
        return schema_str

    def schema_id_int_and_schema_str_to_generalizedProtocolMessageType(self, schema_id_int, schema_str):
        path_str = f"/{tempfile.gettempdir()}/kash.py/clusters/{self.cluster_str}"
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
        avro_schema_str = self.schema_id_int_to_schema_str(schema_id_int)
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
        jsonschema_str = self.schema_id_int_to_schema_str(schema_id_int)
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
        if key_type_str.lower() == "str":
            decode_key = bytes_to_str
        elif key_type_str.lower() == "bytes":
            decode_key = bytes_to_bytes
        elif key_type_str.lower() == "json":
            decode_key = json.loads
        elif key_type_str.lower() in ["pb", "protobuf"]:
            def decode_key(bytes):
                return self.bytes_protobuf_to_dict(bytes, key_bool=True)
        elif key_type_str.lower() == "avro":
            def decode_key(bytes):
                return self.bytes_avro_to_dict(bytes, key_bool=True)
        elif key_type_str.lower() == "jsonschema":
            def decode_key(bytes):
                return self.bytes_jsonschema_to_dict(bytes, key_bool=True)
        #
        if value_type_str.lower() == "str":
            decode_value = bytes_to_str
        elif value_type_str.lower() == "bytes":
            decode_value = bytes_to_bytes
        elif value_type_str.lower() == "json":
            decode_value = json.loads
        elif value_type_str.lower() in ["pb", "protobuf"]:
            def decode_value(bytes):
                return self.bytes_protobuf_to_dict(bytes, key_bool=False)
        elif value_type_str.lower() == "avro":
            def decode_value(bytes):
                return self.bytes_avro_to_dict(bytes, key_bool=False)
        elif value_type_str.lower() == "jsonschema":
            def decode_value(bytes):
                return self.bytes_jsonschema_to_dict(bytes, key_bool=False)
        #
        message_dict = {"headers": message.headers(), "topic": message.topic(), "partition": message.partition(), "offset": message.offset(), "timestamp": message.timestamp(), "key": decode_key(message.key()), "value": decode_value(message.value())}
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

    def watermarks(self, pattern_str_or_str_list, timeout=-1.0):
        """Get low and high offsets (=so called "watermarks") of topics on the cluster.

        Returns a dictionary of the topics whose names match the bash-like pattern pattern_str and the low and high offsets of their partitions.

        Args:
            pattern_str_or_str_list (:obj:`str` | :obj:`list(str)`): The pattern or list of patterns for selecting the topics.
            timeout (:obj:`float`, optional): The timeout (in seconds) for the individual get_watermark_offsets() method calls from confluent_kafka.Consumer. Defaults to -1.0 (infinite=no timeout).

        Returns:
            :obj:`dict(str, dict(int, tuple(int, int)))`: Dictionary of strings (topic names) and dictionaries of integers (partition) and pairs of integers (low and high offsets of the respective partition of the respective topic)

        Examples:
            Return the watermarks for all topics whose name starts with "test" and their partitions::

                c.watermarks("test*")

            Return the watermarks for the topics "test" and "bla" and time out the internally used get_watermark_offsets() method after one second::

                c.watermarks(["test", "bla"], timeout=1.0)
        """
        timeout_float = timeout
        #
        config_dict = self.config_dict
        config_dict["group.id"] = "dummy_group_id"
        consumer = get_consumer(config_dict)
        #
        topic_str_list = self.topics(pattern_str_or_str_list)
        topic_str_partition_int_offsets_tuple_dict_dict = {}
        for topic_str in topic_str_list:
            partitions_int = self.partitions(topic_str)[topic_str]
            partition_int_offsets_tuple_dict = {partition_int: consumer.get_watermark_offsets(TopicPartition(topic_str, partition=partition_int), timeout_float) for partition_int in range(partitions_int)}
            topic_str_partition_int_offsets_tuple_dict_dict[topic_str] = partition_int_offsets_tuple_dict
        return topic_str_partition_int_offsets_tuple_dict_dict

    def list_topics(self):
        topic_str_list = list(self.adminClient.list_topics().topics.keys())
        return topic_str_list

    def config(self, pattern_str_or_str_list):
        """Return the configuration of topics.

        Return the configuration of those topics whose names match the bash-like pattern (or list of patterns) pattern_str_or_str_list.

        Args:
            pattern_str_or_str_list (:obj:`str` | :obj:`list(str)`): The pattern (or list of patterns) for selecting the topics for which the configuration shall be returned.

        Returns:
            dict(str, dict(str, str)): Dictionary of strings (topic names) and dictionaries of strings (configuration keys) and strings (configuration values).

        Examples:
            Return the configuration of the topic "test"::

                c.config("test")

            Return the configuration of all topics matching the patterns "test?" or "bla?"::

                c.config(["test?", "bla?"])
        """
        topic_str_list = self.topics(pattern_str_or_str_list)
        #
        topic_str_config_dict_dict = {topic_str: self.get_config_dict(ResourceType.TOPIC, topic_str) for topic_str in topic_str_list}
        #
        return topic_str_config_dict_dict

    def set_config(self, pattern_str_or_str_list, config_dict, test=False):
        """Set a configuration item of topics.

        Set the configuration item with key key_str and value value_str of those topics whose names match the bash-like pattern (or list of patterns) pattern_str_or_str_list.

        Args:
            pattern_str_or_str_list (:obj:`str` | :obj:`list(str)`): The pattern (or list of patterns) for selecting those topics whose configuration shall be changed.
            test (:obj:`bool`, optional): If True, the request is only validated without changing the configuration. Defaults to False.

        Returns:
            :obj:`dict(str, tuple(str, str))`: Dictionary of strings (topic names) and tuples of strings (configuration keys) and strings (configuration values)

        Examples:
            Sets the configuration key "retention.ms" to configuration value "4711" for topic "test"::

                c.set_config("test", "retention.ms", "4711")

            Verifies if the configuration key "retention.ms" can be set to configuration value "4711" for topic "test", but does not change the configuration::

                c.set_config("test", "retention.ms", "4711", test=True)

            Sets the configuration key "retention.ms" to configuration value "42" for all topics whose names start with "test" or "bla"::

                c.set_config(["test*", "bla*"], "42")
        """
        test_bool = test
        #
        topic_str_list = self.topics(pattern_str_or_str_list)
        #
        for topic_str in topic_str_list:
            self.set_config_dict(ResourceType.TOPIC, topic_str, config_dict, test_bool)
        #
        topic_str_key_str_value_str_tuple_dict = {topic_str: config_dict for topic_str in topic_str_list}
        return topic_str_key_str_value_str_tuple_dict

    def block_topic(self, topic_str, exists=True):
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
            if num_retries_int >= self.kash_dict["block.num.retries"]:
                break
            time.sleep(self.kash_dict["block.interval"])
        return False

    def create(self, topic_str, partitions=1, config={}, block=True):
        """Create a topic.

        Create a topic.

        Args:
            topic_str (:obj:`str`): The name of the topic to be created.
            partitions (:obj:`int`, optional): The number of partitions for the topic to be created. Defaults to 1.
            config (:obj:`dict(str, str)`, optional): Configuration overrides for the topic to be created. Note that the default "retention.ms" can also be set in the kash.py cluster configuration file (e.g. you can set it to -1 to have infinite retention for all topics that you create). Defaults to {}.
            block (:obj:`bool`, optional): Block until the topic is created. Defaults to True.

        Returns:
            :obj:`str`: Name of the created topic.

        Examples:
            Create the topic "test" with one partition, and block until it is created::

                c.create("test")

            Create the topic "test" with two partitions, and block until it is created::

                c.create("test", partitions=2)

            Create the topic "test" with one partition, a retention time of 4711ms and block until it is created::

                c.create("test", config={"retention.ms": "4711"})

            Create the topic "test" with one partition, and *do not* block until it is created::

                c.create("test", block=False)
        """
        partitions_int = partitions
        config_dict = config
        block_bool = block
        #
        config_dict["retention.ms"] = self.kash_dict["retention.ms"]
        #
        newTopic = NewTopic(topic_str, partitions_int, config=config_dict)
        self.adminClient.create_topics([newTopic])
        #
        if block_bool:
            self.block_topic(topic_str, exists=True)
        #
        return topic_str

    touch = create
    """Create a topic.

    Create a topic (shell synonym for ``Cluster.create()``)

    Args:
        topic_str (:obj:`str`): The name of the topic to be created.
        partitions (:obj:`int`, optional): The number of partitions for the topic to be created. Defaults to 1.
        config (:obj:`dict(str, str)`, optional): Configuration overrides for the topic to be created. Note that the default "retention.ms" can also be set in the kash.py cluster configuration file (e.g. you can set it to -1 to have infinite retention for all topics that you create). Defaults to {}.
        block (:obj:`bool`, optional): Block until the topic is created. Defaults to True.

    Returns:
        :obj:`str`: Name of the created topic.

    Examples:
        Create the topic "test" with one partition, and block until it is created::

            c.touch("test")

        Create the topic "test" with two partitions, and block until it is created::

            c.touch("test", partitions=2)

        Create the topic "test" with one partition, a retention time of 4711ms and block until it is created::

            c.touch("test", config={"retention.ms": "4711"})

        Create the topic "test" with one partition, and *do not* block until it is created::

            c.touch("test", block=False)
    """

    def delete(self, pattern_str_or_str_list, block=True):
        """Delete topics.

        Delete those topics whose names match the bash-like pattern (or list of patterns) pattern_str_or_str_list.

        Args:
            pattern_str_or_str_list (:obj:`str` | :obj:`list(str)`): The pattern (or list of patterns) for selecting the topics to be deleted.
            block (:obj:`bool`, optional): Block until the topic is deleted. Defaults to True.

        Returns:
            :obj:`list(str)`: List of strings of names of the deleted topics.

        Examples:
            Delete the topic "test" and block until it is deleted::

                c.delete("test")

            Delete the topic "test" and *do not* block until it is deleted::

                c.delete("test", block=False)

            Delete all topics starting with "test" or "bla"::

                c.delete(["test*", "bla*"])
        """
        block_bool = block
        #
        topic_str_list = self.topics(pattern_str_or_str_list)
        #
        if topic_str_list:
            self.adminClient.delete_topics(topic_str_list)
            if block_bool:
                for topic_str in topic_str_list:
                    self.block_topic(topic_str, exists=False)
        #
        return topic_str_list

    rm = delete
    """Delete topics.

    Delete those topics whose names match the bash-like pattern (or list of patterns) pattern_str_or_str_list (shell synonym for ``Cluster.delete()``).

    Args:
        pattern_str_or_str_list (:obj:`str` | :obj:`list(str)`): The pattern (or list of patterns) for selecting the topics to be deleted.
        block (:obj:`bool`, optional): Block until the topic is deleted. Defaults to True.

    Returns:
        obj:`list(str)`: List of strings of names of the deleted topics.

    Examples:
        Delete the topic "test" and block until it is deleted::

            c.rm("test")

        Delete the topic "test" and *do not* block until it is deleted::

            c.rm("test", block=False)

        Delete all topics starting with "test" or "bla"::

            c.rm(["test*", "bla*"])
    """

    def offsets_for_times(self, pattern_str_or_str_list, partition_int_timestamp_int_dict, timeout=-1.0):
        """Look up offsets corresponding to message timestamps in the partitions of topics.

        Look up those offsets in the individual partitions of all topics matching the bash-like pattern (or list of patterns) pattern_str_or_str_list which correspond to the timestamps provided in partition_int_timestamp_int_dict (for the individual partitions).

        Args:
            pattern_str_or_str_list (:obj:`str` | :obj:`list(str)`): The pattern (or list of patterns) for selecting the topics.
            partition_int_timestamp_int_dict (:obj:`dict(int, int)`): Dictionary of integers (partitions) and integers (timestamps).
            timeout (:obj:`float`, optional): The timeout (in seconds) for the individual offsets_for_times() method calls from confluent_kafka.Consumer. Defaults to -1.0 (infinite=no timeout).

        Returns:
            :obj:`dict(str, dict(int, int))`: Dictionary of strings (topic names) and dictionaries of integers (partitions) and integers (offsets).

        Examples:
            Look up the offset of the first message in the first partition of the topic "test" which has a timestamp greater or equal to 1664644769886 milliseconds from epoch. If the provided timestamp exceeds that of the last message in the partition, a value of -1 will be returned::

                c.offsets_for_times("test", {0: 1664644769886})

            Look up the offset of the first message in the first partition of those topics starting with "te" and ending with "st" with a timestamp greater or equal to 1664644769886 milliseconds from epoch; and look up the offset of the first message in the second partition of those topics with a timestamp greater or equal to 1664645155987 milliseconds from epoch. Time out the internally used offsets_for_times() calls after one second::

                c.offsets_for_times("te*st", {0: 1664644769886, 1: 1664645155987}, timeout=1.0)
        """
        topic_str_list = self.topics(pattern_str_or_str_list)
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

    def describe(self, pattern_str_or_str_list):
        """Describe topics.

        Describe all topics matching the bash-like pattern (or list of patterns) pattern_str_or_str_list.

        Args:
            pattern_str_or_str_list (:obj:`str` | :obj:`list(str)`): The pattern (or list of patterns) for selecting the topics.

        Returns:
            :obj:`dict(str, topic_dict)`: Dictionary of strings (topic names) and topic dictionaries describing the topic (converted from confluent_kafka.TopicMetadata objects).

        Examples:
            Describe the topic "test"::

                c.describe("test")

            Describe all topics whose names start with "test" or "bla"::

                c.describe(["test*", "bla*"])
        """
        if isinstance(pattern_str_or_str_list, str):
            pattern_str_or_str_list = [pattern_str_or_str_list]
        #
        topic_str_topicMetadata_dict = self.adminClient.list_topics().topics
        #
        topic_str_topic_dict_dict = {topic_str: topicMetadata_to_topic_dict(topic_str_topicMetadata_dict[topic_str]) for topic_str in topic_str_topicMetadata_dict if any(fnmatch(topic_str, pattern_str) for pattern_str in pattern_str_or_str_list)}
        #
        return topic_str_topic_dict_dict

    def exists(self, topic_str):
        """Test whether a topic exists on the cluster.

        Test whether a topic exists on the cluster.

        Args:
            topic_str (:obj:`str`): A topic.

        Returns:
            :obj:`bool`: True if the topic topic_str exists, False otherwise.

        Examples:
            Test whether the topic "test" exists on the cluster::
            
                c.exists("test")
        """
        return self.topics(topic_str) != []

    def partitions(self, pattern_str_or_str_list):
        """Get the number of partitions of topics.

        Get the number of partitions of all topics matching the bash-like pattern (or list of patterns) pattern_str_or_str_list.

        Args:
            pattern_str_or_str_list (:obj:`str` | :obj:`list(str)`): The pattern (or list of patterns) for selecting the topics.

        Returns:
            :obj:`dict(str, int)`: Dictionary of strings (topic names) and their respective numbers of partitions.

        Examples:
            Get the number of partitions of the topic "test"::

                c.partitions("test")

            Get the numbers of partitions of all topics whose names start with "test" or "bla"::

                c.partitions(["test*", "bla*"])
        """
        if isinstance(pattern_str_or_str_list, str):
            pattern_str_or_str_list = [pattern_str_or_str_list]
        #
        topic_str_topicMetadata_dict = self.adminClient.list_topics().topics
        #
        topic_str_num_partitions_int_dict = {topic_str: len(topic_str_topicMetadata_dict[topic_str].partitions) for topic_str in topic_str_topicMetadata_dict if any(fnmatch(topic_str, pattern_str) for pattern_str in pattern_str_or_str_list)}
        #
        return topic_str_num_partitions_int_dict

    def set_partitions(self, pattern_str_or_str_list, num_partitions_int, test=False):
        """Set the number of partitions of topics.

        Set the number of partitions of all topics matching the bash-like pattern (or list of patterns) pattern_str_or_str_list. The number of partitions of a topic can only be increased but not decreased, i.e., only additional partitions can be created.

        Args:
            pattern_str_or_str_list (:obj:`str` | :obj:`list(str)`): The pattern (or list of patterns) for selecting the topics.
            num_partitions_int (:obj:`int`): The number of partitions to set for the selected topics. The number of partitions of a topic can only be increased but not decreased, i.e., only additional partitions can be created.
            test (:obj:`bool`, optional): If True, the request is only validated without creating the partitions.

        Returns:
            :obj:`dict(str, int)`: Dictionary of strings (topic names) and their respective new numbers of partitions.

        Examples:
            Set the number of partitions for the topic "test" to 2::

                c.set_partitions("test", 2)

            Set the numbers of partitions of all topics whose names start with "test" or "bla" to 4::

                c.set_partitions(["test*", "bla*"], 4)
        """
        test_bool = test
        #
        topic_str_list = self.topics(pattern_str_or_str_list)
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

    def groups(self, patterns="*", state_patterns="*", state=False):
        """List consumer groups.

        List consumer groups. Optionally return only those consumer groups whose group IDs match bash-like patterns.

        Args:
            patterns (:obj:`str` | :obj:`list(str)`, optional): The pattern or list of patterns for selecting the group IDs of those consumer groups which shall be listed. Defaults to "*".
            state_patterns (:obj:`str` | :obj:`list(str)`, optional): The pattern or list of patterns for selecting the consumer group states ("unknown", "preparing_rebalancing", "completing_rebalancing", "stable", "dead", "empty") to list. Defaults to "*" (all consumer group states).
            state (:obj:`bool`, optional): Return the consumer group states. Defaults to False.

        Returns:
            :obj:`list(str)`) | :obj:`dict(str, str)`): List of strings (group IDs) or dictionary of pairs of strings of group IDs and consumer group states.

        Examples:
            List all consumer groups of the cluster::

                c.groups()

            List all those consumer groups whose group ID starts with "test"::

                c.groups("test*")

            List all those consumer groups whose group ID starts with "test" or "bla"::

                c.groups(["test*", "bla*"])

            List all those consumer groups and their respective states whose group ID starts with "test" or "bla" and whose state is either "unknown", "preparing_rebalancing" or "completing_rebalancing"::

                c.groups(["test*", "bla*"], ["unknown", "*_rebalancing"], state=True)
        """
        pattern_str_or_str_list = [patterns] if isinstance(patterns, str) else patterns
        #
        consumerGroupState_pattern_str_list = [state_patterns] if isinstance(state_patterns, str) else state_patterns
        consumerGroupState_set = set([str_to_consumerGroupState(consumerGroupState_str) for consumerGroupState_str in all_consumerGroupState_str_list if any(fnmatch(consumerGroupState_str, consumerGroupState_pattern_str) for consumerGroupState_pattern_str in consumerGroupState_pattern_str_list)])
        if not consumerGroupState_set:
            return {} if state else []
        #
        listConsumerGroupsResult = self.adminClient.list_consumer_groups(states=consumerGroupState_set).result()
        consumerGroupListing_list = listConsumerGroupsResult.valid
        #
        group_str_state_str_dict = {consumerGroupListing.group_id: consumerGroupState_to_str(consumerGroupListing.state) for consumerGroupListing in consumerGroupListing_list if any(fnmatch(consumerGroupListing.group_id, pattern_str) for pattern_str in pattern_str_or_str_list)}
        #
        return group_str_state_str_dict if state else list(group_str_state_str_dict.keys())

    def describe_groups(self, patterns="*", state_patterns="*"):
        """Describe consumer groups.

        Describe consumer groups whose group IDs match bash-like patterns.

        Args:
            patterns (:obj:`str` | :obj:`list(str)`, optional): The pattern or list of patterns for selecting the group IDs of those consumer groups which shall be described. Defaults to "*".
            state_patterns (:obj:`str` | :obj:`list(str)`, optional): The pattern or list of patterns for selecting the consumer group states ("unknown", "preparing_rebalancing", "completing_rebalancing", "stable", "dead", "empty") to describe. Defaults to "*" (all consumer group states).

        Returns:
            :obj:`dict(str, group_dict)`: Dictionary of strings (group IDs) and group description dictionaries describing the consumer group.

        Examples:
            Describe all those consumer groups whose group ID starts with "test"::

                c.describe_groups("test*")

            Describe all those consumer groups whose group ID starts with "test" or "bla"::

                c.describe_groups(["test*", "bla*"])

            Describe all those consumer groups whose group ID starts with "test" or "bla" and whose state is either "unknown", "preparing_rebalancing" or "completing_rebalancing"::

                c.describe_groups(["test*", "bla*"], ["unknown", "*_rebalancing"])
        """
        group_str_list = self.groups(patterns, state_patterns)
        if not group_str_list:
            return {}
        #
        group_str_consumerGroupDescription_future_dict = self.adminClient.describe_consumer_groups(group_str_list)
        group_str_group_description_dict_dict = {group_str: consumerGroupDescription_to_group_description_dict(consumerGroupDescription_future.result()) for group_str, consumerGroupDescription_future in group_str_consumerGroupDescription_future_dict.items()}
        #
        return group_str_group_description_dict_dict

    def delete_groups(self, patterns, state_patterns="*"):
        """Delete consumer groups.

        Delete consumer groups whose group IDs match bash-like patterns.

        Args:
            patterns (:obj:`str` | :obj:`list(str)`): The pattern or list of patterns for selecting the group IDs of those consumer groups which shall be deleted. Defaults to "*".
            state_patterns (:obj:`str` | :obj:`list(str)`, optional): The pattern or list of patterns for selecting the consumer group states ("unknown", "preparing_rebalancing", "completing_rebalancing", "stable", "dead", "empty") to delete.Defaults to "*" (all consumer group states).

        Returns:
            :obj:`list(str)`: List of strings (consumer group IDs) of the deleted consumer groups.

        Examples:
            Delete all those consumer groups whose group ID starts with "test"::

                c.delete_groups("test*")

            Delete all those consumer groups whose group ID starts with "test" or "bla"::

                c.delete_groups(["test*", "bla*"])

            Delete all those consumer groups whose group ID starts with "test" or "bla" and whose state is either "unknown", "preparing_rebalancing" or "completing_rebalancing"::

                c.delete_groups(["test*", "bla*"], ["unknown", "*_rebalancing"])
        """
        pattern_str_or_str_list = patterns
        #
        group_str_list = self.groups(pattern_str_or_str_list, state_patterns)
        if not group_str_list:
            return []
        #
        group_str_group_future_dict = self.adminClient.delete_consumer_groups(group_str_list)
        for group_future in group_str_group_future_dict.values():
            group_future.result() 
        group_str_list = list(group_str_group_future_dict.keys())
        #
        return group_str_list

    def group_offsets(self, patterns, state_patterns="*"):
        """List consumer group offsets.

        List consumer group offsets of those consumer groups whose group IDs match bash-like patterns.

        Args:
            patterns (:obj:`str` | :obj:`list(str)`): The pattern or list of patterns for selecting those consumer groups whose consumer groups offsets shall be listed.
            state_patterns (:obj:`str` | :obj:`list(str)`, optional): The pattern or list of patterns for selecting the consumer group states ("unknown", "preparing_rebalancing", "completing_rebalancing", "stable", "dead", "empty") to list the consumer group offsets from. Defaults to "*" (all consumer group states).

        Returns:
            :obj:`dict(str, dict(str, dict(int, int)))`: Dictionary mapping group IDs (strings) to dictionaries mapping topics (strings) to dictionaries mapping partitions (integers) to offsets (integers).

        Examples:
            List offsets of all those consumer groups whose group ID starts with "test"::

                c.group_offsets("test*")

            List offsets of those consumer groups whose group ID starts with "test" or "bla"::

                c.group_offsets(["test*", "bla*"])

            List offsets of all those consumer groups whose group ID starts with "test" or "bla" and whose state is either "unknown", "preparing_rebalancing" or "completing_rebalancing"::

                c.group_offsets(["test*", "bla*"], ["unknown", "*_rebalancing"])
        """
        pattern_str_or_str_list = patterns
        #
        group_str_list = self.groups(pattern_str_or_str_list, state_patterns)
        if not group_str_list:
            return {}
        #
        consumerGroupTopicPartitions_list = [_ConsumerGroupTopicPartitions(group_str) for group_str in group_str_list]
        group_str_consumerGroupTopicPartitions_future_dict = self.adminClient.list_consumer_group_offsets(consumerGroupTopicPartitions_list)
        #
        group_offsets = group_str_consumerGroupTopicPartitions_future_dict_to_group_offsets(group_str_consumerGroupTopicPartitions_future_dict)
        #
        return group_offsets

    def alter_group_offsets(self, group_offsets):
        """Alter consumer group offsets.

        Alter consumer group offsets.

        Args:
            group_offsets (:obj:`dict(str, dict(str, dict(int, int)))`): The group offsets dictionary describing which consumer group offsets for which topics and for which partitions shall be altered. A group offsets dictionary maps group IDs (strings) to topic offsets dictionaries, which in turn map topic names (strings) to partition offset dictionaries. Partition offset dictionaries map partitions (integers) to offsets (integers).

        Returns:
            :obj:`dict(str, dict(str, dict(int, int)))`: Group offsets dictionary after the successful alteration of the consumer group offsets.

        Examples:
            Alter offsets of the consumer group "group1" and topic "topic1". Change the offset on partition 0 to 42, and on partition 1 to 4711:

                c.alter_group_offsets({"group1": {"topic1": {0: 42, 1: 4711}}})
        """
        #
        consumerGroupTopicPartitions_list = group_offsets_to_consumerGroupTopicPartitions_list(group_offsets)
        group_str_consumerGroupTopicPartitions_future_dict = self.adminClient.alter_consumer_group_offsets(consumerGroupTopicPartitions_list)
        #
        group_offsets = group_str_consumerGroupTopicPartitions_future_dict_to_group_offsets(group_str_consumerGroupTopicPartitions_future_dict)
        #
        return group_offsets

    # AdminClient - brokers

    def brokers(self, pattern=None):
        """List brokers of the cluster.

        List brokers of the cluster. Optionally only list those brokers whose identifiers match the pattern (or list of patterns) pattern.

        Args:
            pattern (:obj:`str` | :obj:`list(str)`, optional): The pattern or list of patterns for selecting those brokers which shall be listed. Defaults to None.

        Returns:
            :obj:`dict(int, str)`: Dictionary of integers (broker identifiers) and strings (broker URLs and ports).

        Examples:
            List all brokers of the cluster::

                c.brokers()

            List only the brokers 0 and 1 of the cluster::

                c.brokers([0, 1])
        """
        pattern_int_or_str_or_int_or_str_list = pattern
        #
        if pattern_int_or_str_or_int_or_str_list is None:
            pattern_int_or_str_or_int_or_str_list = ["*"]
        else:
            if isinstance(pattern_int_or_str_or_int_or_str_list, int):
                pattern_int_or_str_or_int_or_str_list = [str(pattern_int_or_str_or_int_or_str_list)]
            elif isinstance(pattern_int_or_str_or_int_or_str_list, str):
                pattern_int_or_str_or_int_or_str_list = [pattern_int_or_str_or_int_or_str_list]
            elif isinstance(pattern_int_or_str_or_int_or_str_list, list):
                pattern_int_or_str_or_int_or_str_list = [str(pattern_int_or_str) for pattern_int_or_str in pattern_int_or_str_or_int_or_str_list]
        #
        broker_dict = {broker_int: brokerMetadata.host + ":" + str(brokerMetadata.port) for broker_int, brokerMetadata in self.adminClient.list_topics().brokers.items() if any(fnmatch(str(broker_int), pattern_int_or_str) for pattern_int_or_str in pattern_int_or_str_or_int_or_str_list)}
        #
        return broker_dict

    def broker_config(self, pattern_int_or_str_or_int_or_str_list):
        """List the configurations of brokers of the cluster.

        List the configurations of brokers of the cluster. Optionally only list those brokers whose identifiers match the pattern (or list of patterns) pattern.

        Args:
            pattern_int_or_str_or_int_or_str_list (:obj:`str` | :obj:`list(str)`, optional): The pattern or list of patterns for selecting those brokers which shall be listed. Defaults to None.

        Returns:
            :obj:`dict(int, dict(str, str))`: Dictionary of integers (broker identifiers) and configuration dictionaries (dictionaries of strings (keys) and strings (values)).

        Examples:
            List the configuration of broker 0 of the cluster::

                c.broker_config(0)

            List the configurations of brokers 0 and 1 of the cluster::

                c.broker_config([0, 1])
        """
        broker_dict = self.brokers(pattern_int_or_str_or_int_or_str_list)
        #
        broker_int_broker_config_dict = {broker_int: self.get_config_dict(ResourceType.BROKER, str(broker_int)) for broker_int in broker_dict}
        #
        return broker_int_broker_config_dict

    def set_broker_config(self, pattern_int_or_str_or_int_or_str_list, key_str, value_str, test=False):
        """Set a configuration item of brokers.

        Set the configuration item with key key_str and value value_str of those brokers whose identifiers match the bash-like pattern (or list of patterns) pattern_int_or_str_or_int_or_str_list.

        Args:
            pattern_str_or_str_list (:obj:`str` | :obj:`list(str)`): The pattern (or list of patterns) for selecting those topics whose configuration shall be changed.
            key_str (:obj:`str`): Configuration key.
            value_str (:obj:`str`): Configuration value.
            test (:obj:`bool`, optional): If True, the request is only validated without changing the configuration. Defaults to False.

        Returns:
            :obj:`dict(int, tuple(str, str))`: Dictionary of integers (broker identifiers) and tuples of strings (configuration keys) and strings (configuration values)

        Examples:
            Sets the configuration key "background.threads" to configuration value "5" for broker 0::

                c.set_broker_config(0, "background.threads", "5")

            Verifies if the configuration key "background.threads" can be set to configuration value "5" for broker 0, but does not change the configuration::

                c.set_broker_config(0, "background.threads", "5", test=True)

            Sets the configuration key "background.threads" to configuration value "5" for brokers 0, 1 and 2::

                c.set_broker_config("[0-2]", "background.threads", "5")
        """
        test_bool = test
        #
        broker_dict = self.brokers(pattern_int_or_str_or_int_or_str_list)
        #
        for broker_int in broker_dict:
            self.set_config_dict(ResourceType.BROKER, str(broker_int), {key_str: value_str}, test_bool)
        #
        broker_int_key_str_value_str_tuple_dict = {broker_int: (key_str, value_str) for broker_int in broker_dict}
        return broker_int_key_str_value_str_tuple_dict

    # AdminClient - ACLs

    def acls(self, restype="any", name=None, resource_pattern_type="any", principal=None, host=None, operation="any", permission_type="any"):
        """List ACLs.

        List ACLs of the cluster.

        Args:
            restype (:obj:`str`, optional): The resource type ("unknown", "any", "topic", "group" or "broker"). Defaults to "any".
            name (:obj:`str`, optional): The name. Defaults to None.
            resource_pattern_type (:obj:`str`, optional): The resource pattern type ("unknown", "any", "match", "literal" or "prefixed"). Defaults to "any".
            principal (:obj:`str`, optional): The principal. Defaults to None.
            host (:obj:`str`, optional): The host. Defaults to None.
            operation (:obj:`str`, optional): The operation ("unknown", "any", "all", "read", "write", "create", "delete", "alter", "describe", "cluster_action", "describe_configs", "alter_configs", "idempotent_write"). Defaults to "any"
            permission_type (:obj:`str`, optional): The permission type ("unknown", "any", "deny" or "allow"). Defaults to "any".

        Returns:
            :obj:`list(aclBinding_dict)`: List of ACL Binding dictionaries (converted from confluent_kafka.AclBinding objects) of the selected ACLs.

        Examples:
            List all ACLs of the cluster::

                c.acls()

            List all ACLs for the topics of the cluster::

                c.acls(restype="topic")
        """
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
        """Create an ACL.

        Create an ACL on the cluster.

        Args:
            restype (:obj:`str`, optional): The resource type ("unknown", "any", "topic", "group" or "broker"). Defaults to "any".
            name (:obj:`str`, optional): The name. Defaults to None.
            resource_pattern_type (:obj:`str`, optional): The resource pattern type ("unknown", "any", "match", "literal" or "prefixed"). Defaults to "any".
            principal (:obj:`str`, optional): The principal. Defaults to None.
            host (:obj:`str`, optional): The host. Defaults to None.
            operation (:obj:`str`, optional): The operation ("unknown", "any", "all", "read", "write", "create", "delete", "alter", "describe", "cluster_action", "describe_configs", "alter_configs", "idempotent_write"). Defaults to "any"
            permission_type (:obj:`str`, optional): The permission type ("unknown", "any", "deny" or "allow"). Defaults to "any".

        Returns:
            :obj:`aclBinding_dict`: ACL Binding dictionary (converted from an confluent_kafka.AclBinding object) of the created ACL.

        Examples:
            Grant user "abc" read permission on topic "test"::

                c.create_acl(restype="topic", name="test", resource_pattern_type="literal", principal="User:abc", host="*", operation="read", permission_type="allow")
        """
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
        """Delete ACLs.

        Delete ACLs from the cluster.

        Args:
            restype (:obj:`str`, optional): The resource type ("unknown", "any", "topic", "group" or "broker"). Defaults to "any".
            name (:obj:`str`, optional): The name. Defaults to None.
            resource_pattern_type (:obj:`str`, optional): The resource pattern type ("unknown", "any", "match", "literal" or "prefixed"). Defaults to "any".
            principal (:obj:`str`, optional): The principal. Defaults to None.
            host (:obj:`str`, optional): The host. Defaults to None.
            operation (:obj:`str`, optional): The operation ("unknown", "any", "all", "read", "write", "create", "delete", "alter", "describe", "cluster_action", "describe_configs", "alter_configs", "idempotent_write"). Defaults to "any"
            permission_type (:obj:`str`, optional): The permission type ("unknown", "any", "deny" or "allow"). Defaults to "any".

        Returns:
            :obj:`list(aclBinding_dict)`: List of ACL Binding dictionaries (converted from confluent_kafka.AclBinding objects) of the deleted ACLs.

        Examples:
            Delete the ACL which granted user "abc" read permission on topic "test"::

                c.delete_acl(restype="topic", name="test", resource_pattern_type="literal", principal="User:abc", host="*", operation="read", permission_type="allow")
        """
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

    def write(self, value:Any, key:Any=None, key_type:str="str", value_type:str="str", key_schema:str=None, value_schema:str=None, partition:int=RD_KAFKA_PARTITION_UA, timestamp:int=CURRENT_TIME, headers:Union[Dict, List]=None, on_delivery:Callable[[KafkaError, Message], None]=None):
        return self.produce(self.topic_str, value, key, key_type, value_type, key_schema, value_schema, partition, timestamp, headers, on_delivery)

    #

    def produce(self, topic_str, value, key=None, key_type="str", value_type="str", key_schema=None, value_schema=None, partition=RD_KAFKA_PARTITION_UA, timestamp=CURRENT_TIME, headers=None, on_delivery=None):
        """Produce a message to a topic.

        Produce a message to a topic. The key and the value of the message can be either bytes, a string, a dictionary, or a schema-based format supported by the Confluent Schema Registry (currently Avro, Protobuf or JSONSchema).

        Args:
            topic_str (:obj:`str`): The topic to produce to.
            value (:obj:`bytes` | :obj:`str` |  :obj:`dict`): The value of the message to be produced.
            key (:obj:`bytes` | :obj:`str` |  :obj:`dict`, optional): The key of the message to be produced. Defaults to None.
            key_type (:obj:`str`, optional): The key type ("bytes", "str", "json", "avro", "protobuf" or "pb", or "jsonschema"). Defaults to "str".
            value_type (:obj:`str`, optional): The value type ("bytes", "str", "json", "avro", "protobuf" or "pb", or "jsonschema"). Defaults to "str".
            key_schema (:obj:`str`, optional): The schema of the key of the message to be produced (if key_type is either "avro", "protobuf" or "pb", or "jsonschema"). Defaults to None.
            value_schema (:obj:`str`, optional): The schema of the value of the message to be produced (if key_type is either "avro", "protobuf" or "pb", or "jsonschema"). Defaults to None.
            partition (:obj:`int`, optional): The partition to produce to. Defaults to RD_KAFKA_PARTITION_UA = -1, i.e., the partition is selected by configured built-in partitioner.
            timestamp (:obj:`int`, optional): Message timestamp (CreateTime) in milliseconds since epoch UTC. Defaults to CURRENT_TIME = 0.
            headers (:obj:`dict` | :obj:`list`, optional): Message headers to set on the message. The header key must be a string while the value must be binary, unicode or None. Accepts a list of (key,value) or a dict. Defaults to None.
            on_delivery (:obj:`function`, optional): Delivery report callback to call (from poll() or flush()) on successful or failed delivery. Passed on to confluent_kafka.Producer.produce(). Takes confluent_kafka.kafkaError and confluent_kafka.Message objects and returns nothing.

        Returns:
            :obj:`tuple(bytes | str, bytes | str)`: Pair of bytes or string and bytes or string (=the key and the value of the produced message).

        Examples:
            Produce a message with value = "value 1" and key = None to the topic "test"::

                c.produce("test", "value 1")

            Produce a message with value = "value 1" and key = "key 1" to the topic "test"::

                c.produce("test", "value 1", key="key 1")

            Produce a message with value = "value 1" and key = "key 1" to partition 0 of the topic "test"::

                c.produce("test", "value 1", key="key 1", partition=0)

            Produce a message with value = "value 1" and key = "key 1" to the topic "test", set the timestamp of this message to 1664902656169::

                c.produce("test", "value 1", key="key 1", timestamp=1664902656169)

            Produce a message with value = "value 1" and key = None to the topic "test", set the headers of this message to {"bla": "blups"}::

                c.produce("test", "value 1", headers={"bla": "blups"})

            Produce a message with value = {'name': 'cookie', 'calories': 500.0, 'colour': 'brown'} and key = None to the topic "test", using JSON without schema::

                c.produce("test", {'name': 'cookie', 'calories': 500.0, 'colour': 'brown'}, value_type="json")

            Produce a message with value = {'name': 'cookie', 'calories': 500.0, 'colour': 'brown'} and key = None to the topic "test", using Avro with the schema '{ "type": "record", "name": "myrecord", "fields": [{"name": "name",  "type": "string" }, {"name": "calories", "type": "float" }, {"name": "colour", "type": "string" }] }'::

                c.produce("test", {'name': 'cookie', 'calories': 500.0, 'colour': 'brown'}, value_type="avro", value_schema='{ "type": "record", "name": "myrecord", "fields": [{"name": "name",  "type": "string" }, {"name": "calories", "type": "float" }, {"name": "colour", "type": "string" }] }')

            Produce a message with value = {'name': 'cookie', 'calories': 500.0, 'colour': 'brown'} and key = None to the topic "test", using Protobuf with the schema 'message Snack { required string name = 1; required float calories = 2; optional string colour = 3; }'::

                c.produce("test", {'name': 'cookie', 'calories': 500.0, 'colour': 'brown'}, value_type="protobuf", value_schema='message Snack { required string name = 1; required float calories = 2; optional string colour = 3; }')

            Produce a message with value = {'name': 'cookie', 'calories': 500.0, 'colour': 'brown'} and key = None to the topic "test", using JSONSchema with the schema '{ "title": "abc", "definitions" : { "record:myrecord" : { "type" : "object", "required" : [ "name", "calories" ], "additionalProperties" : false, "properties" : { "name" : {"type" : "string"}, "calories" : {"type" : "number"}, "colour" : {"type" : "string"} } } }, "$ref" : "#/definitions/record:myrecord" }'::

                c.produce("test", {'name': 'cookie', 'calories': 500.0, 'colour': 'brown'}, value_type="jsonschema", value_schema='{ "title": "abc", "definitions" : { "record:myrecord" : { "type" : "object", "required" : [ "name", "calories" ], "additionalProperties" : false, "properties" : { "name" : {"type" : "string"}, "calories" : {"type" : "number"}, "colour" : {"type" : "string"} } } }, "$ref" : "#/definitions/record:myrecord" }')
        """
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
            if type_str.lower() == "json":
                if isinstance(payload, dict):
                    payload_str_or_bytes = json.dumps(payload)
                else:
                    payload_str_or_bytes = payload
            elif type_str.lower() in ["pb", "protobuf"]:
                generalizedProtocolMessageType = self.schema_str_to_generalizedProtocolMessageType(schema_str, topic_str, key_bool)
                protobufSerializer = ProtobufSerializer(generalizedProtocolMessageType, self.schemaRegistryClient, {"use.deprecated.format": False})
                payload_dict = payload_to_payload_dict(payload)
                protobuf_message = generalizedProtocolMessageType()
                ParseDict(payload_dict, protobuf_message)
                payload_str_or_bytes = protobufSerializer(protobuf_message, SerializationContext(topic_str, messageField))
            elif type_str.lower() == "avro":
                avroSerializer = AvroSerializer(self.schemaRegistryClient, schema_str)
                payload_dict = payload_to_payload_dict(payload)
                payload_str_or_bytes = avroSerializer(payload_dict, SerializationContext(topic_str, messageField))
            elif type_str.lower() == "jsonschema":
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
        self.producer.produce(topic_str, value_str_or_bytes, key_str_or_bytes, partition=partition_int, timestamp=timestamp_int, headers=headers_dict_or_list, on_delivery=on_delivery)
        #
        self.produced_messages_counter_int += 1
        #
        return key_str_or_bytes, value_str_or_bytes

    def flush(self):
        """Wait for all messages in the Producer queue to be delivered.

        Wait for all messages in the Producer queue to be delivered. Uses the "flush.timeout" setting from the cluster configuration file ("kash"-section).
        """
        self.producer.flush(self.kash_dict["flush.timeout"])
        return self.topic_str

    # Consumer

    def subscribe(self, topics, group=None, offsets=None, config={}, key_type="str", value_type="str"):
        """Subscribe to a topic or a list of topics.

        Subscribe to a topic or a list of topics, optionally explicitly set the consumer group, initial offsets, and augment the consumer configuration. Prerequisite for consuming from a topic. Set "auto.offset.reset" to the configured "auto.offset.value" in the configuration ("kash"-section), and "enable.auto.commit" and "session.timeout.ms" as well.

        Args:
            topic (:obj:`str`): The topic to subscribe to.
            group (:obj:`str`, optional): The consumer group name. If set to None, automatically create a new unique consumer group name. Defaults to None.
            offsets (:obj:`dict(int, int)`, optional): Dictionary of integers (partitions) and integers (initial offsets for the individual partitions of the topic). If set to None, does not set any initial offsets. Defaults to None.
            config (:obj:`dict(str, str)`, optional): Dictionary of strings (keys) and strings (values) to augment the consumer configuration. Defaults to {}.
            key_type (:obj:`str`, optional): The key type ("bytes", "str", "json", "avro", "protobuf" or "pb", or "jsonschema"). Defaults to "str".
            value_type (:obj:`str`, optional): The value type ("bytes", "str", "json", "avro", "protobuf" or "pb", or "jsonschema"). Defaults to "str".

        Returns:
            :obj:`tuple(str, str)` Pair of the topic subscribed to (string) and the used consumer group name (string).

        Examples:
            Subscribe to the topic "test" using an automatically created new unique consumer group::

                c.subscribe("test")

            Susbcribe to the topic "test" using the consumer group name "test_group"::

                c.subscribe("test", group="test_group")

            Subscribe to the topic "test" using an automatically created new unique consumer group. Set the initial offset for the first partition to 42, and for the second partition to 4711::

                c.subscribe("test", offsets={0: 42, 1: 4711})

            Subscribe to the topic "test" using an automatically created new unique consumer group. Set the configuration key "enable.auto.commit" to "False"::

                c.subscribe("test", config={"enable.auto.commit": "False"})

            Subscribe to the topic "test" using an automatically created new unique consumer group. Consume with key and value type "avro"::

                c.subscribe("test", key_type="avro", value_type="avro")
        """
        # topics
        topic_str_list = topics if isinstance(topics, list) else [topics]
        if not topic_str_list:
            raise Exception("No topic to subscribe to.")
        # group
        if group is None:
            group_str = self.create_unique_group_id()
        else:
            group_str = group
        # offsets
        if offsets is None:
            offsets_dict = None
        else:
            str_or_int = list(offsets.keys())[0]
            if isinstance(str_or_int, str):
                offsets_dict = offsets
            elif isinstance(str_or_int, int):
                offsets_dict = {topic_str: offsets for topic_str in topic_str_list}
        # config
        config_dict = config
        # key_type
        if isinstance(key_type, dict):
            key_type_dict = key_type
        else:
            key_type_dict = {topic_str: key_type for topic_str in topic_str_list}
        # value_type
        if isinstance(value_type, dict):
            value_type_dict = value_type
        else:
            value_type_dict = {topic_str: value_type for topic_str in topic_str_list}
        #
        self.config_dict["group.id"] = group_str
        self.config_dict["auto.offset.reset"] = self.kash_dict["auto.offset.reset"]
        self.config_dict["enable.auto.commit"] = self.kash_dict["enable.auto.commit"]
        self.config_dict["session.timeout.ms"] = self.kash_dict["session.timeout.ms"]
        for key_str, value in config_dict.items():
            self.config_dict[key_str] = value
        consumer = get_consumer(self.config_dict)
        #
        def on_assign(consumer, partitions):
            def set_offset(topicPartition):
                if topicPartition.topic in offsets_dict:
                    offsets = offsets_dict[topicPartition.topic]
                    if topicPartition.partition in offsets:
                        offset_int = offsets[topicPartition.partition]
                        topicPartition.offset = offset_int
                return topicPartition
            #
            if offsets_dict is not None:
                topicPartition_list = [set_offset(topicPartition) for topicPartition in partitions]
                consumer.assign(topicPartition_list)
        consumer.subscribe(topic_str_list, on_assign=on_assign)
        #
        subscription_id_int = self.create_subscription_id()
        self.subscription_dict[subscription_id_int] = {"topics": topic_str_list, "group": group_str, "key_type": key_type_dict, "value_type": value_type_dict, "consumer": consumer, "last_key_schema": None, "last_value_schema": None}
        #
        return topic_str_list, group_str, subscription_id_int
    
    def unsubscribe(self, sub=None):
        """Unsubscribe.

        Unsubscribe a subscription.

        Returns:
            :obj:`tuple(str, str)` Pair of the topic unsubscribed from (string) and the used consumer group (string).
        """
        subscription_id_int = self.get_subscription_id(sub)
        subscription = self.subscription_dict[subscription_id_int]
        #
        consumer.unsubscribe()
        #
        topic_str_list = subscription["topics"]
        group_str = subscription["group"]
        consumer = subscription["consumer"]
        self.subscription_dict.pop(subscription_id_int)
        #
        return topic_str_list, group_str, subscription_id_int

    def close(self, sub=None):
        """Close the consumer.

        Close the consumer.
        """
        subscription = self.get_subscription(sub)
        consumer = subscription["consumer"]
        #
        topic_str_list, group_str, subscription_id_int = self.unsubscribe(sub)
        #
        consumer.close()
        #
        return topic_str_list, group_str, subscription_id_int

    def consume(self, n=1, sub=None):
        """Consume messages from a subscribed topic.

        Consume messages from a subscribed topic.

        Args:
            n (:obj:`int`, optional): Maximum number of messages to return. Defaults to 1.

        Returns:
            :obj:`list(message_dict)`: List of message dictionaries (converted from confluent_kafka.Message).

        Examples:
            Consume the next message from the topic subscribed to before::

                c.consume()

            Consume the next 100 messages from the topic subscribed to before::

                c.consume(n=100)
        """
        subscription = self.get_subscription(sub)
        #
        num_messages_int = n
        #
        consumer = subscription["consumer"]
        message_list = consumer.consume(num_messages_int, self.kash_dict["consume.timeout"])
        #
        message_dict_list = [self.message_to_message_dict(message, key_type=subscription["key_type"][message.topic()], value_type=subscription["value_type"][message.topic()]) for message in message_list]
        #
        return message_dict_list

    def commit(self, offsets=None, asynchronous=False, sub=None):
        """Commit the last consumed message from the topic subscribed to.

        Commit the last consumed message from the topic subscribed to.

        Args:
            asynchronous (:obj:`bool`, optional): Passed to the confluent_kafka.Consumer.commit() method: If true, asynchronously commit, if False, the commit() call will block until the commit succeeds or fails. Defaults to False.

        Returns:
            :obj:`message_dict`: Last consumed message dictionary (converted from confluent_kafka.Message).
        """
        subscription = self.get_subscription(sub)
        #
        consumer = subscription["consumer"]
        #
        if offsets is None:
            offsets_topicPartition_list = None
            commit_topicPartition_list = consumer.commit(asynchronous=asynchronous)
        else:
            str_or_int = list(offsets.keys())[0]
            if isinstance(str_or_int, str):
                offsets_dict = offsets
            elif isinstance(str_or_int, int):
                offsets_dict = {topic_str: offsets for topic_str in subscription["topics"]}
            #
            offsets_topicPartition_list = [TopicPartition(topic_str, partition_int, offset_int) for topic_str, offsets in offsets_dict.items() for partition_int, offset_int in offsets.items()]
            commit_topicPartition_list = consumer.commit(offsets=offsets_topicPartition_list, asynchronous=asynchronous)
        #
        offsets_dict = topicPartition_list_to_offsets_dict(commit_topicPartition_list)
        #
        return offsets_dict

    def offsets(self, timeout=-1.0, sub=None):
        """Get committed offsets of the subscribed topic.

        Get committed offsets of the subscribed topic.

        Args:
            timeout (:obj:`float`, optional): Timeout (in seconds) for calling confluent_kafka.committed(). Defaults to -1.0 (no timeout).

        Returns:
            :obj:`dict(int, int)`: (Partition) offsets dictionary for the subscribed topic. A (partition) offsets dictionary maps partitions (integers) to offsets (integers).

        Examples:
            Get the offsets of the subscribed topic::

                c.offsets()

            Get the offsets of the subscribed topic, using a timeout of 1 second::

                c.offsets(timeout=1.0)
        """
        subscription = self.get_subscription(sub)
        #
        timeout_float = timeout
        #
        consumer = subscription["consumer"]
        assignment_topicPartition_list = consumer.assignment()
        committed_topicPartition_list = consumer.committed(assignment_topicPartition_list, timeout=timeout_float)
        #
        offsets_dict = topicPartition_list_to_offsets_dict(committed_topicPartition_list)
        #
        return offsets_dict

    def memberid(self, sub=None):
        """Get the group member ID of the consumer.

        Get the group member ID of the consumer.

        Returns:
            :obj:`str`: Group member ID of the consumer.
        """
        subscription = self.get_subscription(sub)
        #
        consumer = subscription["consumer"]
        member_id_str = consumer.memberid()
        #
        return member_id_str 
