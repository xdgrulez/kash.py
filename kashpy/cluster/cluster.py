from confluent_kafka import KafkaError, Message, OFFSET_BEGINNING, OFFSET_END, OFFSET_INVALID, OFFSET_STORED, Producer, TopicPartition
from confluent_kafka.admin import AclBinding, AclBindingFilter, AclOperation, AclPermissionType, AdminClient, ConfigResource, _ConsumerGroupTopicPartitions, _ConsumerGroupState, NewPartitions, NewTopic, ResourceType, ResourcePatternType
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from google.protobuf.json_format import ParseDict
from piny import YamlLoader
from fnmatch import fnmatch
import glob
import importlib
import json
import os
import re
import requests
import sys
import tempfile
import time

from kashpy.kafka import Kafka
from kashpy.cluster.consumer import Consumer

# Cluster class

class Cluster(Kafka):
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
