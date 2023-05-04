import json

import confluent_kafka
from confluent_kafka import TopicPartition
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from google.protobuf.json_format import MessageToDict
import importlib
import os
import sys
import tempfile

from kashpy.helpers import get_millis
from kashpy.schemaregistry import SchemaRegistry

class Consumer:
    def __init__(self, kafka_config_dict, schema_registry_config_dict, kash_config_dict):
        self.kafka_config_dict = kafka_config_dict
        self.schema_registry_config_dict = schema_registry_config_dict
        self.kash_config_dict = kash_config_dict
        #
        self.group_str = None
        self.topic_str_list = None
        self.key_type_dict = None
        self.value_type_dict = None
        #
        self.consumer = None
        self.schema_registry = SchemaRegistry()
        #
        self.schema_id_int_generalizedProtocolMessageType_protobuf_schema_str_tuple_dict = {}

    #

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

    def schema_id_int_to_generalizedProtocolMessageType_protobuf_schema_str_tuple(self, schema_id_int):
        schema_dict = self.schemaRegistry.get_schema(schema_id_int)
        schema_id_int = schema_dict["schema_id"]
        schema_str = schema_dict["schema_str"]
        #
        generalizedProtocolMessageType = self.schema_id_int_and_schema_str_to_generalizedProtocolMessageType(schema_id_int, schema_str)
        #
        return generalizedProtocolMessageType, schema_str

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
        schema_dict = self.schemaRegistry.get_schema(schema_id_int)
        schema_str = schema_dict["schema_str"]
        #
        if key_bool:
            self.last_consumed_message_key_schema_str = schema_str
        else:
            self.last_consumed_message_value_schema_str = schema_str
        #
        avroDeserializer = AvroDeserializer(self.schemaRegistry.schemaRegistryClient, schema_str)
        dict = avroDeserializer(bytes, None)
        return dict

    def bytes_jsonschema_to_dict(self, bytes, key_bool):
        schema_id_int = int.from_bytes(bytes[1:5], "big")
        schema_dict = self.schemaRegistry.get_schema(schema_id_int)
        schema_str = schema_dict["schema_str"]
        #
        if key_bool:
            self.last_consumed_message_key_schema_str = schema_str
        else:
            self.last_consumed_message_value_schema_str = schema_str
        #
        jsonDeserializer = JSONDeserializer(schema_str)
        dict = jsonDeserializer(bytes, None)
        return dict

    def topicPartition_list_to_offsets_dict(self, topicPartition_list):
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

    #

    def subscribe(self, group, topics, offsets=None, config={}, key_type="str", value_type="str"):
        if group is None:
            self.group_str = str(get_millis())
        else:
            self.group_str = group
        #
        topic_str_list = topics if isinstance(topics, list) else [topics]
        if not topic_str_list:
            raise Exception("No topic to subscribe to.")
        self.topic_str_list = topic_str_list
        #
        if offsets is None:
            offsets_dict = None
        else:
            str_or_int = list(offsets.keys())[0]
            if isinstance(str_or_int, str):
                offsets_dict = offsets
            elif isinstance(str_or_int, int):
                offsets_dict = {topic_str: offsets for topic_str in topic_str_list}
        #        
        self.config_dict = {}
        self.config_dict["group.id"] = self.group_str
        for key_str, value in config.items():
            self.config_dict[key_str] = value
        if isinstance(key_type, dict):
            self.key_type_dict = key_type
        else:
            self.key_type_dict = {topic_str: key_type for topic_str in topic_str_list}
        #
        if isinstance(value_type, dict):
            self.value_type_dict = value_type
        else:
            self.value_type_dict = {topic_str: value_type for topic_str in topic_str_list}
        #
        self.config_dict["group.id"] = self.group_str
        for key_str, value in config.items():
            self.config_dict[key_str] = value
        self.consumer = confluent_kafka.Consumer(self.kafka_config_dict)
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
        self.consumer.subscribe(topic_str_list, on_assign=on_assign)
        #
        return self.topic_str_list, self.group_str
    
    def unsubscribe(self):
        self.consumer.unsubscribe()
        #
        topic_str_list = self.topic_str_list
        #
        self.topic_str_list = None
        self.key_type_dict = None
        self.value_type_dict = None
        #
        return topic_str_list

    def close(self):
        topic_str_list = self.unsubscribe()
        #
        self.consumer.close()
        #
        group_str = self.group_str
        #
        self.group_str = None
        #
        return topic_str_list, group_str

    def consume(self, n=1):
        n_int = n
        #
        message_list = self.consumer.consume(n_int, self.cluster.kash_dict["consume.timeout"])
        #
        message_dict_list = [self.message_to_message_dict(message, key_type=self.key_type_dict[message.topic()], value_type=self.value_type_dict[message.topic()]) for message in message_list]
        #
        return message_dict_list

    def commit(self, offsets=None, asynchronous=False):
        if offsets is None:
            offsets_topicPartition_list = None
            commit_topicPartition_list = self.consumer.commit(asynchronous=asynchronous)
        else:
            str_or_int = list(offsets.keys())[0]
            if isinstance(str_or_int, str):
                offsets_dict = offsets
            elif isinstance(str_or_int, int):
                offsets_dict = {topic_str: offsets for topic_str in self.topic_str_list}
            #
            offsets_topicPartition_list = [TopicPartition(topic_str, partition_int, offset_int) for topic_str, offsets in offsets_dict.items() for partition_int, offset_int in offsets.items()]
            commit_topicPartition_list = self.consumer.commit(offsets=offsets_topicPartition_list, asynchronous=asynchronous)
        #
        offsets_dict = self.topicPartition_list_to_offsets_dict(commit_topicPartition_list)
        #
        return offsets_dict

    def offsets(self, timeout=-1.0):
        timeout_float = timeout
        #
        assignment_topicPartition_list = self.consumer.assignment()
        committed_topicPartition_list = self.consumer.committed(assignment_topicPartition_list, timeout=timeout_float)
        #
        offsets_dict = self.topicPartition_list_to_offsets_dict(committed_topicPartition_list)
        #
        return offsets_dict

    def memberid(self):
        member_id_str = self.consumer.memberid()
        #
        return member_id_str 