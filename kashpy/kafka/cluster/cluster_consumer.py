import json

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from google.protobuf.json_format import MessageToDict
import importlib
import os
import sys
import tempfile

from kashpy.helpers import get_millis
from kashpy.kafka.schemaregistry import SchemaRegistry

# Constants

ALL_MESSAGES = -1

#

class ClusterConsumer():
    def __init__(self, kafka_obj, *topics, **kwargs):
        self.kafka_config_dict = kafka_obj.kafka_config_dict
        self.schema_registry_config_dict = kafka_obj.schema_registry_config_dict
        self.kash_config_dict = kafka_obj.kash_config_dict
        #
        # Topics
        #
        self.topic_str_list = list(topics)
        #
        # Group
        #
        self.group_str = kafka_obj.get_group_str(**kwargs)
        #
        # Offsets
        #
        self.offsets_dict = kafka_obj.get_offsets_dict(self.topic_str_list, **kwargs)
        #
        # Key and Value Types
        #
        (self.key_type, self.value_type) = kafka_obj.get_key_value_type_tuple(**kwargs)
        #
        (self.key_type_dict, self.value_type_dict) = kafka_obj.get_key_value_type_dict_tuple(self.key_type, self.value_type, self.topic_str_list)
        #
        if "schema.registry.url" in self.schema_registry_config_dict:
            self.schemaRegistry = SchemaRegistry(self.schema_registry_config_dict, self.kash_config_dict)
        else:
            self.schemaRegistry = None
        #
        self.schema_id_int_generalizedProtocolMessageType_protobuf_schema_str_tuple_dict = {}
        #
        # Consumer Config
        #
        self.consumer_config_dict = self.kafka_config_dict.copy()
        self.consumer_config_dict["group.id"] = self.group_str
        self.consumer_config_dict["auto.offset.reset"] = self.kash_config_dict["auto.offset.reset"]
        self.consumer_config_dict["enable.auto.commit"] = self.kash_config_dict["enable.auto.commit"]
        self.consumer_config_dict["session.timeout.ms"] = self.kash_config_dict["session.timeout.ms"]
        if "config" in kwargs:
            for key_str, value in kwargs["config"].items():
                self.consumer_config_dict[key_str] = value
        #
        self.consumer = Consumer(self.consumer_config_dict)
        #
        self.subscribe()

    def __del__(self):
        self.close()

    #

    def read(self, n=ALL_MESSAGES, **kwargs):
        def foldl_function(message_dict_list, message_dict):
            message_dict_list.append(message_dict)
            #
            return message_dict_list
        #
        return self.foldl(foldl_function, [], n, **kwargs)

    #

    def foldl(self, foldl_function, initial_acc, n=ALL_MESSAGES, **kwargs):
        n_int = n
        #
        break_function = kwargs["break_function"] if "break_function" in kwargs else lambda _, _1: False
        #
        acc = initial_acc
        message_counter_int = 0
        break_bool = False
        verbose_int = self.kash_config_dict["verbose"]
        # Consume batch size should not be higher than n_int (otherwise would skip messages when called multiple times).
        consume_num_messages_int = n_int if self.kash_config_dict["consume.num.messages"] > n_int else self.kash_config_dict["consume.num.messages"]
        while True:
            message_dict_list = self.consume(n=consume_num_messages_int, **kwargs)
            if not message_dict_list:
                break
            #
            for message_dict in message_dict_list:
                if break_function(acc, message_dict):
                    break_bool = True
                    break
                acc = foldl_function(acc, message_dict)
            #
            message_counter_int += len(message_dict_list)
            #
            if break_bool:
                break
            #
            if verbose_int > 0 and message_counter_int % self.kash_config_dict["progress.num.messages"] == 0:
                print(f"Consumed: {message_counter_int}")
            if n_int != ALL_MESSAGES:
                if message_counter_int >= n_int:
                    break
        #
        return (acc, message_counter_int)

    #

    def subscribe(self):
        def on_assign(consumer, partitions):
            def set_offset(topicPartition):
                if topicPartition.topic in self.offsets_dict:
                    offsets = self.offsets_dict[topicPartition.topic]
                    if topicPartition.partition in offsets:
                        offset_int = offsets[topicPartition.partition]
                        topicPartition.offset = offset_int
                return topicPartition
            #
            if self.offsets_dict is not None:
                topicPartition_list = [set_offset(topicPartition) for topicPartition in partitions]
                consumer.assign(topicPartition_list)
        self.consumer.subscribe(self.topic_str_list, on_assign=on_assign)
        #
        return self.topic_str_list, self.group_str
    
    def unsubscribe(self):
        self.consumer.unsubscribe()
        #
        return self.topic_str_list, self.group_str

    def close(self):
        self.consumer.close()
        #
        return self.topic_str_list, self.group_str

    #

    def consume(self, **kwargs):
        n_int = kwargs["batch_size"] if "batch_size" in kwargs else 1
        #
        message_list = self.consumer.consume(n_int, self.kash_config_dict["consume.timeout"])
        #
        message_dict_list = [self.message_to_message_dict(message, key_type=self.key_type_dict[message.topic()], value_type=self.value_type_dict[message.topic()]) for message in message_list]
        #
        return message_dict_list

    def commit(self, offsets=None, asynchronous=False): # TODO: Support message argument
        asynchronous_bool = asynchronous
        #
        if offsets is None:
            commit_topicPartition_list = self.consumer.commit() # cimpl.KafkaException: KafkaError{code=_NO_OFFSET,val=-168,str="Commit failed: Local: No offset stored"} if "asynchronous" argument is set
        else:
            str_or_int = list(offsets.keys())[0]
            if isinstance(str_or_int, str):
                offsets_dict = offsets
            elif isinstance(str_or_int, int):
                offsets_dict = {topic_str: offsets for topic_str in self.topic_str_list}
            #
            offsets_topicPartition_list = [TopicPartition(topic_str, partition_int, offset_int) for topic_str, offsets in offsets_dict.items() for partition_int, offset_int in offsets.items()]
            #
            commit_topicPartition_list = self.consumer.commit(offsets=offsets_topicPartition_list, asynchronous=asynchronous_bool)
        #
        offsets_dict = topicPartition_list_to_offsets_dict(commit_topicPartition_list)
        #
        return offsets_dict

    def offsets(self, timeout=-1.0):
        timeout_float = timeout
        #
        assignment_topicPartition_list = self.consumer.assignment()
        committed_topicPartition_list = self.consumer.committed(assignment_topicPartition_list, timeout=timeout_float)
        #
        offsets_dict = topicPartition_list_to_offsets_dict(committed_topicPartition_list)
        #
        return offsets_dict

    def memberid(self):
        member_id_str = self.consumer.memberid()
        #
        return member_id_str 

    #

    def message_to_message_dict(self, message, key_type, value_type):
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
                return self.bytes_protobuf_to_dict(bytes)
        elif key_type_str.lower() == "avro":
            def decode_key(bytes):
                return self.bytes_avro_to_dict(bytes)
        elif key_type_str.lower() == "jsonschema":
            def decode_key(bytes):
                return self.bytes_jsonschema_to_dict(bytes)
        #
        if value_type_str.lower() == "str":
            decode_value = bytes_to_str
        elif value_type_str.lower() == "bytes":
            decode_value = bytes_to_bytes
        elif value_type_str.lower() == "json":
            decode_value = json.loads
        elif value_type_str.lower() in ["pb", "protobuf"]:
            def decode_value(bytes):
                return self.bytes_protobuf_to_dict(bytes)
        elif value_type_str.lower() == "avro":
            def decode_value(bytes):
                return self.bytes_avro_to_dict(bytes)
        elif value_type_str.lower() == "jsonschema":
            def decode_value(bytes):
                return self.bytes_jsonschema_to_dict(bytes)
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

    def bytes_protobuf_to_dict(self, bytes):
        schema_id_int = int.from_bytes(bytes[1:5], "big")
        if schema_id_int in self.schema_id_int_generalizedProtocolMessageType_protobuf_schema_str_tuple_dict:
            generalizedProtocolMessageType, protobuf_schema_str = self.schema_id_int_generalizedProtocolMessageType_protobuf_schema_str_tuple_dict[schema_id_int]
        else:
            generalizedProtocolMessageType, protobuf_schema_str = self.schema_id_int_to_generalizedProtocolMessageType_protobuf_schema_str_tuple(schema_id_int)
            self.schema_id_int_generalizedProtocolMessageType_protobuf_schema_str_tuple_dict[schema_id_int] = (generalizedProtocolMessageType, protobuf_schema_str)
        #
        protobufDeserializer = ProtobufDeserializer(generalizedProtocolMessageType, {"use.deprecated.format": False})
        protobuf_message = protobufDeserializer(bytes, None)
        dict = MessageToDict(protobuf_message)
        return dict

    def bytes_avro_to_dict(self, bytes):
        schema_id_int = int.from_bytes(bytes[1:5], "big")
        schema_dict = self.schemaRegistry.get_schema(schema_id_int)
        schema_str = schema_dict["schema_str"]
        #
        avroDeserializer = AvroDeserializer(self.schemaRegistry.schemaRegistryClient, schema_str)
        dict = avroDeserializer(bytes, None)
        return dict

    def bytes_jsonschema_to_dict(self, bytes):
        schema_id_int = int.from_bytes(bytes[1:5], "big")
        schema_dict = self.schemaRegistry.get_schema(schema_id_int)
        schema_str = schema_dict["schema_str"]
        #
        jsonDeserializer = JSONDeserializer(schema_str)
        dict = jsonDeserializer(bytes, None)
        return dict

#

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
