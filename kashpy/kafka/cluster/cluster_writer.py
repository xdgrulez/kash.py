import importlib
import json
import os
import sys
import tempfile

from google.protobuf.json_format import ParseDict

from confluent_kafka import Producer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

from kashpy.kafka.kafka_writer import KafkaWriter
from kashpy.kafka.schemaregistry import SchemaRegistry

# Constants

CURRENT_TIME = 0
RD_KAFKA_PARTITION_UA = -1

#

class ClusterWriter(KafkaWriter):
    def __init__(self, kafka_obj, topic, **kwargs):
        super().__init__(kafka_obj, topic, **kwargs)
        #
        self.kafka_config_dict = kafka_obj.kafka_config_dict
        self.config_str = kafka_obj.config_str
        #
        self.schema_hash_int_generalizedProtocolMessageType_dict = {}
        #
        self.on_delivery_function = kwargs["on_delivery"] if "on_delivery" in kwargs else None
        #
        if "schema.registry.url" in self.schema_registry_config_dict:
            self.schemaRegistry = SchemaRegistry(self.schema_registry_config_dict, self.kash_config_dict)
        else:
            self.schemaRegistry = None
        #
        self.kafka_config_dict.pop("group.id", None)
        #
        self.producer = Producer(self.kafka_config_dict)

    def __del__(self):
        self.flush()

    #

    def close(self):
        self.flush()
        return self.topic_str

    #

    def flush(self):
        self.producer.flush(self.kash_config_dict["flush.timeout"])
        #
        return self.topic_str

    def produce(self, value, **kwargs):
        key = kwargs["key"] if "key" in kwargs else None
        partition = kwargs["partition"] if "partition" in kwargs and kwargs["partition"] is not None else RD_KAFKA_PARTITION_UA
        timestamp = kwargs["timestamp"] if "timestamp" in kwargs and kwargs["timestamp"] is not None else CURRENT_TIME
        headers = kwargs["headers"] if "headers" in kwargs else None
        #
        value_list = value if isinstance(value, list) else [value]
        #
        key_list = key if isinstance(key, list) else [key for _ in value_list]
        partition_int_list = partition if isinstance(partition, list) else [partition for _ in value_list]
        timestamp_int_list = timestamp if isinstance(timestamp, list) else [timestamp for _ in value_list]
        headers_str_bytes_tuple_list_list = headers if isinstance(headers, list) else [headers for _ in value_list]
        #

        def serialize(key_bool, normalize_schemas=False):
            type_str = self.key_type_str if key_bool else self.value_type_str
            schema_str = self.key_schema_str if key_bool else self.value_schema_str
            schema_id_int = self.key_schema_id_int if key_bool else self.value_schema_id_int
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
            def get_schema(schema_str):
                if schema_str is None:
                    if schema_id_int is None:
                        raise Exception("Please provide a schema or schema ID for the " + ("key" if key_bool else "value") + ".")
                    schema = self.schemaRegistry.schemaRegistryClient.get_schema(schema_id_int)
                else:
                    schema = schema_str
                return schema
            #
            if type_str.lower() == "json":
                if isinstance(payload, dict):
                    payload_str_or_bytes = json.dumps(payload)
                else:
                    payload_str_or_bytes = payload
            elif type_str.lower() in ["pb", "protobuf"]:
                schema = get_schema(schema_str)
                generalizedProtocolMessageType = self.schema_str_to_generalizedProtocolMessageType(schema, self.topic_str, key_bool, normalize_schemas)
                protobufSerializer = ProtobufSerializer(generalizedProtocolMessageType, self.schemaRegistry.schemaRegistryClient, {"use.deprecated.format": False})
                payload_dict = payload_to_payload_dict(payload)
                protobuf_message = generalizedProtocolMessageType()
                ParseDict(payload_dict, protobuf_message)
                payload_str_or_bytes = protobufSerializer(protobuf_message, SerializationContext(self.topic_str, messageField))
            elif type_str.lower() == "avro":
                schema = get_schema(schema_str)
                avroSerializer = AvroSerializer(self.schemaRegistry.schemaRegistryClient, schema)
                payload_dict = payload_to_payload_dict(payload)
                payload_str_or_bytes = avroSerializer(payload_dict, SerializationContext(self.topic_str, messageField))
            elif type_str.lower() in ["jsonschema", "json_sr"]:
                schema = get_schema(schema_str)
                jSONSerializer = JSONSerializer(schema, self.schemaRegistry.schemaRegistryClient)
                payload_dict = payload_to_payload_dict(payload)
                payload_str_or_bytes = jSONSerializer(payload_dict, SerializationContext(self.topic_str, messageField))
            else:
                payload_str_or_bytes = payload
            return payload_str_or_bytes
        #
        key_str_or_bytes_list = []
        value_str_or_bytes_list = []
        for value, key, partition_int, timestamp_int, headers_str_bytes_tuple_list in zip(value_list, key_list, partition_int_list, timestamp_int_list, headers_str_bytes_tuple_list_list):
            key_str_or_bytes = serialize(key_bool=True)
            value_str_or_bytes = serialize(key_bool=False)
            #
            self.producer.produce(self.topic_str, value_str_or_bytes, key_str_or_bytes, partition=partition_int, timestamp=timestamp_int, headers=headers_str_bytes_tuple_list, on_delivery=self.on_delivery_function)
            #
            self.produced_counter_int += 1
            #
            key_str_or_bytes_list.append(key_str_or_bytes)
            value_str_or_bytes_list.append(value_str_or_bytes)
        #
        return key_str_or_bytes_list, value_str_or_bytes_list

    # Helpers

    def schema_str_to_generalizedProtocolMessageType(self, schema_str, topic_str, key_bool, normalize_schemas=False):
        schema_hash_int = hash(schema_str)
        if schema_hash_int in self.schema_hash_int_generalizedProtocolMessageType_dict:
            generalizedProtocolMessageType = self.schema_hash_int_generalizedProtocolMessageType_dict[schema_hash_int]
        else:
            subject_name_str = self.schemaRegistry.create_subject_name_str(topic_str, key_bool)
            schema_dict = self.schemaRegistry.create_schema_dict(schema_str, "PROTOBUF")
            schema_id_int = self.schemaRegistry.register_schema(subject_name_str, schema_dict, normalize_schemas)
            #
            generalizedProtocolMessageType = self.schema_id_int_and_schema_str_to_generalizedProtocolMessageType(schema_id_int, schema_str)
            #
            self.schema_hash_int_generalizedProtocolMessageType_dict[schema_hash_int] = generalizedProtocolMessageType
        #
        return generalizedProtocolMessageType

    def schema_id_int_and_schema_str_to_generalizedProtocolMessageType(self, schema_id_int, schema_str):
        path_str = f"/{tempfile.gettempdir()}/kash.py/clusters/{self.config_str}"
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
