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

from kashpy.schemaregistry import SchemaRegistry

# Constants

CURRENT_TIME = 0
RD_KAFKA_PARTITION_UA = -1

#

class ClusterProducer:
    def __init__(self, kafka_config_dict, schema_registry_config_dict, kash_config_dict, config_str, topic, **kwargs):
        self.kafka_config_dict = kafka_config_dict
        self.schema_registry_config_dict = schema_registry_config_dict
        self.kash_config_dict = kash_config_dict
        self.config_str = config_str
        #
        self.topic_str = topic
        #
        self.key_type_str = kwargs["key_type"] if "key_type" in kwargs else "str"
        self.value_type_str = kwargs["value_type"] if "value_type" in kwargs else "str"
        #
        self.key_schema_str = kwargs["key_schema"] if "key_schema" in kwargs else None
        self.value_schema_str = kwargs["value_schema"] if "value_schema" in kwargs else None
        #
        self.on_delivery_function = kwargs["on_delivery"] if "on_delivery" in kwargs else None
        #
        self.produced_counter_int = 0
        #
        self.schema_registry = SchemaRegistry(schema_registry_config_dict, kash_config_dict)
        #
        self.producer = Producer(self.kafka_config_dict)

    def __del__(self):
        self.flush()

    #

    def write(self, value, **kwargs):
        return self.produce(value, **kwargs)

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
        partition_int = kwargs["partition"] if "partition" in kwargs else RD_KAFKA_PARTITION_UA
        timestamp_int = kwargs["timestamp"] if "timestamp" in kwargs else CURRENT_TIME
        headers_dict_or_list = kwargs["headers"] if "headers" in kwargs else None
        #

        def serialize(key_bool, normalize_schemas=False):
            type_str = self.key_type_str if key_bool else self.value_type_str
            schema_str = self.key_schema_str if key_bool else self.value_schema_str
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
                generalizedProtocolMessageType = self.schema_str_to_generalizedProtocolMessageType(schema_str, self.topic_str, key_bool, normalize_schemas)
                protobufSerializer = ProtobufSerializer(generalizedProtocolMessageType, self.schemaRegistry.schemaRegistryClient, {"use.deprecated.format": False})
                payload_dict = payload_to_payload_dict(payload)
                protobuf_message = generalizedProtocolMessageType()
                ParseDict(payload_dict, protobuf_message)
                payload_str_or_bytes = protobufSerializer(protobuf_message, SerializationContext(self.topic_str, messageField))
            elif type_str.lower() == "avro":
                avroSerializer = AvroSerializer(self.schemaRegistryClient.schemaRegistryClient, schema_str)
                payload_dict = payload_to_payload_dict(payload)
                payload_str_or_bytes = avroSerializer(payload_dict, SerializationContext(self.topic_str, messageField))
            elif type_str.lower() == "jsonschema":
                jSONSerializer = JSONSerializer(schema_str, self.schemaRegistryClient.schemaRegistryClient)
                payload_dict = payload_to_payload_dict(payload)
                payload_str_or_bytes = jSONSerializer(payload_dict, SerializationContext(self.topic_str, messageField))
            else:
                payload_str_or_bytes = payload
            return payload_str_or_bytes
        #
        key_str_or_bytes = serialize(key_bool=True)
        value_str_or_bytes = serialize(key_bool=False)
        #
        self.producer.produce(self.topic_str, value_str_or_bytes, key_str_or_bytes, partition=partition_int, timestamp=timestamp_int, headers=headers_dict_or_list, on_delivery=self.on_delivery_function)
        #
        self.produced_counter_int += 1
        #
        return key_str_or_bytes, value_str_or_bytes

    # helpers

    def schema_str_to_generalizedProtocolMessageType(self, schema_str, topic_str, key_bool, normalize_schemas=False):
        subject_name_str = self.schemaRegistry.create_subject_name_str(topic_str, key_bool)
        schema_dict = self.schemaRegistry.create_schema_dict(schema_str, "PROTOBUF")
        schema_dict = self.schemaRegistry.register_schema(subject_name_str, schema_dict, normalize_schemas)
        #
        generalizedProtocolMessageType = self.schema_id_int_and_schema_str_to_generalizedProtocolMessageType(schema_dict["schema_id"], schema_str)
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
