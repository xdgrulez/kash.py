from kashpy.helpers import post, get_auth_str_tuple

import json

# Constants

CURRENT_TIME = 0
RD_KAFKA_PARTITION_UA = -1

#

class RestProxyProducer:
    def __init__(self, rest_proxy_config_dict, schema_registry_config_dict, kash_config_dict, cluster_id_str, topic, **kwargs):
        self.rest_proxy_config_dict = rest_proxy_config_dict
        self.schema_registry_config_dict = schema_registry_config_dict
        self.kash_config_dict = kash_config_dict
        #
        self.cluster_id_str = cluster_id_str
        #
        self.topic_str = topic
        #
        self.key_type_str = kwargs["key_type"] if "key_type" in kwargs else "json"
        self.value_type_str = kwargs["value_type"] if "value_type" in kwargs else "json"
        #
        self.key_schema_str = kwargs["key_schema"] if "key_schema" in kwargs else None
        self.value_schema_str = kwargs["value_schema"] if "value_schema" in kwargs else None
        #
        self.key_schema_id_int = kwargs["key_schema_id"] if "key_schema_id" in kwargs else None
        self.value_schema_id_int = kwargs["value_schema_id"] if "value_schema_id" in kwargs else None
        #
        self.produced_counter_int = 0

    #

    def write(self, value, **kwargs):
        return self.produce(value, **kwargs)

    #

    def produce(self, value, **kwargs):
        key = kwargs["key"] if "key" in kwargs else None
        partition_int = kwargs["partition"] if "partition" in kwargs else RD_KAFKA_PARTITION_UA
        #
        rest_proxy_url_str = self.rest_proxy_config_dict["rest.proxy.url"]
        auth_str_tuple = self.get_auth_str_tuple()
        #
        url_str = f"{rest_proxy_url_str}/v3/clusters/{self.cluster_id_str}/topics/{self.topic_str}/records"
        #
        headers_dict = {"Content-Type": "application/json"}
        if self.value_type_str.lower() == "json":
            type_str = "JSON"
            value = json.loads(value)
        elif self.value_type_str.lower() == "avro":
            type_str = "AVRO"
            value = json.loads(value)
        elif self.value_type_str.lower() in ["pb", "protobuf"]:
            type_str = "PROTOBUF"
            value = json.loads(value)
        elif self.value_type_str.lower() == "jsonschema":
            type_str = "JSONSCHEMA"
            value = json.loads(value)
        else:
            type_str = "BINARY"
        #
        if self.value_schema_id_int is not None:
            payload_dict = {"value": {"schema_id": self.value_schema_id_int, "data": value}}
        elif self.value_schema_str is not None:
            payload_dict = {"value": {"type": type_str, "schema": self.value_schema_str, "data": value}}
        else:
            payload_dict = {"value": {"type": type_str, "data": value}}
        #
        if key is not None:
            if self.key_type_str.lower() == "json":
                type_str = "JSON"
                key = json.loads(key)
            elif self.key_type_str.lower() == "avro":
                type_str = "AVRO"
                key = json.loads(key)
            elif self.key_type_str.lower() in ["pb", "protobuf"]:
                type_str = "PROTOBUF"
                key = json.loads(key)
            elif self.key_type_str.lower() == "jsonschema":
                type_str = "JSONSCHEMA"
                key = json.loads(key)
            else:
                type_str = "BINARY"
            #
            if self.key_schema_id_int is not None:
                payload_dict["key"] = {"schema_id": self.key_schema_id_int, "data": key}
            elif self.key_schema_str is not None:
                payload_dict["key"] = {"type": type_str, "schema": self.key_schema_str, "data": key}
            else:
                payload_dict["key"] = {"type": type_str, "data": key}
        #
        if partition_int != RD_KAFKA_PARTITION_UA:
            payload_dict["partition_id"] = partition_int
        #
        post(url_str, headers_dict, payload_dict, auth_str_tuple=auth_str_tuple)
        #
        self.produced_counter_int += 1
        #
        return key, value

    #

    def get_auth_str_tuple(self):
        if "basic.auth.user.info" in self.rest_proxy_config_dict:
            return tuple(self.rest_proxy_config_dict["basic.auth.user.info"].split(":"))
        else:
            return None
