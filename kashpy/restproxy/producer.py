from kashpy.abstractproducer import AbstractProducer
from kashpy.helpers import post

# Constants

RD_KAFKA_PARTITION_UA = -1

#

class RestProxyProducer(AbstractProducer):
    def __init__(self, rest_proxy_config_dict, schema_registry_config_dict, kash_config_dict, cluster_id_str, topic_str, key_type_str="str", value_type_str="str", key_schema_str=None, value_schema_str=None):
        self.rest_proxy_config_dict = rest_proxy_config_dict
        self.schema_registry_config_dict = schema_registry_config_dict
        self.kash_config_dict = kash_config_dict
        #
        self.cluster_id_str = cluster_id_str
        #
        self.topic_str = topic_str
        self.key_type_str = key_type_str
        self.value_type_str = value_type_str
        self.key_schema_str = key_schema_str
        self.value_schema_str = value_schema_str
        #
        self.produced_messages_counter_int = 0

    #

    def write(self, value, key=None, partition=RD_KAFKA_PARTITION_UA):
        return self.produce(value, key, partition)

    #

    def produce(self, value, key=None, partition=RD_KAFKA_PARTITION_UA):
        partition_int = partition
        #
        rest_proxy_url_str = "http://localhost:8082"
        #
        url_str = f"{rest_proxy_url_str}/v3/clusters/{self.cluster_id_str}/topics/{self.topic_str}/records"
        #
        headers_dict = {"Content-Type": "application/json"}
        payload_dict = {"value": {"type": "JSON", "data": value}}
        if key is not None:
            payload_dict["key"] = {"type": "JSON", "data": key}
        #
        if partition_int != RD_KAFKA_PARTITION_UA:
            payload_dict["partition_id"] = partition_int
        #
        post(url_str, headers_dict, payload_dict)
        return key, value
