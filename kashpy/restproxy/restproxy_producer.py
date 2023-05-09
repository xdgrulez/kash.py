from kashpy.kafka_producer import KafkaProducer
from kashpy.helpers import post, get_auth_str_tuple

# Constants

CURRENT_TIME = 0
RD_KAFKA_PARTITION_UA = -1

#

class RestProxyProducer(KafkaProducer):
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

    def produce(self, value, key=None, partition=RD_KAFKA_PARTITION_UA, timestamp=CURRENT_TIME, headers=None):
        partition_int = partition
        #
        rest_proxy_url_str = self.rest_proxy_config_dict["rest.proxy.url"]
        auth_str_tuple = self.get_auth_str_tuple()
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
        post(url_str, headers_dict, payload_dict, auth_str_tuple=auth_str_tuple)
        return key, value

    #

    def get_auth_str_tuple(self):
        if "basic.auth.user.info" in self.rest_proxy_config_dict:
            auth_str_tuple = get_auth_str_tuple(self.rest_proxy_config_dict["basic.auth.user.info"])
        else:
            auth_str_tuple = None
        #
        return auth_str_tuple
