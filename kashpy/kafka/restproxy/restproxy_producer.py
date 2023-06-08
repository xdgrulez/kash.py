from kashpy.helpers import post, get_auth_str_tuple

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
        self.key_type_str = kwargs["key_type"] if "key_type" in kwargs else "str"
        self.value_type_str = kwargs["value_type"] if "value_type" in kwargs else "str"
        #
        self.key_schema_str = kwargs["key_schema"] if "key_schema" in kwargs else None
        self.value_schema_str = kwargs["value_schema"] if "value_schema" in kwargs else None
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
        payload_dict = {"value": {"type": "JSON", "data": value}}
        if key is not None:
            payload_dict["key"] = {"type": "JSON", "data": key}
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
            auth_str_tuple = get_auth_str_tuple(self.rest_proxy_config_dict["basic.auth.user.info"])
        else:
            auth_str_tuple = None
        #
        return auth_str_tuple
