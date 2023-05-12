from kashpy.helpers import post, get_auth_str_tuple

# Constants

CURRENT_TIME = 0
RD_KAFKA_PARTITION_UA = -1

#

class RestProxyProducer:
    def __init__(self, rest_proxy_config_dict, schema_registry_config_dict, kash_config_dict, cluster_id_str, topics, key_type="str", value_type="str", key_schema=None, value_schema=None):
        self.rest_proxy_config_dict = rest_proxy_config_dict
        self.schema_registry_config_dict = schema_registry_config_dict
        self.kash_config_dict = kash_config_dict
        #
        self.cluster_id_str = cluster_id_str
        #
        self.topic_str_list = topics if isinstance(topics, list) else [topics]
        #
        if isinstance(key_type, dict):
            self.key_type_dict = key_type
        else:
            self.key_type_dict = {topic_str: key_type for topic_str in self.topic_str_list}
        #
        if isinstance(value_type, dict):
            self.value_type_dict = value_type
        else:
            self.value_type_dict = {topic_str: value_type for topic_str in self.topic_str_list}
        #
        if isinstance(key_schema, dict):
            self.key_schema_dict = key_schema
        else:
            self.key_schema_dict = {topic_str: key_schema for topic_str in self.topic_str_list}
        #
        if isinstance(value_schema, dict):
            self.value_schema_dict = value_schema
        else:
            self.value_schema_dict = {topic_str: value_schema for topic_str in self.topic_str_list}
        #
        self.topic_str_counter_int_dict = {topic_str: 0 for topic_str in self.topic_str_list}

    #

    def write(self, value, key=None, partition=RD_KAFKA_PARTITION_UA, timestamp=CURRENT_TIME, headers=None):
        return self.produce(value, key, partition, timestamp, headers)

    #

    def produce(self, value, key=None, partition=RD_KAFKA_PARTITION_UA, timestamp=CURRENT_TIME, headers=None):
        partition_int = partition
        #
        rest_proxy_url_str = self.rest_proxy_config_dict["rest.proxy.url"]
        auth_str_tuple = self.get_auth_str_tuple()
        #
        for topic_str in self.topic_str_list:
            url_str = f"{rest_proxy_url_str}/v3/clusters/{self.cluster_id_str}/topics/{topic_str}/records"
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
            self.topic_str_counter_int_dict[topic_str] += 1
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
