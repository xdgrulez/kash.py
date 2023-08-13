from kashpy.kafka.kafka_reader import KafkaReader

from kashpy.helpers import get, delete, post

#

class RestProxyReader(KafkaReader):
    def __init__(self, restproxy_obj, *topics, **kwargs):
        super().__init__(restproxy_obj, *topics, **kwargs)
        #
        self.rest_proxy_config_dict = restproxy_obj.rest_proxy_config_dict
        #
        self.cluster_id_str = restproxy_obj.cluster_id_str
        #
        for topic_str in self.topic_str_list:
            key_type_str = self.key_type_dict[topic_str]
            value_type_str = self.value_type_dict[topic_str]
            #
            if key_type_str != value_type_str:
                raise f"Cannot set up consumer for topic '{topic_str}': Confluent REST Proxy requires and key and value types to be the same."
        #
        if len(set(list(self.key_type_dict.values()) + list(self.value_type_dict.values()))) > 1:
            raise f"Cannot set up consumer for topics '{self.topic_str_list}'. Confluent REST Proxy requires all key types and value types of all the topics to be the same."
        #
        # Consumer Config
        #
        self.consumer_config_dict = {}
        self.consumer_config_dict["auto.offset.reset"] = self.kash_config_dict["auto.offset.reset"]
        self.consumer_config_dict["auto.commit.enable"] = self.kash_config_dict["auto.commit.enable"]
        self.consumer_config_dict["fetch.min.bytes"] = self.kash_config_dict["fetch.min.bytes"]
        self.consumer_config_dict["consumer.request.timeout.ms"] = self.kash_config_dict["consumer.request.timeout.ms"]
        if "config" in kwargs:
            for key_str, value in kwargs["config"].items():
                self.consumer_config_dict[key_str] = value
        #
        # Instance ID
        #
        self.instance_id_str = None
        #
        self.subscribe()

    #

    def subscribe(self):
        rest_proxy_url_str = self.rest_proxy_config_dict["rest.proxy.url"]
        auth_str_tuple = self.get_auth_str_tuple()
        #
        url_str1 = f"{rest_proxy_url_str}/consumers/{self.group_str}"
        headers_dict1 = {"Content-Type": "application/vnd.kafka.v2+json"}
        #
        value_type_str = self.key_type_dict[self.topic_str_list[0]]
        if value_type_str.lower() == "json":
            type_str = "JSON"
        elif value_type_str.lower() == "avro":
            type_str = "AVRO"
        elif value_type_str.lower() in ["pb", "protobuf"]:
            type_str = "PROTOBUF"
        elif value_type_str.lower() in ["jsonschema", "json_sr"]:
            type_str = "JSONSCHEMA"
        else:
            type_str = "BINARY"
        #
        payload_dict1 = {"format": type_str}
        payload_dict1.update(self.consumer_config_dict)
        response_dict = post(url_str1, headers_dict1, payload_dict1, auth_str_tuple=auth_str_tuple, retries=self.kash_config_dict["requests.num.retries"])
        self.instance_id_str = response_dict["instance_id"]
        #
        url_str2 = f"{rest_proxy_url_str}/consumers/{self.group_str}/instances/{self.instance_id_str}/subscription"
        headers_dict2 = {"Content-Type": "application/vnd.kafka.v2+json"}
        payload_dict2 = {"topics": self.topic_str_list}
        response_dict = post(url_str2, headers_dict2, payload_dict2, auth_str_tuple=auth_str_tuple, retries=self.kash_config_dict["requests.num.retries"])
        #
        return self.topic_str_list, self.group_str
    
    def unsubscribe(self):
        rest_proxy_url_str = self.rest_proxy_config_dict["rest.proxy.url"]
        auth_str_tuple = self.get_auth_str_tuple()
        #
        url_str = f"{rest_proxy_url_str}/consumers/{self.group_str}/instances/{self.instance_id_str}/subscription"
        headers_dict = {"Content-Type": "application/vnd.kafka.v2+json"}
        delete(url_str, headers_dict, auth_str_tuple=auth_str_tuple, retries=self.kash_config_dict["requests.num.retries"])
        #
        return self.topic_str_list, self.group_str

    def close(self):
        self.unsubscribe()
        #
        rest_proxy_url_str = self.rest_proxy_config_dict["rest.proxy.url"]
        auth_str_tuple = self.get_auth_str_tuple()
        #
        url_str = f"{rest_proxy_url_str}/consumers/{self.group_str}/instances/{self.instance_id_str}"
        headers_dict = {"Content-Type": "application/vnd.kafka.v2+json"}
        delete(url_str, headers_dict, auth_str_tuple=auth_str_tuple, retries=self.kash_config_dict["requests.num.retries"])
        #
        return self.topic_str_list, self.group_str

    #

    def consume(self, **kwargs):
        timeout_int = kwargs["timeout"] if "timeout" in kwargs else None
        max_bytes_int = kwargs["max_bytes"] if "max_bytes" in kwargs else None 
        #
        rest_proxy_url_str = self.rest_proxy_config_dict["rest.proxy.url"]
        auth_str_tuple = self.get_auth_str_tuple()
        #
        if timeout_int is None:
            timeout_int = self.consumer_config_dict["consumer.request.timeout.ms"]
        #
        if max_bytes_int is None:
            max_bytes_int = 67108864
        #
        value_type_str = self.key_type_dict[self.topic_str_list[0]]
        if value_type_str.lower() == "json":
            type_str = "json"
        elif value_type_str.lower() == "avro":
            type_str = "avro"
        elif value_type_str.lower() in ["pb", "protobuf"]:
            type_str = "protobuf"
        elif value_type_str.lower() in ["jsonschema", "json_sr"]:
            type_str = "jsonschema"
        else:
            type_str = "binary"
        #
        url_str = f"{rest_proxy_url_str}/consumers/{self.group_str}/instances/{self.instance_id_str}/records?timeout={timeout_int}&max_bytes={max_bytes_int}"
        headers_dict = {"Accept": f"application/vnd.kafka.{type_str}.v2+json"}
        #
        message_dict_list = []
        for _ in range(0, self.kash_config_dict["consume.num.attempts"]):
            response_dict = get(url_str, headers_dict, auth_str_tuple=auth_str_tuple, retries=self.kash_config_dict["requests.num.retries"])
            #
            message_dict_list += [{"headers": None, "topic": rest_message_dict["topic"], "partition": rest_message_dict["partition"], "offset": rest_message_dict["offset"], "timestamp": None, "key": rest_message_dict["key"], "value": rest_message_dict["value"]} for rest_message_dict in response_dict]
        #
        return message_dict_list

    #

    def commit(self, offsets=None):
        offsets_dict = offsets
        #
        rest_proxy_url_str = self.rest_proxy_config_dict["rest.proxy.url"]
        auth_str_tuple = self.get_auth_str_tuple()
        #
        headers_dict = {"Content-Type": "application/vnd.kafka.v2+json"}
        #
        if offsets is None:
            payload_dict = None
        else:
            rest_offsets_dict_list = offsets_dict_to_rest_offsets_dict_list(offsets_dict, self.topic_str_list)
            payload_dict = {"offsets": rest_offsets_dict_list}
        #
        url_str = f"{rest_proxy_url_str}/consumers/{self.group_str}/instances/{self.instance_id_str}/offsets"
        response_dict = post(url_str, headers_dict, payload_dict, auth_str_tuple=auth_str_tuple, retries=self.kash_config_dict["requests.num.retries"])
        #
        return response_dict

    def offsets(self):
        rest_proxy_url_str = self.rest_proxy_config_dict["rest.proxy.url"]
        auth_str_tuple = self.get_auth_str_tuple()
        #
        url_str1 = f"{rest_proxy_url_str}/consumers/{self.group_str}/instances/{self.instance_id_str}/assignments"
        headers_dict1 = {"Accept": "application/vnd.kafka.v2+json"}
        response_dict1 = get(url_str1, headers_dict1, auth_str_tuple=auth_str_tuple, retries=self.kash_config_dict["requests.num.retries"])
        #
        url_str2 = f"{rest_proxy_url_str}/consumers/{self.group_str}/instances/{self.instance_id_str}/offsets"
        headers_dict2 = {"Content-Type": "application/vnd.kafka.v2+json"}
        response_dict2 = get(url_str2, headers_dict2, response_dict1, auth_str_tuple=auth_str_tuple, retries=self.kash_config_dict["requests.num.retries"])
        #
        offsets_dict = {}
        for rest_topic_partition_offset_dict in response_dict2["offsets"]:
            topic_str = rest_topic_partition_offset_dict["topic"]
            partition_int = rest_topic_partition_offset_dict["partition"]
            offset_int = rest_topic_partition_offset_dict["offset"]
            #
            if topic_str in offsets_dict:
                partition_int_offset_int_dict = offsets_dict[topic_str]
            else:
                partition_int_offset_int_dict = {}
            partition_int_offset_int_dict[partition_int] = offset_int
            #
            offsets_dict[topic_str] = partition_int_offset_int_dict
        #
        return offsets_dict
    
    #

    def get_auth_str_tuple(self):
        if "basic.auth.user.info" in self.rest_proxy_config_dict:
            return tuple(self.rest_proxy_config_dict["basic.auth.user.info"].split(":"))
        else:
            return None

#

def offsets_dict_to_rest_offsets_dict_list(offsets_dict, topic_str_list):
    str_or_int = list(offsets_dict.keys())[0]
    if isinstance(str_or_int, str):
        topic_str_offsets_dict_dict = offsets_dict
    elif isinstance(str_or_int, int):
        topic_str_offsets_dict_dict = {topic_str: offsets_dict for topic_str in topic_str_list}
    #
    rest_offsets_dict_list = [{"topic": topic_str, "partition": partition_int, "offset": offset_int} for topic_str, offsets_dict in topic_str_offsets_dict_dict.items() for partition_int, offset_int in offsets_dict.items()]
    return rest_offsets_dict_list
