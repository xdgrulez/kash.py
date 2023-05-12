from kashpy.helpers import get, delete, post, get_auth_str_tuple, get_millis

#

class RestProxyConsumer:
    def __init__(self, rest_proxy_config_dict, schema_registry_config_dict, kash_config_dict, cluster_id_str, topics, group=None, offsets=None, config={}, key_type="str", value_type="str"):
        self.rest_proxy_config_dict = rest_proxy_config_dict
        self.schema_registry_config_dict = schema_registry_config_dict
        self.kash_config_dict = kash_config_dict
        #
        self.cluster_id_str = cluster_id_str
        #
        self.topic_str_list = topics if isinstance(topics, list) else [topics]
        if not self.topic_str_list:
            raise Exception("No topic to subscribe to.")
        #
        self.instance_id_str = str(get_millis())
        #
        if group is None:
            self.group_str = str(get_millis())
        else:
            self.group_str = group
        #
        if offsets is None:
            self.offsets_dict = None
        else:
            str_or_int = list(offsets.keys())[0]
            if isinstance(str_or_int, int):
                self.offsets_dict = {topic_str: self.offsets for topic_str in self.topic_str_list}
        #
        self.config_dict = {}
        self.config_dict["auto.offset.reset"] = self.kash_config_dict["auto.offset.reset"]
        #
        self.config_dict["auto.commit.enable"] = self.kash_config_dict["auto.commit.enable"]
        self.config_dict["fetch.min.bytes"] = self.kash_config_dict["fetch.min.bytes"]
        self.config_dict["consumer.request.timeout.ms"] = self.kash_config_dict["consumer.request.timeout.ms"]
        for key_str, value in config.items():
            self.config_dict[key_str] = value
        #
        if isinstance(key_type, dict):
            self.key_type_dict = key_type
        else:
            self.key_type_dict = {topic_str: key_type for topic_str in self.topic_str_list}
        #
        self.value_type_dict = value_type
        if isinstance(value_type, dict):
            self.value_type_dict = value_type
        else:
            self.value_type_dict = {topic_str: value_type for topic_str in self.topic_str_list}
        #
        self.schema_id_int_generalizedProtocolMessageType_protobuf_schema_str_tuple_dict = {}
        #
        self.subscribe()

    #

    def read(self, n=1):
        return self.consume(n)

    #

    def subscribe(self):
        rest_proxy_url_str = self.rest_proxy_config_dict["rest.proxy.url"]
        auth_str_tuple = self.get_auth_str_tuple()
        #
        url_str1 = f"{rest_proxy_url_str}/consumers/{self.group_str}"
        headers_dict1 = {"Content-Type": "application/vnd.kafka.v2+json"}
        payload_dict1 = {"name": self.instance_id_str, "format": "json", "fetch.min.bytes": self.config_dict["fetch.min.bytes"], "consumer.request.timeout.ms": self.config_dict["consumer.request.timeout.ms"]}
        payload_dict1.update(self.config_dict)
        post(url_str1, headers_dict1, payload_dict1, auth_str_tuple=auth_str_tuple)
        #
        url_str2 = f"{rest_proxy_url_str}/consumers/{self.group_str}/instances/{self.instance_id_str}/subscription"
        headers_dict2 = {"Content-Type": "application/vnd.kafka.v2+json"}
        payload_dict2 = {"topics": self.topic_str_list}
        post(url_str2, headers_dict2, payload_dict2, auth_str_tuple=auth_str_tuple)
        #
        return self.topic_str_list, self.group_str
    
    def unsubscribe(self):
        rest_proxy_url_str = self.rest_proxy_config_dict["rest.proxy.url"]
        auth_str_tuple = self.get_auth_str_tuple()
        #
        url_str = f"{rest_proxy_url_str}/consumers/{self.group_str}/instances/{self.instance_id_str}/subscription"
        headers_dict = {"Content-Type": "application/vnd.kafka.v2+json"}
        delete(url_str, headers_dict, auth_str_tuple=auth_str_tuple)
        #
        return self.topic_str_list, self.group_str

    def close(self):
        rest_proxy_url_str = self.rest_proxy_config_dict["rest.proxy.url"]
        auth_str_tuple = self.get_auth_str_tuple()
        #
        url_str = f"{rest_proxy_url_str}/consumers/{self.group_str}/instances/{self.instance_id_str}"
        headers_dict = {"Content-Type": "application/vnd.kafka.v2+json"}
        delete(url_str, headers_dict, auth_str_tuple=auth_str_tuple)
        #
        return self.topic_str_list, self.group_str

    #

    def consume(self, n=1, timeout=None, max_bytes=None):
        timeout_int = timeout
        max_bytes_int = max_bytes
        #
        rest_proxy_url_str = self.rest_proxy_config_dict["rest.proxy.url"]
        auth_str_tuple = self.get_auth_str_tuple()
        #
        if timeout_int is None:
            timeout_int = self.config_dict["consumer.request.timeout.ms"]
        #
        if max_bytes_int is None:
            max_bytes_int = 67108864
        #
        url_str = f"{rest_proxy_url_str}/consumers/{self.group_str}/instances/{self.instance_id_str}/records?timeout={timeout_int}&max_bytes={max_bytes_int}"
        headers_dict = {"Accept": "application/vnd.kafka.json.v2+json"}
        response_dict = get(url_str, headers_dict, auth_str_tuple=auth_str_tuple)
        #
        message_dict_list = [{"headers": None, "topic": rest_message_dict["topic"], "partition": rest_message_dict["partition"], "offset": rest_message_dict["offset"], "timestamp": None, "key": rest_message_dict["key"], "value": rest_message_dict["value"]} for rest_message_dict in response_dict]
        #
        return message_dict_list

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
        response_dict = post(url_str, headers_dict, payload_dict, auth_str_tuple=auth_str_tuple)
        #
        return response_dict

    def offsets(self):
        rest_proxy_url_str = self.rest_proxy_config_dict["rest.proxy.url"]
        auth_str_tuple = self.get_auth_str_tuple()
        #
        url_str1 = f"{rest_proxy_url_str}/consumers/{self.group_str}/instances/{self.instance_id_str}/assignments"
        headers_dict1 = {"Accept": "application/vnd.kafka.v2+json"}
        response_dict1 = get(url_str1, headers_dict1, auth_str_tuple=auth_str_tuple)
        #
        url_str2 = f"{rest_proxy_url_str}/consumers/{self.group_str}/instances/{self.instance_id_str}/offsets"
        headers_dict2 = {"Content-Type": "application/vnd.kafka.v2+json"}
        response_dict2 = get(url_str2, headers_dict2, response_dict1, auth_str_tuple=auth_str_tuple)
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
            auth_str_tuple = get_auth_str_tuple(self.rest_proxy_config_dict["basic.auth.user.info"])
        else:
            auth_str_tuple = None
        #
        return auth_str_tuple

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
