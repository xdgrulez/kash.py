from fnmatch import fnmatch
import json
import requests

from kashpy.kafka import Kafka

#

def get(url_str, headers_dict, payload_dict=None):
    response = requests.get(url_str, headers=headers_dict, json=payload_dict)
    if response.text == "":
        response_dict = {}
    else:
        response_dict = response.json()
    #
    if response.ok:
        return response_dict
    else:
        raise Exception(response_dict)

def delete(url_str, headers_dict):
    response = requests.delete(url_str, headers=headers_dict)
    if response.text == "":
        response_dict = {}
    else:
        response_dict = response.json()
    #
    if response.ok:
        return response_dict
    else:
        raise Exception(response_dict)

def post(url_str, headers_dict, payload_dict):
    response = requests.post(url_str, headers=headers_dict, json=payload_dict)
    if response.text == "":
        response_dict = {}
    else:
        response_dict = response.json()
    #
    if response.ok:
        return response_dict
    else:
        raise Exception(response_dict)

#

class RestProxy(Kafka):
    def __init__(self):
        super().__init__()
        #
        self.topic_str = None
        #
        self.cluster_id_str = self.get_cluster_id()

    #

    # AdminClient - ACLs

    def kafkaAcl_dict_to_dict(kafkaAcl_dict):
        dict = {"restype": kafkaAcl_dict["resource_type"],
                "name": kafkaAcl_dict["resource_name"],
                "resource_pattern_type": kafkaAcl_dict["pattern_type"],
                "principal": kafkaAcl_dict["principal"],
                "host": kafkaAcl_dict["host"],
                "operation": kafkaAcl_dict["operation"],
                "permission_type": kafkaAcl_dict["permission"]}
        return dict

    def acls(self, restype="any", name=None, resource_pattern_type="any", principal=None, host=None, operation="any", permission_type="any"):
        rest_proxy_url_str = "http://localhost:8082"
        #
        url_str = f"{rest_proxy_url_str}/v3/clusters/{self.cluster_id_str}/acls"
        headers_dict = {"Content-Type": "application/json"}
        #
        payload_dict = {"resource_type": restype, "pattern_type": resource_pattern_type, "operation": operation, "permission": permission_type}
        if name is not None:
            payload_dict["resource_name"] = name
        if principal is not None:
            payload_dict["principal"] = principal
        if host is not None:
            payload_dict["host"] = host 
        #
        response_dict = get(url_str, headers_dict, payload_dict=payload_dict)
        kafkaAcl_dict_list = response_dict["data"]
        #
        dict_list = [self.kafkaAcl_dict_to_dict(kafkaAcl_dict) for kafkaAcl_dict in kafkaAcl_dict_list]
        return dict_list

    def create_acl(self, restype="any", name=None, resource_pattern_type="any", principal=None, host=None, operation="any", permission_type="any"):
        rest_proxy_url_str = "http://localhost:8082"
        #
        url_str = f"{rest_proxy_url_str}/v3/clusters/{self.cluster_id_str}/acls"
        headers_dict = {"Content-Type": "application/json"}
        #
        payload_dict = {"resource_type": restype, "pattern_type": resource_pattern_type, "operation": operation, "permission": permission_type}
        if name is not None:
            payload_dict["resource_name"] = name
        if principal is not None:
            payload_dict["principal"] = principal
        if host is not None:
            payload_dict["host"] = host 
        #
        post(url_str, headers_dict, payload_dict=payload_dict)

    def delete_acl(self, restype="any", name=None, resource_pattern_type="any", principal=None, host=None, operation="any", permission_type="any"):
        rest_proxy_url_str = "http://localhost:8082"
        #
        url_str = f"{rest_proxy_url_str}/v3/clusters/{self.cluster_id_str}/acls"
        headers_dict = {"Content-Type": "application/json"}
        #
        payload_dict = {"resource_type": restype, "pattern_type": resource_pattern_type, "operation": operation, "permission": permission_type}
        if name is not None:
            payload_dict["resource_name"] = name
        if principal is not None:
            payload_dict["principal"] = principal
        if host is not None:
            payload_dict["host"] = host 
        #
        response_dict = delete(url_str, headers_dict, payload_dict=payload_dict)
        kafkaAcl_dict_list = response_dict["data"]
        #
        dict_list = [self.kafkaAcl_dict_to_dict(kafkaAcl_dict) for kafkaAcl_dict in kafkaAcl_dict_list]
        return dict_list

    #

    def get_cluster_id(self):
        rest_proxy_url_str = "http://localhost:8082"
        #
        url_str = f"{rest_proxy_url_str}/v3/clusters"
        headers_dict = {"Content-Type": "application/json"}
        response_dict = get(url_str, headers_dict)
        #
        cluster_id_str = response_dict["data"][0]["cluster_id"]
        return cluster_id_str
    
    #

    def list_topics(self):
        rest_proxy_url_str = "http://localhost:8082"
        #
        url_str = f"{rest_proxy_url_str}/v3/clusters/{self.cluster_id_str}/topics"
        headers_dict = {"Content-Type": "application/json"}
        response_dict = get(url_str, headers_dict)
        kafkaTopic_dict_list = response_dict["data"]
        topic_str_list = [kafkaTopic_dict["topic_name"] for kafkaTopic_dict in kafkaTopic_dict_list]
        #
        return topic_str_list

    def config(self, pattern_str_or_str_list):
        rest_proxy_url_str = "http://localhost:8082"
        #
        topic_str_list = self.topics(pattern_str_or_str_list)
        #
        def kafkaTopicConfigList_dict_to_config_dict(kafkaTopicConfigList_dict):
            config_dict = {}
            #
            kafkaTopicConfig_dict_list = kafkaTopicConfigList_dict["data"]
            for kafkaTopicConfig_dict in kafkaTopicConfig_dict_list:
                config_key_str = kafkaTopicConfig_dict["name"]
                config_value_str = kafkaTopicConfig_dict["value"]
                #
                config_dict[config_key_str] = config_value_str
            #
            return config_dict

        #
        topic_str_config_dict_dict = {topic_str: kafkaTopicConfigList_dict_to_config_dict(get(f"{rest_proxy_url_str}/v3/clusters/{self.cluster_id_str}/topics/{topic_str}/configs", None)) for topic_str in topic_str_list}
        #
        return topic_str_config_dict_dict
    
    def set_config(self, pattern_str_or_str_list, config_dict, test=False):
        rest_proxy_url_str = "http://localhost:8082"
        #
        topic_str_list = self.topics(pattern_str_or_str_list)
        #
        topic_str_config_dict_dict = {}
        for topic_str in topic_str_list:
            url_str = f"{rest_proxy_url_str}/v3/clusters/{self.cluster_id_str}/topics/{topic_str}/configs:alter"
            headers_dict = {"Content-Type": "application/json"}
            key_str_value_str_dict_list = [{"name": config_key_str, "value": config_value_str} for config_key_str, config_value_str in config_dict.items()]
            payload_dict = {"data": key_str_value_str_dict_list}
            post(url_str, headers_dict, payload_dict)
            #
            topic_str_config_dict_dict[topic_str] = config_dict
        #
        return topic_str_config_dict_dict

    #

    def create(self, topic_str, partitions=1, config={}, block=True):
        rest_proxy_url_str = "http://localhost:8082"
        #
        partitions_int = partitions
        config_dict = config
        #
        config_dict["retention.ms"] = "-1"
        #
        url_str = f"{rest_proxy_url_str}/v3/clusters/{self.cluster_id_str}/topics"
        headers_dict = {"Content-Type": "application/json"}
        configs_dict_list = [{"name": config_key_str, "value": config_value_str} for config_key_str, config_value_str in config_dict.items()]
        payload_dict = {"topic_name": topic_str, "partitions_count": partitions_int, "configs": configs_dict_list}
        post(url_str, headers_dict, payload_dict)
        #
        return topic_str

    touch = create

    def delete(self, pattern_str_or_str_list, block=True):
        rest_proxy_url_str = "http://localhost:8082"
        #
        topic_str_list = self.topics(pattern_str_or_str_list)
        #
        if topic_str_list:
            for topic_str in topic_str_list:
                url_str = f"{rest_proxy_url_str}/v3/clusters/{self.cluster_id_str}/topics/{topic_str}"
                headers_dict = {"Content-Type": "application/json"}
                delete(url_str, headers_dict)
        #
        return topic_str_list

    rm = delete

    #

    def partitions(self, pattern):
        pattern_str_or_str_list = pattern
        if pattern_str_or_str_list is None:
            pattern_str_or_str_list = ["*"]
        elif isinstance(pattern_str_or_str_list, str):
            pattern_str_or_str_list = [pattern_str_or_str_list]
        #
        rest_proxy_url_str = "http://localhost:8082"
        #
        url_str = f"{rest_proxy_url_str}/v3/clusters/{self.cluster_id_str}/topics"
        headers_dict = {"Content-Type": "application/json"}
        response_dict = get(url_str, headers_dict)
        kafkaTopic_dict_list = response_dict["data"]
        topic_str_num_partitions_int_dict = {kafkaTopic_dict["topic_name"]: kafkaTopic_dict["partitions_count"] for kafkaTopic_dict in kafkaTopic_dict_list if any(fnmatch(kafkaTopic_dict["topic_name"], pattern_str) for pattern_str in pattern_str_or_str_list)}
        #
        return topic_str_num_partitions_int_dict

    def watermarks(self, pattern, timeout=-1.0):
        rest_proxy_url_str = "http://localhost:8082"
        #
        topic_str_num_partitions_int_dict = self.partitions(pattern)
        #
        topic_str_partition_int_offsets_tuple_dict_dict = {}
        for topic_str, num_partitions_int in topic_str_num_partitions_int_dict.items():
            topic_str_partition_int_offsets_tuple_dict_dict[topic_str] = {}
            for partition_int in range(num_partitions_int):
                url_str = f"{rest_proxy_url_str}/topics/{topic_str}/partitions/{partition_int}/offsets"
                headers_dict = {"Content-Type": "application/vnd.kafka.v2+json"}
                response_dict = get(url_str, headers_dict)
                topic_str_partition_int_offsets_tuple_dict_dict[topic_str][partition_int] = (response_dict["beginning_offset"], response_dict["end_offset"])
        #
        return topic_str_partition_int_offsets_tuple_dict_dict

    # Consumer

    def subscribe(self, topics, group=None, offsets=None, config={}, key_type="str", value_type="str"):
        # topics
        topic_str_list = topics if isinstance(topics, list) else [topics]
        if not topic_str_list:
            raise Exception("No topic to subscribe to.")
        # group
        if group is None:
            group_str = self.create_unique_group_id()
        else:
            group_str = group
        # key_type
        if isinstance(key_type, dict):
            key_type_dict = key_type
        else:
            key_type_dict = {topic_str: key_type for topic_str in topic_str_list}
        # value_type
        if isinstance(value_type, dict):
            value_type_dict = value_type
        else:
            value_type_dict = {topic_str: value_type for topic_str in topic_str_list}
        rest_proxy_url_str = "http://localhost:8082"
        #
        url_str1 = f"{rest_proxy_url_str}/consumers/{group_str}"
        headers_dict1 = {"Content-Type": "application/vnd.kafka.v2+json"}
        subscription_id_int = self.create_subscription_id()
        payload_dict1 = {"name": str(subscription_id_int), "format": "json", "auto.offset.reset": "earliest", "auto.commit.enable": "true", "fetch.min.bytes": 1, "consumer.request.timeout.ms": 1000}
        post(url_str1, headers_dict1, payload_dict1)
        #
        url_str2 = f"{rest_proxy_url_str}/consumers/{group_str}/instances/{subscription_id_int}/subscription"
        headers_dict2 = {"Content-Type": "application/vnd.kafka.v2+json"}
        payload_dict2 = {"topics": topic_str_list}
        post(url_str2, headers_dict2, payload_dict2)
        #
        self.subscription_dict[subscription_id_int] = {"topics": topic_str_list, "group": group_str, "key_type": key_type_dict, "value_type": value_type_dict, "last_key_schema": None, "last_value_schema": None}
        #
        return topic_str_list, group_str, subscription_id_int

    def consume(self, n=1, sub=None):
        subscription_id_int = self.get_subscription_id(sub)
        subscription = self.subscription_dict[subscription_id_int]
        #
        rest_proxy_url_str = "http://localhost:8082"
        timeout_int = 1000
        max_bytes_int = 100000
        #
        group_str = subscription["group"]
        url_str = f"{rest_proxy_url_str}/consumers/{group_str}/instances/{subscription_id_int}/records?timeout={timeout_int}&max_bytes={max_bytes_int}"
        headers_dict = {"Accept": "application/vnd.kafka.json.v2+json"}
        response_dict = get(url_str, headers_dict)
        #
        message_dict_list = [{"headers": None, "topic": rest_message_dict["topic"], "partition": rest_message_dict["partition"], "offset": rest_message_dict["offset"], "timestamp": None, "key": rest_message_dict["key"], "value": rest_message_dict["value"]} for rest_message_dict in response_dict]
        return message_dict_list

    def offsets_dict_to_rest_offsets_dict_list(self, offsets_dict, topic_str_list):
        str_or_int = list(offsets_dict.keys())[0]
        if isinstance(str_or_int, str):
            topic_str_offsets_dict_dict = offsets_dict
        elif isinstance(str_or_int, int):
            topic_str_offsets_dict_dict = {topic_str: offsets_dict for topic_str in topic_str_list}
        #
        rest_offsets_dict_list = [{"topic": topic_str, "partition": partition_int, "offset": offset_int} for topic_str, offsets_dict in topic_str_offsets_dict_dict.items() for partition_int, offset_int in offsets_dict.items()]
        return rest_offsets_dict_list

    def commit(self, offsets=None, asynchronous=False, sub=None):
        offsets_dict = offsets
        #
        subscription_id_int = self.get_subscription_id(sub)
        subscription = self.subscription_dict[subscription_id_int]
        #
        rest_proxy_url_str = "http://localhost:8082"
        #
        group_str = subscription["group"]
        headers_dict = {"Content-Type": "application/vnd.kafka.v2+json"}
        #
        if offsets is None:
            payload_dict = None
        else:
            rest_offsets_dict_list = self.offsets_dict_to_rest_offsets_dict_list(offsets_dict, subscription["topics"])
            payload_dict = {"offsets": rest_offsets_dict_list}
        #
        url_str = f"{rest_proxy_url_str}/consumers/{group_str}/instances/{subscription_id_int}/offsets"
        response_dict = post(url_str, headers_dict, payload_dict)
        #
        return response_dict

    def offsets(self, timeout=-1.0, sub=None):
        rest_proxy_url_str = "http://localhost:8082"
        #
        subscription_id_int = self.get_subscription_id(sub)
        subscription = self.subscription_dict[subscription_id_int]
        group_str = subscription["group"]
        #
        url_str1 = f"{rest_proxy_url_str}/consumers/{group_str}/instances/{subscription_id_int}/assignments"
        headers_dict1 = {"Accept": "application/vnd.kafka.v2+json"}
        response_dict1 = get(url_str1, headers_dict1)
# {
#   "partitions": [
#     {
#       "topic": "test",
#       "partition": 0
#     },
#     {
#       "topic": "test",
#       "partition": 1
#     }

#   ]
# }
        #
        url_str2 = f"{rest_proxy_url_str}/consumers/{group_str}/instances/{subscription_id_int}/offsets"
        headers_dict2 = {"Content-Type": "application/vnd.kafka.v2+json"}
        response_dict2 = get(url_str2, headers_dict2, response_dict1)
# {"offsets":
#  [
#   {
#     "topic": "test",
#     "partition": 0,
#     "offset": 21,
#     "metadata":""
#   },
#   {
#     "topic": "test",
#     "partition": 1,
#     "offset": 31,
#     "metadata":""
#   }
#  ]
# }
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
 
    def unsubscribe(self, sub=None):
        subscription_id_int = self.get_subscription_id(sub)
        subscription = self.subscription_dict[subscription_id_int]
        #
        rest_proxy_url_str = "http://localhost:8082"
        #
        group_str = subscription["group"]
        url_str = f"{rest_proxy_url_str}/consumers/{group_str}/instances/{subscription_id_int}/subscription"
        headers_dict = {"Content-Type": "application/vnd.kafka.v2+json"}
        delete(url_str, headers_dict)
        #
        topic_str_list = subscription["topics"]
        self.subscription_dict.pop(subscription_id_int)
        #
        return topic_str_list, group_str, subscription_id_int

    def close(self, sub=None):
        subscription_id_int = self.get_subscription_id(sub)
        #
        topic_str_list, group_str, subscription_id_int = self.unsubscribe(sub)
        #
        rest_proxy_url_str = "http://localhost:8082"
        #
        url_str = f"{rest_proxy_url_str}/consumers/{group_str}/instances/{subscription_id_int}"
        headers_dict = {"Content-Type": "application/vnd.kafka.v2+json"}
        delete(url_str, headers_dict)
        #
        return topic_str_list, group_str, subscription_id_int

    # Producer

    # def write(self, value:Any, key:Any=None, key_type:str="str", value_type:str="str", key_schema:str=None, value_schema:str=None, partition:int=RD_KAFKA_PARTITION_UA, timestamp:int=CURRENT_TIME, headers:Union[Dict, List]=None, on_delivery:Callable[[KafkaError, Message], None]=None):
    #     return self.produce(self.topic_str, value, key, key_type, value_type, key_schema, value_schema, partition, timestamp, headers, on_delivery)

    # def write(self, value:Any, key:Any=None):
    #     return produce(value, key)

    #

    def produce(self, topic, value, key=None):
        topic_str = topic
        #
        rest_proxy_url_str = "http://localhost:8082"
        #
        url_str = f"{rest_proxy_url_str}/v3/clusters/{self.cluster_id_str}/topics/{topic_str}/records"
        headers_dict = {"Content-Type": "application/json"}
        #
        rest_message_dict = {"value": {"type": "JSON", "data": value}}
        if key is not None:
            rest_message_dict["key"] = {"type": "JSON", "data": key}
        #
        post(url_str, headers_dict, rest_message_dict)
        return key, value
