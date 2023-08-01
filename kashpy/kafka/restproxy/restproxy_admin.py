from fnmatch import fnmatch

from kashpy.helpers import get, delete, post, is_pattern

#

class RestProxyAdmin:
    def __init__(self, kafka_obj, cluster_id_str):
        self.rest_proxy_config_dict = kafka_obj.rest_proxy_config_dict
        self.kash_config_dict = kafka_obj.kash_config_dict
        #
        self.cluster_id_str = cluster_id_str

    # ACLs

    def acls(self, restype="any", name=None, resource_pattern_type="any", principal=None, host=None, operation="any", permission_type="any"):
        rest_proxy_url_str = self.rest_proxy_config_dict["rest.proxy.url"]
        auth_str_tuple = self.get_auth_str_tuple()
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
        response_dict = get(url_str, headers_dict, payload_dict=payload_dict, auth_str_tuple=auth_str_tuple, retries=self.kash_config_dict["requests.num.retries"])
        kafkaAcl_dict_list = response_dict["data"]
        #
        dict_list = [kafkaAcl_dict_to_dict(kafkaAcl_dict) for kafkaAcl_dict in kafkaAcl_dict_list]
        return dict_list

    def create_acl(self, restype="any", name=None, resource_pattern_type="any", principal=None, host=None, operation="any", permission_type="any"):
        rest_proxy_url_str = self.rest_proxy_config_dict["rest.proxy.url"]
        auth_str_tuple = self.get_auth_str_tuple()
        #
        url_str = f"{rest_proxy_url_str}/v3/clusters/{self.cluster_id_str}/acls"
        headers_dict = {"Content-Type": "application/json"}
        #
        payload_dict = {"resource_type": restype.upper(), "pattern_type": resource_pattern_type.upper(), "operation": operation.upper(), "permission": permission_type.upper()}
        if name is not None:
            payload_dict["resource_name"] = name
        if principal is not None:
            payload_dict["principal"] = principal
        if host is not None:
            payload_dict["host"] = host 
        #
        post(url_str, headers_dict, payload_dict=payload_dict, auth_str_tuple=auth_str_tuple, retries=self.kash_config_dict["requests.num.retries"])

    def delete_acl(self, restype="any", name=None, resource_pattern_type="any", principal=None, host=None, operation="any", permission_type="any"):
        rest_proxy_url_str = self.rest_proxy_config_dict["rest.proxy.url"]
        auth_str_tuple = self.get_auth_str_tuple()
        #
        url_str = f"{rest_proxy_url_str}/v3/clusters/{self.cluster_id_str}/acls?resource_type={restype.upper()}&pattern_type={resource_pattern_type.upper()}&operation={operation.upper()}&permission={permission_type.upper()}"
        if name is not None:
            url_str += f"&resource_name={name}"
        if principal is not None:
            url_str += f"&principal={principal}"
        if host is not None:
            url_str += f"&host={host}"         
        #
        headers_dict = {"Content-Type": "application/json"}
        response_dict = delete(url_str, headers_dict, auth_str_tuple=auth_str_tuple, retries=self.kash_config_dict["requests.num.retries"])
        kafkaAcl_dict_list = response_dict["data"]
        #
        dict_list = [kafkaAcl_dict_to_dict(kafkaAcl_dict) for kafkaAcl_dict in kafkaAcl_dict_list]
        return dict_list

    # Brokers

    def brokers(self):
        rest_proxy_url_str = self.rest_proxy_config_dict["rest.proxy.url"]
        auth_str_tuple = self.get_auth_str_tuple()
        #
        url_str = f"{rest_proxy_url_str}/v3/clusters/{self.cluster_id_str}/brokers"
        headers_dict = {"Content-Type": "application/json"}
        response_dict = get(url_str, headers_dict, auth_str_tuple=auth_str_tuple, retries=self.kash_config_dict["requests.num.retries"])
        kafkaBroker_dict_list = response_dict["data"]
        #
        broker_dict = {kafkaBroker_dict["broker_id"]: kafkaBroker_dict["host"] + ":" + str(kafkaBroker_dict["port"]) for kafkaBroker_dict in kafkaBroker_dict_list}
        #
        return broker_dict

    def broker_config(self):
        rest_proxy_url_str = self.rest_proxy_config_dict["rest.proxy.url"]
        auth_str_tuple = self.get_auth_str_tuple()
        #
        url_str = f"{rest_proxy_url_str}/v3/clusters/{self.cluster_id_str}/broker-configs"
        headers_dict = {"Content-Type": "application/json"}
        response_dict = get(url_str, headers_dict, auth_str_tuple=auth_str_tuple, retries=self.kash_config_dict["requests.num.retries"])
        kafkaClusterConfig_dict_list = response_dict["data"]
        #
        config_dict = {}
        for kafkaClusterConfig_dict in kafkaClusterConfig_dict_list:
            key_str = kafkaClusterConfig_dict["name"]
            value_str = kafkaClusterConfig_dict["value"]
            #
            config_dict[key_str] = value_str
        #
        return config_dict

    def set_broker_config(self, config_dict, test=False):
        rest_proxy_url_str = self.rest_proxy_config_dict["rest.proxy.url"]
        auth_str_tuple = self.get_auth_str_tuple()
        #
        url_str = f"{rest_proxy_url_str}/v3/clusters/{self.cluster_id_str}/broker-configs:alter"
        headers_dict = {"Content-Type": "application/json"}
        #
        dict_list = [{"name": key_str, "value": value_str} for key_str, value_str in config_dict.items()]
        payload_dict = {"data": dict_list}
        post(url_str, headers_dict, payload_dict, auth_str_tuple=auth_str_tuple, retries=self.kash_config_dict["requests.num.retries"])
        #
        return config_dict

    # Groups

    def describe_groups(self, patterns="*", state_patterns="*"):
        pattern_str_list = [patterns] if isinstance(patterns, str) else patterns
        state_pattern_str_list = [state_patterns] if isinstance(patterns, str) else state_patterns
        #
        kafkaConsumerGroup_dict_list = self.get_kafkaConsumerGroup_dict_list(pattern_str_list)
        #
        group_str_group_description_dict_dict = {kafkaConsumerGroup_dict["consumer_group_id"]: {"group_id": kafkaConsumerGroup_dict["consumer_group_id"], "is_simple_consumer_group": kafkaConsumerGroup_dict["is_simple"], "partition_assignor": kafkaConsumerGroup_dict["partition_assignor"], "state": kafkaConsumerGroup_dict["state"]} for kafkaConsumerGroup_dict in kafkaConsumerGroup_dict_list if any(fnmatch(kafkaConsumerGroup_dict["consumer_group_id"], pattern_str) for pattern_str in pattern_str_list) and any(fnmatch(kafkaConsumerGroup_dict["state"], state_pattern_str.upper()) for state_pattern_str in state_pattern_str_list)}
        #
        rest_proxy_url_str = self.rest_proxy_config_dict["rest.proxy.url"]
        auth_str_tuple = self.get_auth_str_tuple()
        #
        for group_str, group_description_dict in group_str_group_description_dict_dict.items():
            url_str = f"{rest_proxy_url_str}/v3/clusters/{self.cluster_id_str}/consumer-groups/{group_str}/consumers"
            headers_dict = {"Content-Type": "application/json"}
            response_dict = get(url_str, headers_dict, auth_str_tuple=auth_str_tuple, retries=self.kash_config_dict["requests.num.retries"])
            kafkaConsumer_dict_list = response_dict["data"]
            #
            dict_list = [{"member_id": kafkaConsumer_dict["consumer_id"], "client_id": kafkaConsumer_dict["client_id"], "host": kafkaConsumer_dict["cluster_id"], "group_instance_id": kafkaConsumer_dict["instance_id"]} for kafkaConsumer_dict in kafkaConsumer_dict_list]
            for dict in dict_list:
                consumer_id_str = dict["member_id"]
                url_str = f"{rest_proxy_url_str}/v3/clusters/{self.cluster_id_str}/consumer-groups/{group_str}/consumers/{consumer_id_str}/assignments"
                headers_dict = {"Content-Type": "application/json"}
                response_dict = get(url_str, headers_dict, auth_str_tuple=auth_str_tuple, retries=self.kash_config_dict["requests.num.retries"])
                kafkaConsumerAssignment_dict_list = response_dict["data"]
                #
                dict["topic_partitions"] = [{"error": None, "metadata": None, "offset": None, "partition": kafkaConsumerAssignment_dict["partition_id"], "topic": kafkaConsumerAssignment_dict["topic_name"]} for kafkaConsumerAssignment_dict in kafkaConsumerAssignment_dict_list]
            #
            group_description_dict["members"] = dict_list
        #
        return group_str_group_description_dict_dict

    def groups(self, patterns="*", state_patterns="*", state=False):
        pattern_str_list = [patterns] if isinstance(patterns, str) else patterns
        state_pattern_str_list = [state_patterns] if isinstance(patterns, str) else state_patterns
        state_bool = state
        #
        kafkaConsumerGroup_dict_list = self.get_kafkaConsumerGroup_dict_list(pattern_str_list)
        #
        group_str_state_str_dict = {kafkaConsumerGroup_dict["consumer_group_id"]: kafkaConsumerGroup_dict["state"] for kafkaConsumerGroup_dict in kafkaConsumerGroup_dict_list if any(fnmatch(kafkaConsumerGroup_dict["consumer_group_id"], pattern_str) for pattern_str in pattern_str_list) and any(fnmatch(kafkaConsumerGroup_dict["state"], state_pattern_str.upper()) for state_pattern_str in state_pattern_str_list)}
        #
        return group_str_state_str_dict if state_bool else list(group_str_state_str_dict.keys())

    def group_offsets(self, patterns, state_patterns="*"):
        pattern_str_list = [patterns] if isinstance(patterns, str) else patterns
        state_pattern_str_list = [state_patterns] if isinstance(patterns, str) else state_patterns
        #
        kafkaConsumerGroup_dict_list = self.get_kafkaConsumerGroup_dict_list(pattern_str_list)
        #
        group_str_list = [kafkaConsumerGroup_dict["consumer_group_id"] for kafkaConsumerGroup_dict in kafkaConsumerGroup_dict_list if any(fnmatch(kafkaConsumerGroup_dict["consumer_group_id"], pattern_str) for pattern_str in pattern_str_list) and any(fnmatch(kafkaConsumerGroup_dict["state"], state_pattern_str.upper()) for state_pattern_str in state_pattern_str_list)]
        #
        rest_proxy_url_str = self.rest_proxy_config_dict["rest.proxy.url"]
        auth_str_tuple = self.get_auth_str_tuple()
        #
        group_offsets = {}
        headers_dict = {"Content-Type": "application/json"}
        for group_str in group_str_list:
            url_str = f"{rest_proxy_url_str}/v3/clusters/{self.cluster_id_str}/consumer-groups/{group_str}/lags"
            response_dict = get(url_str, headers_dict, auth_str_tuple=auth_str_tuple, retries=self.kash_config_dict["requests.num.retries"])
            kafkaConsumerLag_dict_list = response_dict["data"]
            #
            if group_str in group_offsets:
                topic_str_partition_int_offset_int_dict_dict = group_offsets[group_str]
            else:
                topic_str_partition_int_offset_int_dict_dict = {}
            #
            for kafkaConsumerLag_dict in kafkaConsumerLag_dict_list:
                topic_str = kafkaConsumerLag_dict["topic_name"]
                partition_int = kafkaConsumerLag_dict["partition_id"]
                offset_int = kafkaConsumerLag_dict["current_offset"]
                if topic_str in topic_str_partition_int_offset_int_dict_dict:
                    partition_int_offset_int_dict = topic_str_partition_int_offset_int_dict_dict[topic_str]
                else:
                    partition_int_offset_int_dict = {}
                #
                partition_int_offset_int_dict[partition_int] = offset_int
                topic_str_partition_int_offset_int_dict_dict[topic_str] = partition_int_offset_int_dict
            #
            group_offsets[group_str] = topic_str_partition_int_offset_int_dict_dict
        #
        return group_offsets

    # Topics

    def config(self, pattern_str_or_str_list):
        rest_proxy_url_str = self.rest_proxy_config_dict["rest.proxy.url"]
        auth_str_tuple = self.get_auth_str_tuple()
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
        topic_str_config_dict_dict = {topic_str: kafkaTopicConfigList_dict_to_config_dict(get(f"{rest_proxy_url_str}/v3/clusters/{self.cluster_id_str}/topics/{topic_str}/configs", None, auth_str_tuple=auth_str_tuple, retries=self.kash_config_dict["requests.num.retries"])) for topic_str in topic_str_list}
        #
        return topic_str_config_dict_dict
    
    def set_config(self, pattern_str_or_str_list, config_dict, test=False):
        rest_proxy_url_str = self.rest_proxy_config_dict["rest.proxy.url"]
        auth_str_tuple = self.get_auth_str_tuple()
        #
        topic_str_list = self.topics(pattern_str_or_str_list)
        #
        topic_str_config_dict_dict = {}
        for topic_str in topic_str_list:
            url_str = f"{rest_proxy_url_str}/v3/clusters/{self.cluster_id_str}/topics/{topic_str}/configs:alter"
            headers_dict = {"Content-Type": "application/json"}
            key_str_value_str_dict_list = [{"name": config_key_str, "value": config_value_str} for config_key_str, config_value_str in config_dict.items()]
            payload_dict = {"data": key_str_value_str_dict_list}
            post(url_str, headers_dict, payload_dict, auth_str_tuple=auth_str_tuple, retries=self.kash_config_dict["requests.num.retries"])
            #
            topic_str_config_dict_dict[topic_str] = config_dict
        #
        return topic_str_config_dict_dict

    #

    def create(self, topic_str, partitions=1, config={}, block=True):
        rest_proxy_url_str = self.rest_proxy_config_dict["rest.proxy.url"]
        auth_str_tuple = self.get_auth_str_tuple()
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
        post(url_str, headers_dict, payload_dict, auth_str_tuple=auth_str_tuple, retries=self.kash_config_dict["requests.num.retries"])
        #
        return topic_str

    def delete(self, pattern_str_or_str_list, block=True):
        rest_proxy_url_str = self.rest_proxy_config_dict["rest.proxy.url"]
        auth_str_tuple = self.get_auth_str_tuple()
        #
        topic_str_list = self.list_topics(pattern_str_or_str_list)
        #
        if topic_str_list:
            for topic_str in topic_str_list:
                url_str = f"{rest_proxy_url_str}/v3/clusters/{self.cluster_id_str}/topics/{topic_str}"
                headers_dict = {"Content-Type": "application/json"}
                delete(url_str, headers_dict, auth_str_tuple=auth_str_tuple, retries=self.kash_config_dict["requests.num.retries"])
        #
        return topic_str_list

    #

    def list_topics(self, pattern=None):
        pattern_str_or_str_list = pattern
        #
        rest_proxy_url_str = self.rest_proxy_config_dict["rest.proxy.url"]
        auth_str_tuple = self.get_auth_str_tuple()
        #
        url_str = f"{rest_proxy_url_str}/v3/clusters/{self.cluster_id_str}/topics"
        headers_dict = {"Content-Type": "application/json"}
        response_dict = get(url_str, headers_dict, auth_str_tuple=auth_str_tuple, retries=self.kash_config_dict["requests.num.retries"])
        kafkaTopic_dict_list = response_dict["data"]
        topic_str_list = [kafkaTopic_dict["topic_name"] for kafkaTopic_dict in kafkaTopic_dict_list]
        #
        if pattern_str_or_str_list is not None:
            if isinstance(pattern_str_or_str_list, str):
                pattern_str_or_str_list = [pattern_str_or_str_list]
            topic_str_list = [topic_str for topic_str in topic_str_list if any(fnmatch(topic_str, pattern_str) for pattern_str in pattern_str_or_str_list)]
        #
        topic_str_list.sort()
        #
        return topic_str_list

    def partitions(self, pattern=None, verbose=False):
        pattern_str_or_str_list = pattern
        if pattern_str_or_str_list is None:
            pattern_str_or_str_list = ["*"]
        elif isinstance(pattern_str_or_str_list, str):
            pattern_str_or_str_list = [pattern_str_or_str_list]
        #
        verbose_bool = verbose
        #
        rest_proxy_url_str = self.rest_proxy_config_dict["rest.proxy.url"]
        auth_str_tuple = self.get_auth_str_tuple()
        #
        url_str = f"{rest_proxy_url_str}/v3/clusters/{self.cluster_id_str}/topics"
        headers_dict = {"Content-Type": "application/json"}
        response_dict = get(url_str, headers_dict, auth_str_tuple=auth_str_tuple, retries=self.kash_config_dict["requests.num.retries"])
        kafkaTopic_dict_list = response_dict["data"]
        #
        topic_str_num_partitions_int_dict = {kafkaTopic_dict["topic_name"]: kafkaTopic_dict["partitions_count"] for kafkaTopic_dict in kafkaTopic_dict_list if any(fnmatch(kafkaTopic_dict["topic_name"], pattern_str) for pattern_str in pattern_str_or_str_list)}
        #
        if verbose_bool:
            topic_str_partition_int_partition_dict_dict_dict = {}
            for topic_str in topic_str_num_partitions_int_dict.keys():
                partition_int_partition_dict_dict = {}
                for partition_int in range(topic_str_num_partitions_int_dict[topic_str]):
                    url_str = f"{rest_proxy_url_str}/v3/clusters/{self.cluster_id_str}/topics/{topic_str}/partitions/{partition_int}/replicas"
                    headers_dict = {"Content-Type": "application/json"}
                    response_dict = get(url_str, headers_dict, auth_str_tuple=auth_str_tuple, retries=self.kash_config_dict["requests.num.retries"])
                    kafkaReplica_dict_list = response_dict["data"]
                    #
                    if partition_int in partition_int_partition_dict_dict:
                        partition_dict = partition_int_partition_dict_dict[partition_int]
                    else:
                        partition_dict = {}
                    #
                    for kafkaReplica_dict in kafkaReplica_dict_list:
                        broker_id_int = kafkaReplica_dict["broker_id"]
                        partition_int = kafkaReplica_dict["partition_id"]
                        is_leader_bool = kafkaReplica_dict["is_leader"]
                        is_in_sync_bool =  kafkaReplica_dict["is_in_sync"]
                        #
                        if is_leader_bool:
                            partition_dict["leader"] = broker_id_int
                        #
                        if "replicas" in partition_dict:
                            replica_int_list = partition_dict["replicas"]
                        else:
                            replica_int_list = []
                        replica_int_list.append(broker_id_int)
                        partition_dict["replicas"] = replica_int_list
                        #
                        if "isrs" in partition_dict:
                            isr_int_list = partition_dict["isrs"]
                        else:
                            isr_int_list = []
                        if is_in_sync_bool:
                            isr_int_list.append(broker_id_int)
                        partition_dict["isrs"] = isr_int_list
                    #
                    partition_int_partition_dict_dict[partition_int] = partition_dict
                #
                topic_str_partition_int_partition_dict_dict_dict[topic_str] = partition_int_partition_dict_dict
            #
            return topic_str_partition_int_partition_dict_dict_dict
        else:
            return topic_str_num_partitions_int_dict

    def watermarks(self, pattern, timeout=-1.0):
        rest_proxy_url_str = self.rest_proxy_config_dict["rest.proxy.url"]
        auth_str_tuple = self.get_auth_str_tuple()
        #
        topic_str_num_partitions_int_dict = self.partitions(pattern)
        #
        topic_str_partition_int_offsets_tuple_dict_dict = {}
        for topic_str, num_partitions_int in topic_str_num_partitions_int_dict.items():
            topic_str_partition_int_offsets_tuple_dict_dict[topic_str] = {}
            for partition_int in range(num_partitions_int):
                url_str = f"{rest_proxy_url_str}/topics/{topic_str}/partitions/{partition_int}/offsets"
                headers_dict = {"Content-Type": "application/vnd.kafka.v2+json"}
                response_dict = get(url_str, headers_dict, auth_str_tuple=auth_str_tuple, retries=self.kash_config_dict["requests.num.retries"])
                topic_str_partition_int_offsets_tuple_dict_dict[topic_str][partition_int] = (response_dict["beginning_offset"], response_dict["end_offset"])
        #
        return topic_str_partition_int_offsets_tuple_dict_dict

    #

    def get_auth_str_tuple(self):
        if "basic.auth.user.info" in self.rest_proxy_config_dict:
            return tuple(self.rest_proxy_config_dict["basic.auth.user.info"].split(":"))
        else:
            return None

    #

    def get_kafkaConsumerGroup_dict_list(self, pattern_str_list):
        rest_proxy_url_str = self.rest_proxy_config_dict["rest.proxy.url"]
        auth_str_tuple = self.get_auth_str_tuple()
        #
        headers_dict = {"Content-Type": "application/json"}
        #
        if len(pattern_str_list) == 1 and not(is_pattern(pattern_str_list[0])):
            url_str = f"{rest_proxy_url_str}/v3/clusters/{self.cluster_id_str}/consumer-groups/{pattern_str_list[0]}"
            response_dict = get(url_str, headers_dict, auth_str_tuple=auth_str_tuple, retries=self.kash_config_dict["requests.num.retries"])
            kafkaConsumerGroup_dict_list = [response_dict]
        else:
            url_str = f"{rest_proxy_url_str}/v3/clusters/{self.cluster_id_str}/consumer-groups"
            response_dict = get(url_str, headers_dict, auth_str_tuple=auth_str_tuple, retries=self.kash_config_dict["requests.num.retries"])
            kafkaConsumerGroup_dict_list = response_dict["data"]
        #
        return kafkaConsumerGroup_dict_list

#

def kafkaAcl_dict_to_dict(kafkaAcl_dict):
    dict = {"restype": kafkaAcl_dict["resource_type"],
            "name": kafkaAcl_dict["resource_name"],
            "resource_pattern_type": kafkaAcl_dict["pattern_type"],
            "principal": kafkaAcl_dict["principal"],
            "host": kafkaAcl_dict["host"],
            "operation": kafkaAcl_dict["operation"],
            "permission_type": kafkaAcl_dict["permission"]}
    return dict
