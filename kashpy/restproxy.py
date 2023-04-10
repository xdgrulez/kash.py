import json
import requests

from kashpy.kafka import Kafka

class RestProxy(Kafka):
    def __init__(self):
        self.topic_str = None
        #
        self.cluster_id_str = self.get_cluster_id()

    #

    def get_cluster_id(self):
        rest_proxy_url_str = "http://localhost:8082"
        #
        url_str = f"{rest_proxy_url_str}/v3/clusters"
        #
        response = requests.get(url_str)
        response_dict = response.json()
        #
        cluster_id_str = response_dict["data"][0]["cluster_id"]
        return cluster_id_str
        
    # Consumer

    # def openr(self, topic:str, group:str=None, offsets:Dict[int, int]=None, config:Dict[str, str]={}, key_type:str="str", value_type:str="str"):
    #     return self.subscribe(topic, group, offsets, config, key_type, value_type)

    # def closer(self):
    #     return self.close()

    # def read(self, n:str=1):
    #     return self.consume(n)

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
        response = requests.post(url_str, headers=headers_dict, json=rest_message_dict)
        response_dict = response.json()
        #
        if response_dict["error_code"] == 200:
            return key, value
        else:
            raise Exception(response_dict)
