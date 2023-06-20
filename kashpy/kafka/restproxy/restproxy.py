from kashpy.restproxy.restproxy_admin import RestProxyAdmin
from kashpy.restproxy.restproxy_consumer import RestProxyConsumer
from kashpy.restproxy.restproxy_producer import RestProxyProducer
from kashpy.kafka import Kafka
from kashpy.helpers import get

# RestProxy class

class RestProxy(Kafka):
    def __init__(self, config_str):
        self.cluster_id_str = self.get_cluster_id()
        #
        super().__init__("restproxies", config_str, ["rest_proxy"], ["schema_registry", "kash"])

    #

    def get_admin(self):
        admin = RestProxyAdmin(self.rest_proxy_config_dict, self.kash_config_dict, self.cluster_id_str)
        #
        return admin

    #

    def get_consumer(self, topics, **kwargs):
        consumer = RestProxyConsumer(self.rest_proxy_config_dict, self.schema_registry_config_dict, self.kash_config_dict, self.cluster_id_str, topics, **kwargs)
        #
        return consumer

    #

    def get_producer(self, topics, **kwargs):
        producer = RestProxyProducer(self.rest_proxy_config_dict, self.schema_registry_config_dict, self.kash_config_dict, self.cluster_id_str, topics, **kwargs)
        #
        return producer
    
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