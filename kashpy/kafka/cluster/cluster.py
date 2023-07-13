from kashpy.kafka.cluster.cluster_admin import ClusterAdmin
from kashpy.kafka.cluster.cluster_consumer import ClusterConsumer
from kashpy.kafka.cluster.cluster_producer import ClusterProducer
from kashpy.kafka import Kafka

# Cluster class

class Cluster(Kafka):
    def __init__(self, config_str):
        super().__init__("clusters", config_str, ["kafka"], ["schema_registry"])
        #
        self.admin = self.get_admin()

    #

    def get_admin(self):
        admin = ClusterAdmin(self.kafka_config_dict, self.kash_config_dict)
        #
        return admin

    #
    def get_consumer(self, topics, **kwargs):
        consumer = ClusterConsumer(self.kafka_config_dict, self.schema_registry_config_dict, self.kash_config_dict, topics, **kwargs)
        #
        return consumer

    #

    def get_producer(self, topic, **kwargs):
        producer = ClusterProducer(self.kafka_config_dict, self.schema_registry_config_dict, self.kash_config_dict, self.config_str, topic, **kwargs)
        #
        return producer
