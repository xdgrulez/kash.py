from kashpy.kafka.cluster.cluster_admin import ClusterAdmin
from kashpy.kafka.cluster.cluster_reader import ClusterReader
from kashpy.kafka.cluster.cluster_writer import ClusterWriter
from kashpy.kafka.kafka import Kafka

# Cluster class

class Cluster(Kafka):
    def __init__(self, config_str):
        super().__init__("clusters", config_str, ["kafka"], ["schema_registry"])
        #
        self.admin = self.get_admin()

    #

    def get_admin(self):
        admin = ClusterAdmin(self)
        #
        return admin

    #
    def get_consumer(self, topics, **kwargs):
        consumer = ClusterReader(self, topics, **kwargs)
        #
        return consumer

    #

    def get_producer(self, topic, **kwargs):
        producer = ClusterWriter(self, topic, **kwargs)
        #
        return producer
