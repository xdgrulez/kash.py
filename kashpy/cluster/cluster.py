from kashpy.cluster.admin import Admin
from kashpy.cluster.consumer import Consumer
from kashpy.cluster.producer import Producer
from kashpy.kafka import Kafka

# Cluster class

class Cluster(Kafka):
    def __init__(self, config_str):
        super().__init__("clusters", config_str, ["kafka"], ["schema_registry", "kash"])

    #

    def get_admin(self):
        admin = Admin(self.kafka_config_dict, self.kash_config_dict)
        #
        return admin

    #

    def get_consumer(self, topics, group, offsets, config, key_type, value_type):
        consumer = Consumer(self.kafka_config_dict, self.schema_registry_config_dict, self.kash_config_dict, topics, group, offsets, config, key_type, value_type)
        #
        return consumer

    #

    def get_producer(self, topics, key_type, value_type, key_schema, value_schema, on_delivery):
        producer = Producer(self.kafka_config_dict, self.schema_registry_config_dict, self.kash_config_dict, self.config_str, topics, key_type, value_type, key_schema, value_schema, on_delivery)
        #
        return producer
