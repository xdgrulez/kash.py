class KafkaWriter:
    def __init__(self, kafka_obj, topic, **kwargs):
        self.kafka_obj = kafka_obj
        #
        self.topic_str = topic
        #
        (self.key_type_str, self.value_type_str) = kafka_obj.get_key_value_type_tuple(**kwargs)
        #
        (self.key_schema_str, self.value_schema_str, self.key_schema_id_int, self.value_schema_id_int) = self.get_key_value_schema_tuple(**kwargs)
        #
        self.produced_counter_int = 0

    #

    def write(self, value, **kwargs):
        return self.produce(value, **kwargs)

    # Helpers

    def get_key_value_schema_tuple(self, **kwargs):
        key_schema_str = kwargs["key_schema"] if "key_schema" in kwargs else None
        value_schema_str = kwargs["value_schema"] if "value_schema" in kwargs else None
        #
        key_schema_id_int = kwargs["key_schema_id"] if "key_schema_id" in kwargs else None
        value_schema_id_int = kwargs["value_schema_id"] if "value_schema_id" in kwargs else None
        #
        return (key_schema_str, value_schema_str, key_schema_id_int, value_schema_id_int)
