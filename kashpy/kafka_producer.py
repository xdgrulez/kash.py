# Constants

CURRENT_TIME = 0
RD_KAFKA_PARTITION_UA = -1

#

class KafkaProducer():
    def __init__(self):
        pass

    def write(self, value, key=None, partition=RD_KAFKA_PARTITION_UA, timestamp=CURRENT_TIME, headers=None):
        return self.produce(value, key, partition, timestamp, headers)
