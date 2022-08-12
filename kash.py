from confluent_kafka import TopicPartition
import confluent_kafka
import confluent_kafka.admin
import configparser
import os
import time

def get_config_dict(cluster_str):
    rawConfigParser = configparser.RawConfigParser()
    if os.path.exists(f"./clusters_secured/{cluster_str}.conf"):
        rawConfigParser.read(f"./clusters_secured/{cluster_str}.conf")
    elif os.path.exists(f"./clusters_unsecured/{cluster_str}.conf"):
        rawConfigParser.read(f"./clusters_unsecured/{cluster_str}.conf")
    else:
        raise Exception(f"No cluster configuration file \"{cluster_str}.conf\" found in \"clusters_secured\" and \"clusters_unsecured\".")
    config_dict = dict(rawConfigParser.items("kafka"))
    return config_dict


def get_adminClient(config_dict):
    adminClient = confluent_kafka.admin.AdminClient(config_dict)
    return adminClient


def get_producer(config_dict):
    producer = confluent_kafka.Producer(config_dict)
    return producer


def get_consumer(config_dict):
    consumer = confluent_kafka.Consumer(config_dict)
    return consumer


def get_millis():
    return int(time.time()*1000)


def create_unique_group_id():
    return str(get_millis())


def foreach_line(path_str, proc_function, delimiter='\n', bufsize=4096):
    delimiter_str = delimiter
    bufsize_int = bufsize
    #
    buf_str = ""
    #
    with open(path_str) as textIOWrapper:
        while True:
            newbuf_str = textIOWrapper.read(bufsize_int)
            if not newbuf_str:
                if buf_str:
                    proc_function(buf_str)
                break
            buf_str += newbuf_str
            line_str_list = buf_str.split(delimiter_str)
            for line_str in line_str_list[:-1]:
                proc_function(line_str)
            buf_str = line_str_list[-1]


def replicate(source_kash, source_topic_str, target_kash, target_topic_str, group=None, map=None):
    group_str = group
    map_function = map
    #
    if group_str == None:
        group_str = create_unique_group_id()
    #
    source_kash.subscribe(source_topic_str, group_str)
    while True:
        message_dict_list = source_kash.consume(num_messages=500, timeout=1, key_type="bytes", value_type="bytes")
        if not message_dict_list:
            break
        for message_dict in message_dict_list:
            if map_function:
                message_dict = map_function(message_dict)
            target_kash.producer.produce(target_topic_str, key=message_dict["key"], value=message_dict["value"], partition=message_dict["partition"], timestamp=message_dict["timestamp"], headers=message_dict["headers"])
    source_kash.unsubscribe()

class Kash:
    def __init__(self, cluster_str):
        self.cluster_str = cluster_str
        self.config_dict = get_config_dict(cluster_str)
        #
        self.adminClient = get_adminClient(self.config_dict)
        #
        self.producer = get_producer(self.config_dict)

# Admin

    def list(self):
        topic_str_list = list(self.adminClient.list_topics().topics.keys())
        topic_str_list.sort()
        return topic_str_list

    def delete(self, topic_str):
        self.adminClient.delete_topics([topic_str])

    def create(self, topic_str, num_partitions=1):
        num_partitions_int = num_partitions
        #
        newTopic = confluent_kafka.admin.NewTopic(topic_str, num_partitions_int)
        self.adminClient.create_topics([newTopic])

    def describe(self, topic_str):
        topicMetadata = self.adminClient.list_topics(topic=topic_str).topics[topic_str]
        topic_dict = {}
        if topicMetadata:
            partitions_dict = {partition_int: {"id": partitionMetadata.id, "leader": partitionMetadata.leader, "replicas": partitionMetadata.replicas, "isrs": partitionMetadata.isrs, "error": partitionMetadata.error} for partition_int, partitionMetadata in topicMetadata.partitions.items()}
            topic_dict = {"topic": topicMetadata.topic, "partitions": partitions_dict, "error": topicMetadata.error}
        return topic_dict

    def watermarks(self, topic_str):
        config_dict = self.config_dict
        config_dict["group.id"] = "dummy_group_id"
        consumer = get_consumer(config_dict)
        #
        num_partitions_int = self.num_partitions(topic_str)
        offset_int_tuple_list = [consumer.get_watermark_offsets(TopicPartition(topic_str, partition=partition_int)) for partition_int in range(num_partitions_int)]
        return offset_int_tuple_list

    def size(self, topic_str, partition=None):
        offset_int_tuple_list = self.watermarks(topic_str)
        if partition != None:
            offset_int_tuple_list = [offset_int_tuple_list[partition]]
        total_size_int = 0
        for offset_int_tuple in offset_int_tuple_list:
            partition_size_int = offset_int_tuple[1] - offset_int_tuple[0]
            total_size_int += partition_size_int
        return total_size_int

    def num_partitions(self, topic_str):
        num_partitions_int = len(self.adminClient.list_topics(topic=topic_str).topics[topic_str].partitions)
        return num_partitions_int

    def list_groups(self):
        groupMetadata_list = self.adminClient.list_groups()
        group_str_list = [groupMetadata.id for groupMetadata in groupMetadata_list]
        group_str_list.sort()
        return group_str_list

    def describe_group(self, group_str):
        groupMetadata_list = self.adminClient.list_groups(group=group_str)
        group_dict = {}
        if groupMetadata_list:            
            groupMetadata = groupMetadata_list[0]
            group_dict = {"id": groupMetadata.id, "error": groupMetadata.error, "state": groupMetadata.state, "protocol_type": groupMetadata.protocol_type, "protocol": groupMetadata.protocol, "members": groupMetadata.members}
        return group_dict

# Producer

    def produce(self, topic_str, key_str, value_str):
        self.producer.produce(topic_str, key=key_str, value=value_str)
    
    def upload(self, path_str, topic_str, key_value_separator=None, message_separator="\n"):  
        key_value_separator_str = key_value_separator
        message_separator_str = message_separator
        #
        def proc(line_str):
            line_str1 = line_str.strip()
            if key_value_separator_str != None:
                split_str_list = line_str1.split(key_value_separator_str)
                if len(split_str_list) == 2:
                    key_str = split_str_list[0]
                    value_str = split_str_list[1]
                else:
                    key_str = None
                    value_str = line_str1
            else:
                key_str = None
                value_str = line_str1
            #
            self.produce(topic_str, key_str, value_str)
        #
        foreach_line(path_str, proc, delimiter=message_separator_str)
        self.flush()

    def flush(self, timeout=1):
        timeout_float = timeout
        #
        self.producer.flush(timeout_float)

# Consumer
    
    def subscribe(self, topic_str, group_str, offset_reset="earliest", offsets=None):
        offset_reset_str = offset_reset
        offsets_dict = offsets
        #
        self.topic_str = topic_str
        #
        self.config_dict["group.id"] = group_str
        self.config_dict["auto.offset.reset"] = offset_reset_str
        self.consumer = get_consumer(self.config_dict)
        #
        clusterMetaData = self.consumer.list_topics(topic=topic_str)
        self.topicPartition_list = [TopicPartition(topic_str, partition_int) for partition_int in clusterMetaData.topics[topic_str].partitions.keys()]
        #
        def on_assign(consumer, partitions):
            topicPartition_list = partitions
            #
            if offsets_dict != None:
                for index_int, offset_int in offsets_dict.items():
                    topicPartition_list[index_int].offset = offset_int
                consumer.assign(topicPartition_list)
        self.consumer.subscribe([topic_str], on_assign=on_assign)

    def unsubscribe(self):
        self.consumer.unsubscribe()

    def consume(self, num_messages=1, timeout=1.0, key_type="str", value_type="str"):
        num_messages_int = num_messages
        timeout_float = timeout
        key_type_str = key_type
        value_type_str = value_type
        #
        if num_messages_int == 1:
            message_list = [self.consumer.poll(timeout_float)]
        else:
            message_list = self.consumer.consume(num_messages=num_messages_int, timeout=timeout_float)
        #
        if message_list and message_list[0]:
            def bytes_to_str(bytes):
                if bytes:
                    return bytes.decode("utf-8")
                else:
                    return bytes
            #
            def bytes_to_bytes(bytes):
                return bytes
            #
            if key_type_str == "str":
                decode_key = bytes_to_str
            elif key_type_str == "bytes":
                decode_key = bytes_to_bytes
            #
            if value_type_str == "str":
                decode_value = bytes_to_str
            elif value_type_str == "bytes":
                decode_value = bytes_to_bytes
            #
            message_dict_list = [{"headers": message.headers(), "partition": message.partition(), "offset": message.offset(), "timestamp": message.timestamp(), "key": decode_key(message.key()), "value": decode_value(message.value())} for message in message_list]
        else:
            message_dict_list = []
        #
        return message_dict_list

    def offsets(self):
        topicPartition_list = self.consumer.committed(self.topicPartition_list, timeout=1.0)
        offsets_dict = {topicPartition.partition: topicPartition.offset for topicPartition in topicPartition_list}
        return offsets_dict

    def cat(self, topic_str, group=None, foreach=print, key_type="bytes", value_type="bytes"):
        group_str = group
        foreach_function = foreach
        key_type_str = key_type
        value_type_str = value_type
        #
        if group_str == None:
            group_str = create_unique_group_id()
        #
        self.subscribe(topic_str, group_str)
        while True:
            message_dict_list = self.consume(num_messages=500, timeout=1, key_type=key_type_str, value_type=value_type_str)
            if not message_dict_list:
                break
            for message_dict in message_dict_list:
                foreach_function(message_dict)
        self.unsubscribe()

    def download(self, topic_str, path_str, group=None, key_value_separator=None, message_separator="\n", overwrite=True):
        group_str = group
        key_value_separator_str = key_value_separator
        message_separator_str = message_separator
        overwrite_bool = overwrite
        #
        if group_str == None:
            group_str = create_unique_group_id()
        #
        mode_str = "w" if overwrite_bool else "a"
        #
        self.subscribe(topic_str, group_str)
        with open(path_str, mode_str) as textIOWrapper:
            while True:
                message_dict_list = self.consume(num_messages=500, timeout=1)
                output_str_list = []
                if not message_dict_list:
                    break
                for message_dict in message_dict_list:
                    value_str = message_dict["value"]
                    if key_value_separator_str == None:
                        output_str = value_str
                    else:
                        key_str = message_dict["key"]
                        output_str = f"{key_str}{key_value_separator_str}{value_str}"
                    output_str_list += [output_str + message_separator_str]
                textIOWrapper.writelines(output_str_list)
        self.unsubscribe()

