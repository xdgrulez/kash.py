from confluent_kafka import Consumer, KafkaError, Producer, TIMESTAMP_CREATE_TIME, TopicPartition
from confluent_kafka.admin import AdminClient, ConfigResource, NewPartitions, NewTopic, ResourceType
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
    adminClient = AdminClient(config_dict)
    return adminClient


def get_producer(config_dict):
    producer = Producer(config_dict)
    return producer


def get_consumer(config_dict):
    consumer = Consumer(config_dict)
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


def message_to_message_dict(message, key_type="bytes", value_type="bytes"):
    key_type_str = key_type
    value_type_str = value_type
    #
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
    message_dict = {"headers": message.headers(), "partition": message.partition(), "offset": message.offset(), "timestamp": message.timestamp(), "key": decode_key(message.key()), "value": decode_value(message.value())}
    return message_dict


def groupMetadata_to_group_dict(groupMetadata):
    group_dict = {"id": groupMetadata.id, "error": groupMetadata.error, "state": groupMetadata.state, "protocol_type": groupMetadata.protocol_type, "protocol": groupMetadata.protocol, "members": groupMetadata.members}
    return group_dict


def partitionMetadata_to_partition_dict(partitionMetadata):
    partition_dict = {"id": partitionMetadata.id, "leader": partitionMetadata.leader, "replicas": partitionMetadata.replicas, "isrs": partitionMetadata.isrs, "error": kafkaError_to_error_dict(partitionMetadata.error)}
    return partition_dict


def topicMetadata_to_topic_dict(topicMetadata):
    partitions_dict = {partition_int: partitionMetadata_to_partition_dict(partitionMetadata) for partition_int, partitionMetadata in topicMetadata.partitions.items()}
    topic_dict = {"topic": topicMetadata.topic, "partitions": partitions_dict, "error": kafkaError_to_error_dict(topicMetadata.error)}
    return topic_dict


def kafkaError_to_error_dict(kafkaError):
    error_dict = None
    if kafkaError:
        error_dict = {"code": kafkaError.code(), "fatal": kafkaError.fatal(), "name": kafkaError.name(), "retriable": kafkaError.retriable(), "str": kafkaError.str(), "txn_requires_abort": kafkaError.txn_requires_abort()}
    return error_dict


def replicate(source_cluster, source_topic_str, target_cluster, target_topic_str, group=None, map=None, keep_timestamps=True):
    group_str = group
    map_function = map
    keep_timestamps_bool = keep_timestamps
    #
    source_cluster.subscribe(source_topic_str, group=group_str)
    while True:
        message_dict_list = source_cluster.consume(num_messages=500, timeout=1, key_type="bytes", value_type="bytes")
        if not message_dict_list:
            break
        for message_dict in message_dict_list:
            if map_function:
                message_dict = map_function(message_dict)
            #
            if keep_timestamps_bool:
                timestamp_int_int_tuple = message_dict["timestamp"]
                if timestamp_int_int_tuple[0] == TIMESTAMP_CREATE_TIME:
                    timestamp_int = timestamp_int_int_tuple[1]
                    #
                    target_cluster.producer.produce(target_topic_str, key=message_dict["key"], value=message_dict["value"], partition=message_dict["partition"], timestamp=timestamp_int, headers=message_dict["headers"])
                    #
                    continue
            target_cluster.producer.produce(target_topic_str, key=message_dict["key"], value=message_dict["value"], partition=message_dict["partition"], headers=message_dict["headers"])
        target_cluster.flush()
    source_cluster.unsubscribe()


class Cluster:
    def __init__(self, cluster_str):
        self.cluster_str = cluster_str
        self.config_dict = get_config_dict(cluster_str)
        #
        self.adminClient = get_adminClient(self.config_dict)
        #
        self.producer = get_producer(self.config_dict)

# Admin

    def topics(self):
        topic_str_list = list(self.adminClient.list_topics().topics.keys())
        topic_str_list.sort()
        return topic_str_list

    def brokers(self):
        broker_dict = {broker_int: brokerMetadata.host + ":" + str(brokerMetadata.port) for broker_int, brokerMetadata in self.adminClient.list_topics().brokers.items()}
        return broker_dict

    def delete(self, topic_str):
        self.adminClient.delete_topics([topic_str])

    def create(self, topic_str, partitions=1):
        partitions_int = partitions
        #
        newTopic = NewTopic(topic_str, partitions_int)
        self.adminClient.create_topics([newTopic])

    def describe(self, topic_str):
        topicMetadata = self.adminClient.list_topics(topic=topic_str).topics[topic_str]
        topic_dict = {}
        if topicMetadata:
            topic_dict = topicMetadata_to_topic_dict(topicMetadata)
        return topic_dict

    def watermarks(self, topic_str):
        config_dict = self.config_dict
        config_dict["group.id"] = "dummy_group_id"
        consumer = get_consumer(config_dict)
        #
        partitions_int = self.partitions(topic_str)
        offset_int_tuple_dict = {partition_int: consumer.get_watermark_offsets(TopicPartition(topic_str, partition=partition_int)) for partition_int in range(partitions_int)}
        return offset_int_tuple_dict

    def size(self, topic_str, verbose=False):
        verbose_bool = verbose
        #
        watermarks_dict = self.watermarks(topic_str)
        if verbose_bool:
            size_dict = {partition_int: watermarks_dict[partition_int][1]-watermarks_dict[partition_int][0] for partition_int in watermarks_dict.keys()}
            return size_dict
        else:
            total_size_int = 0
            for offset_int_tuple in watermarks_dict.values():
                partition_size_int = offset_int_tuple[1] - offset_int_tuple[0]
                total_size_int += partition_size_int
            return total_size_int

    def exists(self, topic_str):
        topic_dict = self.describe(topic_str)
        if topic_dict["error"] != None:
            if topic_dict["error"]["code"] == KafkaError.UNKNOWN_TOPIC_OR_PART:
                return False
        return True

    def partitions(self, topic_str):
        partitions_int = len(self.adminClient.list_topics(topic=topic_str).topics[topic_str].partitions)
        return partitions_int

    def set_partitions(self, topic_str, new_partitions_int, test=False):
        test_bool = test
        #
        newPartitions = NewPartitions(topic_str, new_partitions_int)
        self.adminClient.create_partitions([newPartitions], validate_only=test_bool)

    def groups(self):
        groupMetadata_list = self.adminClient.list_groups()
        group_str_list = [groupMetadata.id for groupMetadata in groupMetadata_list]
        group_str_list.sort()
        return group_str_list

    def describe_group(self, group_str):
        groupMetadata_list = self.adminClient.list_groups(group=group_str)
        group_dict = {}
        if groupMetadata_list:            
            groupMetadata = groupMetadata_list[0]
            group_dict = groupMetadata_to_group_dict(groupMetadata)
        return group_dict

    def get_config_dict(self, resourceType, resource_str):
        configResource = ConfigResource(resourceType, resource_str)
        # configEntry_dict: ConfigResource -> ConfigEntry
        configEntry_dict = self.adminClient.describe_configs([configResource])[configResource].result()
        # config_dict: str -> str
        config_dict = {config_key_str: configEntry.value for config_key_str, configEntry in configEntry_dict.items()}
        return config_dict

    def config(self, topic_str):
        return self.get_config_dict(ResourceType.TOPIC, topic_str)

    def broker_config(self, broker_int):
        return self.get_config_dict(ResourceType.BROKER, str(broker_int))

    def set_config_dict(self, resourceType, resource_str, new_config_dict, test=False):
        test_bool = test
        #
        old_config_dict = self.get_config_dict(resourceType, resource_str)
        #
        if resourceType == ResourceType.BROKER:
            # https://docs.confluent.io/platform/current/installation/configuration/broker-configs.html#cp-config-brokers
            white_list_key_str_list = ["advertised.listeners", "background.threads", "compression.type", "confluent.balancer.enable", "confluent.balancer.heal.uneven.load.trigger", "confluent.balancer.throttle.bytes.per.second", "confluent.tier.local.hotset.bytes", "confluent.tier.local.hotset.ms", "listeners", "log.flush.interval.messages", "log.flush.interval.ms", "log.retention.bytes", "log.retention.ms", "log.roll.jitter.ms", "log.roll.ms", "log.segment.bytes", "log.segment.delete.delay.ms", "message.max.bytes", "min.insync.replicas", "num.io.threads", "num.network.threads", "num.recovery.threads.per.data.dir", "num.replica.fetchers", "unclean.leader.election.enable", "confluent.balancer.exclude.topic.names", "confluent.balancer.exclude.topic.prefixes", "confluent.clm.enabled", "confluent.clm.frequency.in.hours", "confluent.clm.max.backup.days", "confluent.clm.min.delay.in.minutes", "confluent.clm.topic.retention.days.to.backup.days", "confluent.cluster.link.fetch.response.min.bytes", "confluent.cluster.link.fetch.response.total.bytes", "confluent.cluster.link.io.max.bytes.per.second", "confluent.tier.enable", "confluent.tier.max.partition.fetch.bytes.override", "log.cleaner.backoff.ms", "log.cleaner.dedupe.buffer.size", "log.cleaner.delete.retention.ms", "log.cleaner.io.buffer.load.factor", "log.cleaner.io.buffer.size", "log.cleaner.io.max.bytes.per.second", "log.cleaner.max.compaction.lag.ms", "log.cleaner.min.cleanable.ratio", "log.cleaner.min.compaction.lag.ms", "log.cleaner.threads", "log.cleanup.policy", "log.deletion.max.segments.per.run", "log.index.interval.bytes", "log.index.size.max.bytes", "log.message.timestamp.difference.max.ms", "log.message.timestamp.type", "log.preallocate", "max.connection.creation.rate", "max.connections", "max.connections.per.ip", "max.connections.per.ip.overrides", "principal.builder.class", "sasl.enabled.mechanisms", "sasl.jaas.config", "sasl.kerberos.kinit.cmd", "sasl.kerberos.min.time.before.relogin", "sasl.kerberos.principal.to.local.rules", "sasl.kerberos.service.name", "sasl.kerberos.ticket.renew.jitter", "sasl.kerberos.ticket.renew.window.factor", "sasl.login.refresh.buffer.seconds", "sasl.login.refresh.min.period.seconds", "sasl.login.refresh.window.factor", "sasl.login.refresh.window.jitter", "sasl.mechanism.inter.broker.protocol", "ssl.cipher.suites", "ssl.client.auth", "ssl.enabled.protocols", "ssl.keymanager.algorithm", "ssl.protocol", "ssl.provider", "ssl.trustmanager.algorithm", "confluent.cluster.link.replication.quota.mode", "confluent.metadata.server.cluster.registry.clusters", "confluent.reporters.telemetry.auto.enable", "confluent.security.event.router.config", "confluent.telemetry.enabled", "confluent.tier.topic.delete.backoff.ms", "confluent.tier.topic.delete.check.interval.ms", "confluent.tier.topic.delete.max.inprogress.partitions", "follower.replication.throttled.rate", "follower.replication.throttled.replicas", "leader.replication.throttled.rate", "leader.replication.throttled.replicas", "listener.security.protocol.map", "log.message.downconversion.enable", "metric.reporters", "ssl.endpoint.identification.algorithm", "ssl.engine.factory.class", "ssl.secure.random.implementation"]
        #
        alter_config_dict = {}
        for key_str, value_str in old_config_dict.items():
            if resourceType == ResourceType.BROKER:
                if key_str not in white_list_key_str_list:
                    continue
            if key_str in new_config_dict:
                value_str = new_config_dict[key_str]
            if value_str:
                alter_config_dict[key_str] = value_str
        #
        alter_configResource = ConfigResource(resourceType, resource_str, set_config=alter_config_dict)
        #
        future = self.adminClient.alter_configs([alter_configResource], validate_only=test_bool)[alter_configResource]
        #
        future.result()

    def set_config(self, topic_str, key_str, value_str, test=False):
        self.set_config_dict(ResourceType.TOPIC, topic_str, {key_str: value_str}, test)

    def set_broker_config(self, broker_int, key_str, value_str, test=False):
        self.set_config_dict(ResourceType.BROKER, str(broker_int), {key_str: value_str}, test)
        
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
    
    def subscribe(self, topic_str, group=None, offset_reset="earliest", offsets=None):
        offset_reset_str = offset_reset
        offsets_dict = offsets
        #
        self.topic_str = topic_str
        #
        if group == None:
            group_str = create_unique_group_id()
        else:
            group_str = group
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
            message_dict_list = [message_to_message_dict(message, key_type=key_type_str, value_type=value_type_str) for message in message_list]
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
        self.subscribe(topic_str, group=group_str)
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
        mode_str = "w" if overwrite_bool else "a"
        #
        self.subscribe(topic_str, group=group_str)
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
