import sys
import time
import unittest
import warnings
sys.path.insert(1, "..")
from kashpy.kafka.cluster.cluster import *
from kashpy.helpers import *

cluster_str = "local"
principal_str = None
# cluster_str = "ccloud"
# principal_str = "User:admin"

OFFSET_INVALID = -1001

class Test(unittest.TestCase):
    def setUp(self):
        warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)
        #
        # https://simon-aubury.medium.com/kafka-with-avro-vs-kafka-with-protobuf-vs-kafka-with-json-schema-667494cbb2af
        self.snack_str_list = ['{"name": "cookie", "calories": 500.0, "colour": "brown"}', '{"name": "cake", "calories": 260.0, "colour": "white"}', '{"name": "timtam", "calories": 80.0, "colour": "chocolate"}']
        #
        self.topic_str_list = []
        self.group_str_list = []
        #
        print("Test:", self._testMethodName)

    def tearDown(self):
        cluster = Cluster(cluster_str)
        for group_str in self.group_str_list:
            cluster.delete_groups(group_str)
        for topic_str in self.topic_str_list:
            cluster.delete(topic_str)

    def create_test_topic_name(self):
        topic_str = f"test_topic_{get_millis()}"
        #
        self.topic_str_list.append(topic_str)
        #
        return topic_str

    def create_test_group_name(self):
        group_str = f"test_group_{get_millis()}"
        #
        self.group_str_list.append(group_str)
        #
        return group_str

    ### ClusterAdmin
    # ACLs

    def test_acls(self):
        if principal_str:
            cluster = Cluster(cluster_str)
            topic_str = self.create_test_topic_name()
            cluster.create(topic_str)
            cluster.create_acl(restype="topic", name=topic_str, resource_pattern_type="literal", principal=principal_str, host="*", operation="read", permission_type="allow")
            acl_dict_list = cluster.acls()
            self.assertIn({"restype": "topic", "name": topic_str, "resource_pattern_type": "literal", 'principal': principal_str, 'host': '*', 'operation': 'read', 'permission_type': 'ALLOW'}, acl_dict_list)
            cluster.delete_acl(restype="topic", name=topic_str, resource_pattern_type="literal", principal=principal_str, host="*", operation="read", permission_type="allow")
            self.assertIn({"restype": "topic", "name": topic_str, "resource_pattern_type": "literal", 'principal': principal_str, 'host': '*', 'operation': 'read', 'permission_type': 'ALLOW'}, acl_dict_list)
            cluster.delete(topic_str)

    # Brokers
    
    def test_brokers(self):
        cluster = Cluster(cluster_str)
        if "confluent.cloud" not in cluster.kafka_config_dict["bootstrap.servers"]: # cannot modify cluster properties of non-dedicated Confluent Cloud test cluster
            broker_dict = cluster.brokers()
            broker_int = list(broker_dict.keys())[0]
            broker_dict1 = cluster.brokers(f"{broker_int}")
            self.assertEqual(broker_dict, broker_dict1)
            broker_dict2 = cluster.brokers([broker_int])
            self.assertEqual(broker_dict1, broker_dict2)
            broker_int = list(broker_dict.keys())[0]
            old_background_threads_str = cluster.broker_config(broker_int)[broker_int]["background.threads"]
            cluster.set_broker_config(broker_int, {"background.threads": 5})
            new_background_threads_str = cluster.broker_config(broker_int)[broker_int]["background.threads"]
            self.assertEqual(new_background_threads_str, "5")
            cluster.set_broker_config(broker_int, {"background.threads": old_background_threads_str})

    # Groups

    def test_groups(self):
        cluster = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        cluster.create(topic_str)
        producer = cluster.openw(topic_str)
        producer.produce("message 1")
        producer.produce("message 2")
        producer.produce("message 3")
        producer.close()
        #
        group_str = self.create_test_group_name()
        consumer = cluster.openr(topic_str, group=group_str)
        consumer.consume()
        #
        group_str_list1 = cluster.groups(["test*", "test_group*"])
        self.assertIn(group_str, group_str_list1)
        group_str_list2 = cluster.groups("test_group*")
        self.assertIn(group_str, group_str_list2)
        group_str_list3 = cluster.groups("test_group*", state_pattern=["stable"])
        self.assertIn(group_str, group_str_list3)
        group_str_state_str_dict = cluster.groups("test_group*", state_pattern="stab*", state=True)
        self.assertIn("stable", group_str_state_str_dict[group_str])
        group_str_list4 = cluster.groups(state_pattern="unknown", state=False)
        self.assertEqual(group_str_list4, [])
        #
        consumer.close()

    def test_describe_groups(self):
        cluster = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        cluster.create(topic_str)
        producer = cluster.openw(topic_str)
        producer.produce("message 1")
        producer.produce("message 2")
        producer.produce("message 3")
        producer.close()
        #
        group_str = self.create_test_group_name()
        consumer = cluster.openr(topic_str, group=group_str)
        consumer.consume()
        #
        group_dict = cluster.describe_groups(group_str)[group_str]
        self.assertEqual(group_dict["group_id"], group_str)
        self.assertEqual(group_dict["is_simple_consumer_group"], False)
        self.assertEqual(group_dict["members"][0]["client_id"], "rdkafka")
        self.assertIsNone(group_dict["members"][0]["assignment"]["topic_partitions"][0]["error"])
        self.assertIsNone(group_dict["members"][0]["assignment"]["topic_partitions"][0]["metadata"])
        self.assertEqual(group_dict["members"][0]["assignment"]["topic_partitions"][0]["offset"], -1001)
        self.assertEqual(group_dict["members"][0]["assignment"]["topic_partitions"][0]["partition"], 0)
        self.assertEqual(group_dict["members"][0]["assignment"]["topic_partitions"][0]["topic"], topic_str)
        self.assertIsNone(group_dict["members"][0]["group_instance_id"])
        self.assertEqual(group_dict["partition_assignor"], "range")
        self.assertEqual(group_dict["state"], "stable")
        broker_dict = cluster.brokers()
        broker_int = list(broker_dict.keys())[0]
        self.assertEqual(group_dict["coordinator"]["id"], broker_int)
        self.assertEqual(group_dict["coordinator"]["id_string"], f"{broker_int}")
        #
        consumer.close()

    def test_delete_groups(self):
        cluster = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        cluster.create(topic_str)
        producer = cluster.openw(topic_str)
        producer.produce("message 1")
        producer.produce("message 2")
        producer.produce("message 3")
        producer.close()
        #
        group_str = self.create_test_group_name()
        consumer = cluster.openr(topic_str, group=group_str)
        consumer.consume()
        #
        group_str_list = cluster.delete_groups(group_str, state_pattern=["empt*"])
        self.assertEqual(group_str_list, [])
        group_str_list = cluster.groups(group_str, state_pattern="*")
        self.assertEqual(group_str_list, [group_str])
        #
        consumer.close()

    def test_group_offsets(self):
        cluster = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        cluster.create(topic_str, partitions=2)
        producer = cluster.openw(topic_str)
        producer.produce("message 1", partition=0)
        producer.produce("message 2", partition=1)
        producer.close()
        #
        group_str = self.create_test_group_name()
        consumer = cluster.openr(topic_str, group=group_str, config={"enable.auto.commit": False})
        consumer.consume()
        consumer.commit()
        consumer.consume()
        consumer.commit()
        #
        group_str_topic_str_partition_int_offset_int_dict_dict_dict = cluster.group_offsets(group_str)
        self.assertIn(group_str, group_str_topic_str_partition_int_offset_int_dict_dict_dict)
        self.assertIn(topic_str, group_str_topic_str_partition_int_offset_int_dict_dict_dict[group_str])
        self.assertEqual(group_str_topic_str_partition_int_offset_int_dict_dict_dict[group_str][topic_str][0], 1)
        self.assertEqual(group_str_topic_str_partition_int_offset_int_dict_dict_dict[group_str][topic_str][1], 1)
        #
        consumer.close()

    def test_set_group_offsets(self):
        cluster = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        cluster.create(topic_str)
        producer = cluster.openw(topic_str)
        producer.produce("message 1")
        producer.produce("message 2")
        producer.produce("message 3")
        producer.close()
        #
        group_str = self.create_test_group_name()
        consumer = cluster.openr(topic_str, group=group_str, config={"enable.auto.commit": False})
        group_str_topic_str_partition_int_offset_int_dict_dict_dict = cluster.set_group_offsets({group_str: {topic_str: {0: 2}}})
        self.assertEqual(group_str_topic_str_partition_int_offset_int_dict_dict_dict, {group_str: {topic_str: {0: 2}}})
        [message_dict] = consumer.consume()
        consumer.commit()
        self.assertEqual(message_dict["value"], "message 3")
        #
        consumer.close()

    # Topics

    def test_config_set_config(self):
        cluster = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        cluster.touch(topic_str)
        #
        cluster.set_config(topic_str, {"retention.ms": 4711})
        new_retention_ms_str = cluster.config(topic_str)[topic_str]["retention.ms"]
        self.assertEqual(new_retention_ms_str, "4711")

    def test_create_delete(self):
        cluster = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        cluster.touch(topic_str)
        topic_str_list = cluster.ls()
        self.assertIn(topic_str, topic_str_list)
        cluster.rm(topic_str)
        topic_str_list = cluster.ls()
        self.assertNotIn(topic_str, topic_str_list)

    def test_topics(self):
        cluster = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        old_topic_str_list = cluster.topics(["test_*"])
        self.assertNotIn(topic_str, old_topic_str_list)
        cluster.create(topic_str)
        new_topic_str_list = cluster.ls(["test_*"])
        self.assertIn(topic_str, new_topic_str_list)
        #
        producer = cluster.openw(topic_str)
        producer.produce("message 1", on_delivery=lambda kafkaError, message: print(kafkaError, message))
        producer.produce("message 2")
        producer.produce("message 3")
        producer.close()
        #
        topic_str_size_int_dict_l = cluster.l(pattern=topic_str)
        topic_str_size_int_dict_ll = cluster.ll(pattern=topic_str)
        self.assertEqual(topic_str_size_int_dict_l, topic_str_size_int_dict_ll)
        size_int = topic_str_size_int_dict_l[topic_str]
        self.assertEqual(size_int, 3)
        topic_str_total_size_int_size_dict_tuple_dict = cluster.topics(pattern=topic_str, size=True, partitions=True)
        self.assertEqual(topic_str_total_size_int_size_dict_tuple_dict[topic_str][0], 3)
        self.assertEqual(topic_str_total_size_int_size_dict_tuple_dict[topic_str][1][0], 3)
        topic_str_total_size_int_size_dict_tuple_dict = cluster.topics(pattern=topic_str, size=False, partitions=True)
        self.assertEqual(topic_str_total_size_int_size_dict_tuple_dict[topic_str][0], 3)

    def test_offsets_for_times(self):
        cluster = Cluster(cluster_str)
        #
        cluster.verbose(1)
        #
        topic_str = self.create_test_topic_name()
        cluster.create(topic_str)
        producer = cluster.openw(topic_str)
        producer.produce("message 1")
        time.sleep(1)
        producer.produce("message 2")
        producer.close()
        #
        self.assertEqual(cluster.l(topic_str, partitions=True)[topic_str][1][0], 2)
        #
        group_str = self.create_test_group_name()
        consumer = cluster.openr(topic_str, group=group_str)
        message_dict_list = consumer.consume(n=2)
        consumer.close()
        message1_timestamp_int = message_dict_list[1]["timestamp"][1]
        message1_offset_int = message_dict_list[1]["offset"]
        #
        topic_str_partition_int_offset_int_dict_dict = cluster.offsets_for_times(topic_str, {0: message1_timestamp_int})
        found_message1_offset_int = topic_str_partition_int_offset_int_dict_dict[topic_str][0]
        self.assertEqual(message1_offset_int, found_message1_offset_int)

    def test_partitions_set_partitions(self):
        cluster = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        cluster.create(topic_str)
        num_partitions_int_1 = cluster.partitions(topic_str)[topic_str]
        self.assertEqual(num_partitions_int_1, 1)
        cluster.set_partitions(topic_str, 2)
        num_partitions_int_2 = cluster.partitions(topic_str)[topic_str]
        self.assertEqual(num_partitions_int_2, 2)

    def test_exists(self):
        cluster = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        self.assertFalse(cluster.exists(topic_str))
        cluster.create(topic_str)
        self.assertTrue(cluster.exists(topic_str))

    # Produce/Consume

    def test_produce_consume_bytes_str(self):
        cluster = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        cluster.create(topic_str)
        # Upon produce, the types "bytes" and "string" trigger the conversion of bytes, strings and dictionaries to bytes on Kafka.
        producer = cluster.openw(topic_str, key_type="bytes", value_type="str")
        producer.produce(self.snack_str_list, key=self.snack_str_list)
        producer.close()
        self.assertEqual(cluster.size(topic_str)[topic_str][0], 3)
        #
        group_str = self.create_test_group_name()
        # Upon consume, the type "str" triggers the conversion into a string, and "bytes" into bytes.
        consumer = cluster.openr(topic_str, group=group_str, key_type="str", value_type="bytes")
        message_dict_list = consumer.consume(n=3)
        key_str_list = [message_dict["key"] for message_dict in message_dict_list]
        value_bytes_list = [message_dict["value"] for message_dict in message_dict_list]
        snack_bytes_list = [bytes(snack_str, encoding="utf-8") for snack_str in self.snack_str_list]
        self.assertEqual(key_str_list, self.snack_str_list)
        self.assertEqual(value_bytes_list, snack_bytes_list)
        consumer.close()
    
    def test_produce_consume_json(self):
        cluster = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        cluster.create(topic_str)
        # Upon produce, the types "str" and "json" trigger the conversion of bytes, strings and dictionaries to bytes on Kafka.
        producer = cluster.openw(topic_str, key_type="str", value_type="json")
        snack_dict_list = [json.loads(snack_str) for snack_str in self.snack_str_list]
        producer.produce(snack_dict_list, key=self.snack_str_list)
        producer.close()
        self.assertEqual(cluster.size(topic_str)[topic_str][0], 3)
        #
        group_str = self.create_test_group_name()
        # Upon consume, the type "json" triggers the conversion into a dictionary, and "str" into a string.
        consumer = cluster.openr(topic_str, group=group_str, key_type="json", value_type="str")
        (message_dict_list, _) = consumer.read(n=3)
        key_dict_list = [message_dict["key"] for message_dict in message_dict_list]
        value_str_list = [message_dict["value"] for message_dict in message_dict_list]
        self.assertEqual(key_dict_list, snack_dict_list)
        self.assertEqual(value_str_list, self.snack_str_list)
        consumer.close()

    def test_produce_consume_protobuf(self):
        schema_str = 'message Snack { required string name = 1; required float calories = 2; optional string colour = 3; }'
        #
        cluster = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        cluster.create(topic_str)
        # Upon produce, the type "protobuf" (alias = "pb") triggers the conversion of bytes, strings and dictionaries into Protobuf-encoded bytes on Kafka.
        producer = cluster.openw(topic_str, key_type="protobuf", value_type="pb", key_schema=schema_str, value_schema=schema_str)
        snack_dict_list = [json.loads(snack_str) for snack_str in self.snack_str_list]
        producer.produce(snack_dict_list, key=self.snack_str_list)
        producer.close()
        self.assertEqual(cluster.size(topic_str)[topic_str][0], 3)
        #
        group_str = self.create_test_group_name()
        # Upon consume, the type "protobuf" (alias = "pb") triggers the conversion into a dictionary.
        consumer = cluster.openr(topic_str, group=group_str, key_type="pb", value_type="protobuf")
        (message_dict_list, _) = consumer.read(n=3)
        key_dict_list = [message_dict["key"] for message_dict in message_dict_list]
        value_dict_list = [message_dict["value"] for message_dict in message_dict_list]
        self.assertEqual(key_dict_list, snack_dict_list)
        self.assertEqual(value_dict_list, snack_dict_list)
        consumer.close()

    def test_produce_consume_protobuf_avro(self):
        protobuf_schema_str = 'message Snack { required string name = 1; required float calories = 2; optional string colour = 3; }'
        avro_schema_str = '{ "type": "record", "name": "myrecord", "fields": [{"name": "name",  "type": "string" }, {"name": "calories", "type": "float" }, {"name": "colour", "type": "string" }] }'
        #
        cluster = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        cluster.create(topic_str)
        # Upon produce, the type "protobuf" (alias = "pb") triggers the conversion of bytes, strings and dictionaries into Protobuf-encoded bytes on Kafka, and "avro" into Avro-encoded bytes.
        producer = cluster.openw(topic_str, key_type="protobuf", value_type="avro", key_schema=protobuf_schema_str, value_schema=avro_schema_str)
        snack_dict_list = [json.loads(snack_str) for snack_str in self.snack_str_list]
        snack_bytes_list = [bytes(snack_str, encoding="utf-8") for snack_str in self.snack_str_list]
        producer.produce(snack_dict_list, key=snack_bytes_list)
        producer.close()
        self.assertEqual(cluster.size(topic_str)[topic_str][0], 3)
        #
        group_str = self.create_test_group_name()
        # Upon consume, the types "protobuf" (alias = "pb") and "avro" trigger the conversion into a dictionary.
        consumer = cluster.openr(topic_str, group=group_str, key_type="pb", value_type="avro")
        (message_dict_list, _) = consumer.read(n=3)
        key_dict_list = [message_dict["key"] for message_dict in message_dict_list]
        value_dict_list = [message_dict["value"] for message_dict in message_dict_list]
        self.assertEqual(key_dict_list, snack_dict_list)
        self.assertEqual(value_dict_list, snack_dict_list)
        consumer.close()

    def test_produce_consume_str_jsonschema(self):
        schema_str = '{ "title": "abc", "definitions" : { "record:myrecord" : { "type" : "object", "required" : [ "name", "calories" ], "additionalProperties" : false, "properties" : { "name" : {"type" : "string"}, "calories" : {"type" : "number"}, "colour" : {"type" : "string"} } } }, "$ref" : "#/definitions/record:myrecord" }'
        #
        cluster = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        cluster.create(topic_str)
        # Upon produce, the type "str" triggers the conversion of bytes, strings and dictionaries into bytes on Kafka, and "jsonschema" (alias = "json_sr") into Protobuf/Avro-encoded bytes on Kafka.
        producer = cluster.openw(topic_str, key_type="str", value_type="jsonschema", value_schema=schema_str)
        snack_dict_list = [json.loads(snack_str) for snack_str in self.snack_str_list]
        producer.produce(snack_dict_list, key=self.snack_str_list)
        producer.close()
        self.assertEqual(cluster.size(topic_str)[topic_str][0], 3)
        #
        group_str = self.create_test_group_name()
        # Upon consume, the types "json" and "jsonschema" (alias = "json_sr") trigger the conversion into a dictionary.
        consumer = cluster.openr(topic_str, group=group_str, key_type="json", value_type="json_sr")
        (message_dict_list, _) = consumer.read(n=3)
        key_dict_list = [message_dict["key"] for message_dict in message_dict_list]
        value_dict_list = [message_dict["value"] for message_dict in message_dict_list]
        self.assertEqual(key_dict_list, snack_dict_list)
        self.assertEqual(value_dict_list, snack_dict_list)
        consumer.close()

    def test_consume_from_offsets(self):
        cluster = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        cluster.create(topic_str)
        producer = cluster.openw(topic_str)
        producer.produce("message 1")
        producer.produce("message 2")
        producer.produce("message 3")
        producer.close()
        #
        group_str = self.create_test_group_name()
        consumer = cluster.openr(topic_str, group=group_str, offsets={0: 2})
        message_dict_list = consumer.consume()
        self.assertEqual(len(message_dict_list), 1)
        self.assertEqual(message_dict_list[0]["value"], "message 3")
        consumer.close()

    def test_commit(self):
        cluster = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        cluster.create(topic_str)
        producer = cluster.openw(topic_str)
        producer.produce("message 1")
        producer.produce("message 2")
        producer.produce("message 3")
        producer.close()
        #
        group_str = self.create_test_group_name()
        consumer = cluster.openr(topic_str, group=group_str, config={"enable.auto.commit": "False"})
        consumer.consume()
        offsets_dict = consumer.offsets()
        self.assertEqual(offsets_dict[topic_str][0], OFFSET_INVALID)
        consumer.commit()
        offsets_dict1 = consumer.offsets()
        self.assertEqual(offsets_dict1[topic_str][0], 1)
        consumer.close()
    
    def test_cluster_settings(self):
        cluster = Cluster(cluster_str)
        #
        cluster.verbose(0)
        self.assertEqual(cluster.verbose(), 0)

    def test_configs(self):
        cluster = Cluster(cluster_str)
        #
        config_str_list1 = cluster.configs()
        self.assertIn("local", config_str_list1)
        config_str_list2 = cluster.configs("loc*")
        self.assertIn("local", config_str_list2)
        config_str_list3 = cluster.configs("this_pattern_shall_not_match_anything")
        self.assertEqual(config_str_list3, [])
        #
        config_str_config_dict_dict = cluster.configs(verbose=True)
        self.assertIn("local", config_str_config_dict_dict)
        self.assertEqual(True, config_str_config_dict_dict["local"]["kash"]["enable.auto.commit"])
