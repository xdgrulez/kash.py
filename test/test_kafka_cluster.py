import filecmp
import os
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

class Test(unittest.TestCase):
    def setUp(self):
        warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)
        #
        self.old_home_str = os.environ.get("KASHPY_HOME")
        os.environ["KASHPY_HOME"] = "."
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
        #
        if self.old_home_str:
            os.environ["KASHPY_HOME"] = self.old_home_str

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
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        cluster.produce(topic_str, "message 1")
        cluster.produce(topic_str, "message 2")
        cluster.produce(topic_str, "message 3")
        cluster.flush()
        cluster.cat(topic_str, n=3)
        cluster.subscribe(topic_str, config={"enable.auto.commit": "False"})
        cluster.consume()
        offsets_dict = cluster.offsets()
        self.assertEqual(offsets_dict[0], "OFFSET_INVALID")
        cluster.commit()
        offsets_dict1 = cluster.offsets()
        self.assertEqual(offsets_dict1[0], 1)
        cluster.close()
        cluster.delete(topic_str)

    def test_errors(self):
        cluster = Cluster(cluster_str)
        cluster.cp("./abc", "./abc")
        cluster.consume("abc")
    
    def test_cluster_settings(self):
        cluster = Cluster(cluster_str)
        cluster.verbose(0)
        self.assertEqual(cluster.verbose(), 0)

    def test_transforms_string(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.touch(topic_str)
        cluster.cp("./snacks_value.txt", topic_str, target_value_type="str")
        self.assertEqual(cluster.size(topic_str)[topic_str][0], 3)
        #
        def map_function(message_dict):
            value_dict = json.loads(message_dict["value"])
            value_dict["colour"] += "ish"
            value_str = json.dumps(value_dict)
            message_dict["value"] = value_str
            return message_dict
        cluster.create(f"{topic_str}_1")
        map(cluster, topic_str, cluster, f"{topic_str}_1", map_function, source_value_type="str", n=2)
        self.assertEqual(cluster.size(f"{topic_str}_1")[f"{topic_str}_1"][0], 2)
        #
        cluster.subscribe(f"{topic_str}_1", value_type="str")
        message_dict_list = cluster.consume(n=2)
        for message_dict in message_dict_list:
            value_dict = json.loads(message_dict["value"])            
            self.assertRegex(value_dict["colour"], ".*ish")
        #
        cluster.delete(topic_str)
        cluster.delete(f"{topic_str}_1")

    def test_transforms_json(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        cluster.cp("./snacks_value.txt", topic_str, target_value_type="json", n=2)
        self.assertEqual(cluster.size(topic_str)[topic_str][0], 2)
        #
        def map_function(message_dict):
            message_dict["value"]["colour"] += "ish"
            return message_dict
        cluster.create(f"{topic_str}_1")
        map(cluster, topic_str, cluster, f"{topic_str}_1", map_function, source_value_type="json", n=3)
        #
        cluster.subscribe(f"{topic_str}_1", value_type="json")
        message_dict_list = cluster.consume(n=2)
        for message_dict in message_dict_list:
            self.assertRegex(message_dict["value"]["colour"], ".*ish")
        #
        cluster.delete(topic_str)
        cluster.delete(f"{topic_str}_1")

    def test_transforms_protobuf(self):
        schema_str = 'message Snack { required string name = 1; required float calories = 2; optional string colour = 3; }'
        #
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        cluster.cp("./snacks_key_value.txt", topic_str, target_key_type="pb", target_value_type="pb", target_key_schema=schema_str, target_value_schema=schema_str, key_value_separator="/")
        self.assertEqual(cluster.size(topic_str)[topic_str][0], 3)
        #
        def map_function(message_dict):
            message_dict["key"]["colour"] += "ishy"
            message_dict["value"]["colour"] += "ish"
            return message_dict
        cluster.create(f"{topic_str}_1")
        map(cluster, topic_str, cluster, f"{topic_str}_1", map_function, source_key_type="pb", source_value_type="pb", n=3)
        #
        cluster.subscribe(f"{topic_str}_1", key_type="pb", value_type="pb")
        message_dict_list = cluster.consume(n=3)
        for message_dict in message_dict_list:
            self.assertRegex(message_dict["key"]["colour"], ".*ishy")
            self.assertRegex(message_dict["value"]["colour"], ".*ish")
        #
        cluster.delete(topic_str)
        cluster.delete(f"{topic_str}_1")

    def test_transforms_avro(self):
        schema_str = '{ "type": "record", "name": "myrecord", "fields": [{"name": "name",  "type": "string" }, {"name": "calories", "type": "float" }, {"name": "colour", "type": "string" }] }'
        #
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        cluster.cp("./snacks_value.txt", topic_str, target_value_type="avro", target_value_schema=schema_str)
        self.assertEqual(cluster.size(topic_str)[topic_str][0], 3)
        #
        def map_function(message_dict):
            message_dict["value"]["colour"] += "ish"
            return message_dict
        cluster.create(f"{topic_str}_1")
        map(cluster, topic_str, cluster, f"{topic_str}_1", map_function, source_value_type="avro", n=3)
        #
        cluster.subscribe(f"{topic_str}_1", value_type="avro")
        message_dict_list = cluster.consume(n=3)
        for message_dict in message_dict_list:
            self.assertRegex(message_dict["value"]["colour"], ".*ish")
        #
        cluster.delete(topic_str)
        cluster.delete(f"{topic_str}_1")

    def test_transforms_jsonschema(self):
        schema_str = '{ "title": "abc", "definitions" : { "record:myrecord" : { "type" : "object", "required" : [ "name", "calories" ], "additionalProperties" : false, "properties" : { "name" : {"type" : "string"}, "calories" : {"type" : "number"}, "colour" : {"type" : "string"} } } }, "$ref" : "#/definitions/record:myrecord" }'
        #
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        cluster.cp("./snacks_value.txt", topic_str, target_value_type="jsonschema", target_value_schema=schema_str)
        self.assertEqual(cluster.size(topic_str)[topic_str][0], 3)
        #
        def map_function(message_dict):
            message_dict["value"]["colour"] += "ish"
            return message_dict
        cluster.create(f"{topic_str}_1")
        map(cluster, topic_str, cluster, f"{topic_str}_1", map_function, source_value_type="jsonschema", n=3)
        #
        cluster.subscribe(f"{topic_str}_1", value_type="jsonschema")
        message_dict_list = cluster.consume(n=3)
        for message_dict in message_dict_list:
            self.assertRegex(message_dict["value"]["colour"], ".*ish")
        #
        cluster.delete(topic_str)
        cluster.delete(f"{topic_str}_1")

    def test_transforms_bytes(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        cluster.cp("./snacks_value.txt", topic_str, target_value_type="str")
        self.assertEqual(cluster.size(topic_str)[topic_str][0], 3)
        #
        def flatmap_function(message_dict):
            value_bytearray = bytearray(message_dict["value"])
            value_bytearray[10] = ord("X")
            message_dict["value"] = bytes(value_bytearray)
            return [message_dict]
        cluster.create(f"{topic_str}_1")
        flatmap(cluster, topic_str, cluster, f"{topic_str}_1", flatmap_function, source_value_type="bytes", n=3)
        #
        cluster.subscribe(f"{topic_str}_1", value_type="bytes")
        message_dict_list = cluster.consume(n=3)
        for message_dict in message_dict_list:            
            value_bytes = message_dict["value"]
            self.assertEqual(value_bytes[10], ord("X"))     
        #
        cluster.delete(topic_str)
        cluster.delete(f"{topic_str}_1")

    def test_grep(self):
        schema_str = '{ "type": "record", "name": "myrecord", "fields": [{"name": "name",  "type": "string" }, {"name": "calories", "type": "float" }, {"name": "colour", "type": "string" }] }'
        #
        cluster = Cluster(cluster_str)
        cluster.verbose(1)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        cluster.cp("./snacks_value.txt", topic_str, target_value_type="avro", target_value_schema=schema_str)
        self.assertEqual(cluster.size(topic_str)[topic_str][0], 3)
        #
        (message_dict_list, num_matching_messages_int, message_counter_int) = cluster.grep(topic_str, ".*name.*cake", value_type="avro")
        self.assertEqual(len(message_dict_list), 1)
        self.assertEqual(num_matching_messages_int, 1)
        self.assertEqual(message_counter_int, 3)
        #
        (message_dict_list, num_matching_messages_int, message_counter_int) = cluster.grep_fun(topic_str, lambda message_dict: message_dict["value"]["name"] == "cake", value_type="avro", offsets={0: 2})
        self.assertEqual(len(message_dict_list), 0)
        self.assertEqual(num_matching_messages_int, 0)
        self.assertEqual(message_counter_int, 1)
        #
        cluster.delete(topic_str)
    
    def test_replicate_change_schema(self):
        avro_schema_str = '{ "type": "record", "name": "myrecord", "fields": [{"name": "name",  "type": "string" }, {"name": "calories", "type": "float" }, {"name": "colour", "type": "string" }] }'
        #
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        cluster.cp("./snacks_value.txt", topic_str, target_value_type="avro", target_value_schema=avro_schema_str)
        self.assertEqual(cluster.size(topic_str)[topic_str][0], 3)
        #
        protobuf_schema_str = 'message Snack { required string name = 1; required float calories = 2; optional string colour = 3; }'
        cluster.create(f"{topic_str}_1")
        (consumed_message_counter_int, produced_message_counter_int) = cluster.cp(topic_str, f"{topic_str}_1", source_value_type="avro", target_value_type="pb", target_value_schema=protobuf_schema_str, n=3)
        self.assertEqual(consumed_message_counter_int, 3)
        self.assertEqual(produced_message_counter_int, 3)
        #
        cluster.cp(f"{topic_str}_1", "./snacks_value1.txt", source_value_type="pb", n=3)
        self.assertTrue(filecmp.cmp("./snacks_value.txt", "./snacks_value1.txt"))
        os.remove("./snacks_value1.txt")
        #
        cluster.delete(topic_str)
        cluster.delete(f"{topic_str}_1")

    def test_flush_timeout_bug(self):
        cluster = Cluster(cluster_str)
        #
        for i in range(3):
            print(i)
            topic_str = create_test_topic_name()
            cluster.create(topic_str)
            cluster.create(f"{topic_str}_1")
            #
            cluster.cp("./snacks_value.txt", topic_str, target_value_type="str")
            self.assertEqual(cluster.size(topic_str)[topic_str][0], 3)
            cluster.cp(topic_str, "./snacks_value1.txt", source_value_type="str", n=3)
            cluster.cp("./snacks_value1.txt", f"{topic_str}_1", target_value_type="str")
            self.assertEqual(cluster.size(f"{topic_str}_1")[f"{topic_str}_1"][0], 3)
            #
            cluster.delete(topic_str)
            cluster.delete(f"{topic_str}_1")
    
    def test_diff(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        cluster.cp("./snacks_value.txt", topic_str, target_value_type="json")
        self.assertEqual(cluster.size(topic_str)[topic_str][0], 3)
        #
        cluster.create(f"{topic_str}_1")
        cluster.cp("./snacks_value.txt", f"{topic_str}_1", target_value_type="json")
        self.assertEqual(cluster.size(f"{topic_str}_1")[f"{topic_str}_1"][0], 3)
        #
        def map_function(message_dict):
            if message_dict["value"]["name"] == "cookie":
                message_dict["value"]["colour"] += "ish"
            return message_dict
        cluster.create(f"{topic_str}_2")
        map(cluster, topic_str, cluster, f"{topic_str}_2", map_function, source_value_type="json", n=3)
        self.assertEqual(cluster.size(f"{topic_str}_2")[f"{topic_str}_2"][1][0], 3)
        #
        def break_function(message_dict1, _):
            value_dict1 = json.loads(message_dict1["value"])
            if value_dict1["colour"] == "white":
                return True
            else:
                return False
        #
        cluster2 = Cluster(cluster_str)
        cluster.verbose(1)
        differing_message_dict_tuple_list, num_messages_int1, num_messages_int2 = diff(cluster, topic_str, cluster2, f"{topic_str}_1", break_function=break_function)
        self.assertEqual(differing_message_dict_tuple_list, [])
        self.assertEqual(num_messages_int1, 2)
        self.assertEqual(num_messages_int2, 2)
        #
        differing_message_dict_tuple_list1, num_messages_int1, num_messages_int2 = cluster.diff(topic_str, f"{topic_str}_2", value_type1="json", value_type2="json")
        self.assertEqual(len(differing_message_dict_tuple_list1), 1)
        self.assertEqual(num_messages_int1, 3)
        self.assertEqual(num_messages_int2, 3)
        self.assertEqual(differing_message_dict_tuple_list1[0][0]["value"]["name"], "cookie")
        #
        differing_message_dict_tuple_list1, num_messages_int1, num_messages_int2 = cluster.diff_fun(topic_str, f"{topic_str}_2", lambda x, y: x["value"] != y["value"], value_type1="json", value_type2="json")
        self.assertEqual(len(differing_message_dict_tuple_list1), 1)
        self.assertEqual(num_messages_int1, 3)
        self.assertEqual(num_messages_int2, 3)
        self.assertEqual(differing_message_dict_tuple_list1[0][0]["value"]["name"], "cookie")
        #
        acc, num_messages_int1, num_messages_int2 = cluster.zip_foldl(topic_str, f"{topic_str}_2", lambda acc, x, y: acc + [(x["value"], y["value"])], [], value_type1="json", value_type2="json")
        self.assertEqual(len(acc), 3)
        self.assertEqual(num_messages_int1, 3)
        self.assertEqual(num_messages_int2, 3)
        self.assertEqual(acc[0][0]["name"], "cookie")
        #
        cluster.delete(topic_str)
        cluster.delete(f"{topic_str}_1")
        cluster.delete(f"{topic_str}_2")

    def test_upload_flatmap(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        #
        def flatmap_function(key_str_value_str_tuple):
            return [(key_str_value_str_tuple[0], key_str_value_str_tuple[1]), (key_str_value_str_tuple[0], key_str_value_str_tuple[1])]
        #
        def break_function(key_str_value_str_tuple):
            value_str = key_str_value_str_tuple[1]
            return "white" in value_str
        #
        cluster.cp("./snacks_value.txt", topic_str, flatmap_function=flatmap_function, break_function=break_function, target_value_type="str")
        self.assertEqual(cluster.size(topic_str)[topic_str][0], 2)
        #
        cluster.delete(topic_str)


    def test_download_flatmap(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        #
        cluster.cp("./snacks_value.txt", topic_str, target_value_type="str")
        self.assertEqual(cluster.size(topic_str)[topic_str][0], 3)
        #
        def flatmap_function(message_dict):
            return [message_dict, message_dict]
        (message_counter_int, line_counter_int) = cluster.cp(topic_str, "./snacks_value1.txt", flatmap_function=flatmap_function, target_value_type="str")
        self.assertEqual(message_counter_int, 3)
        self.assertEqual(line_counter_int, 6)
        lines_count_int = count_lines("./snacks_value1.txt")
        self.assertEqual(lines_count_int, 6)
        #
        os.remove("./snacks_value1.txt")
        cluster.delete(topic_str)

    def test_wc(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        #
        cluster.cp("./snacks_value.txt", topic_str, target_value_type="str")
        self.assertEqual(cluster.size(topic_str)[topic_str][0], 3)
        #
        (num_messages_int, num_words_int, num_bytes_int) = cluster.wc(topic_str)
        self.assertEqual(num_messages_int, 3)
        self.assertEqual(num_words_int, 18)
        self.assertEqual(num_bytes_int, 169)
        #
        cluster.delete(topic_str)
        #
        cluster.create(f"{topic_str}_1")
        cluster.cp("./snacks_key_value.txt", f"{topic_str}_1", target_key_type="str", target_value_type="str", key_value_separator="/")
        self.assertEqual(cluster.size(f"{topic_str}_1")[f"{topic_str}_1"][0], 3)
        #
        (num_messages_int, num_words_int, num_bytes_int) = cluster.wc(f"{topic_str}_1")
        self.assertEqual(num_messages_int, 3)
        self.assertEqual(num_words_int, 36)
        self.assertEqual(num_bytes_int, 368)
        #
        cluster.delete(f"{topic_str}_1")

    def test_map(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        #
        cluster.cp("./snacks_value.txt", topic_str, target_value_type="str")
        self.assertEqual(cluster.size(topic_str)[topic_str][0], 3)
        #
        def map_function(message_dict):
            value_dict = json.loads(message_dict["value"])
            value_dict["colour"] += "ish"
            value_str = json.dumps(value_dict)
            message_dict["value"] = value_str
            return message_dict
        #
        def break_function(message_dict):
            value_dict = json.loads(message_dict["value"])
            if value_dict["colour"] == "white":
                return True
            else:
                return False
        #
        message_dict_list, num_messages_int = cluster.map(topic_str, map_function, break_function=break_function)
        self.assertEqual(json.loads(message_dict_list[0]["value"])["colour"], "brownish")
        self.assertEqual(num_messages_int, 2)
        #
        cluster.delete(topic_str)

    def test_filter(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        #
        cluster.cp("./snacks_value.txt", topic_str, target_value_type="str")
        self.assertEqual(cluster.size(topic_str)[topic_str][0], 3)
        #
        def filter_function(message_dict):
            return message_dict["value"]["colour"] == "brown"
        #
        message_dict_list, _ = cluster.filter(topic_str, filter_function, value_type="json")
        self.assertEqual(len(message_dict_list), 1)
        #
        (num_messages_int, produced_messages_counter_int) = filter(cluster, topic_str, cluster, f"{topic_str}_1", filter_function, source_value_type="json")
        self.assertEqual(num_messages_int, 3)
        self.assertEqual(produced_messages_counter_int, 1)
        #
        cluster.delete(topic_str)
        cluster.delete(f"{topic_str}_1")

    def test_map_filter_to_from_file(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        #
        cluster.map_from_file("./snacks_value.txt", topic_str, lambda x: x)
        self.assertEqual(cluster.size(topic_str)[topic_str][0], 3)
        #
        def map_function(message_dict):
            message_dict["value"]["colour"] = "brown"
            return message_dict
        (message_counter_int, line_counter_int) = cluster.map_to_file(topic_str, "./snacks_value1.txt", map_function=map_function, value_type="json")
        self.assertEqual(message_counter_int, 3)
        self.assertEqual(line_counter_int, 3)
        #
        def filter_function(key_str_value_str_tuple):
            _, value_str = key_str_value_str_tuple
            return json.loads(value_str)["name"] == "timtam"
        (lines_counter_int, produced_messages_counter_int) = cluster.filter_from_file("./snacks_value1.txt", topic_str, filter_function)
        self.assertEqual(lines_counter_int, 3)
        self.assertEqual(produced_messages_counter_int, 1)
        #
        def filter_function(message_dict):
            return message_dict["value"]["name"] == "timtam"
        (message_counter_int, line_counter_int) = cluster.filter_to_file(topic_str, "./snacks_value2.txt", filter_function, value_type="json")
        self.assertEqual(message_counter_int, 4)
        self.assertEqual(line_counter_int, 2)
        #
        os.remove("./snacks_value1.txt")
        os.remove("./snacks_value2.txt")
        cluster.delete(topic_str)

    def test_head_tail(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        #
        cluster.cp("./snacks_value.txt", topic_str, flatmap_function=lambda x: [x, x, x, x], target_value_type="str")
        self.assertEqual(cluster.size(topic_str)[topic_str][0], 12)
        #
        topic_str_message_dict_list_dict = cluster.head(topic_str)
        self.assertEqual(len(topic_str_message_dict_list_dict[topic_str]), 10)
        self.assertEqual(topic_str_message_dict_list_dict[topic_str][0]["offset"], 0)
        self.assertEqual(topic_str_message_dict_list_dict[topic_str][9]["offset"], 9)
        #
        topic_str_message_dict_list_dict = cluster.tail(topic_str)
        self.assertEqual(len(topic_str_message_dict_list_dict[topic_str]), 10)
        self.assertEqual(topic_str_message_dict_list_dict[topic_str][0]["offset"], 2)
        self.assertEqual(topic_str_message_dict_list_dict[topic_str][9]["offset"], 11)
        #
        cluster.delete(topic_str)

    def test_cp_no_target_schema(self):
        schema_str = '{ "type": "record", "name": "myrecord", "fields": [{"name": "name",  "type": "string" }, {"name": "calories", "type": "float" }, {"name": "colour", "type": "string" }] }'
        # Create topic with three Avro-encoded messages using the value schema schema_str.
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        cluster.cp("./snacks_value.txt", topic_str, target_value_type="avro", target_value_schema=schema_str)
        self.assertEqual(cluster.size(topic_str)[topic_str][0], 3)
        # Copy the topic to another topic *with* setting the target value schema explicitly.
        cluster.cp(topic_str, f"{topic_str}_1", source_value_type="avro", target_value_type="avro", target_value_schema=schema_str)
#        self.assertEqual(cluster.size(topic_str)[topic_str][0], 3)
#        diff_tuple = cluster.diff(topic_str, f"{topic_str}_1")
#        self.assertEqual(diff_tuple[0], [])
        # Copy the topic to another topic *without* setting the target value schema explicitly.
        cluster.cp(topic_str, f"{topic_str}_2", source_value_type="avro", target_value_type="avro")
#        self.assertEqual(cluster.size(topic_str)[topic_str][0], 3)
#        diff_tuple = cluster.diff(topic_str, f"{topic_str}_2")
#        self.assertEqual(diff_tuple[0], [])
        #
        cluster.delete(topic_str)
        cluster.delete(f"{topic_str}_1")
        cluster.delete(f"{topic_str}_2")

    def test_clusters(self):
        cluster_str_list1 = clusters()
        self.assertIn("local", cluster_str_list1)
        cluster_str_list2 = clusters("loc*")
        self.assertIn("local", cluster_str_list2)
        cluster_str_list3 = clusters("this_pattern_shall_not_match_anything")
        self.assertEqual(cluster_str_list3, [])
