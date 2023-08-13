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
        self.snack_bytes_list = [bytes(snack_str, encoding="utf-8") for snack_str in self.snack_str_list]
        self.snack_dict_list = [json.loads(snack_str) for snack_str in self.snack_str_list]
        #
        self.snack_ish_dict_list = []
        for snack_dict in self.snack_dict_list:
            snack_dict1 = snack_dict.copy()
            snack_dict1["colour"] += "ish"
            self.snack_ish_dict_list.append(snack_dict1)
        #
        self.avro_schema_str = '{ "type": "record", "name": "myrecord", "fields": [{"name": "name",  "type": "string" }, {"name": "calories", "type": "float" }, {"name": "colour", "type": "string" }] }'
        self.protobuf_schema_str = 'message Snack { required string name = 1; required float calories = 2; optional string colour = 3; }'
        self.jsonschema_schema_str = '{ "title": "abc", "definitions" : { "record:myrecord" : { "type" : "object", "required" : [ "name", "calories" ], "additionalProperties" : false, "properties" : { "name" : {"type" : "string"}, "calories" : {"type" : "number"}, "colour" : {"type" : "string"} } } }, "$ref" : "#/definitions/record:myrecord" }'
        #
        self.topic_str_list = []
        self.group_str_list = []
        #
        print("Test:", self._testMethodName)

    def tearDown(self):
        c = Cluster(cluster_str)
        for group_str in self.group_str_list:
            c.delete_groups(group_str)
        for topic_str in self.topic_str_list:
            c.delete(topic_str)

    def create_test_topic_name(self):
        while True:
            topic_str = f"test_topic_{get_millis()}"
            #
            if topic_str not in self.topic_str_list:
                self.topic_str_list.append(topic_str)
                break
        #
        return topic_str

    def create_test_group_name(self):
        while True:
            group_str = f"test_group_{get_millis()}"
            #
            if group_str not in self.group_str_list:
                self.group_str_list.append(group_str)
                break
        #
        return group_str

    ### ClusterAdmin
    # ACLs

    def test_acls(self):
        if principal_str:
            c = Cluster(cluster_str)
            topic_str = self.create_test_topic_name()
            c.create(topic_str)
            c.create_acl(restype="topic", name=topic_str, resource_pattern_type="literal", principal=principal_str, host="*", operation="read", permission_type="allow")
            acl_dict_list = c.acls()
            self.assertIn({"restype": "topic", "name": topic_str, "resource_pattern_type": "literal", 'principal': principal_str, 'host': '*', 'operation': 'read', 'permission_type': 'ALLOW'}, acl_dict_list)
            c.delete_acl(restype="topic", name=topic_str, resource_pattern_type="literal", principal=principal_str, host="*", operation="read", permission_type="allow")
            self.assertIn({"restype": "topic", "name": topic_str, "resource_pattern_type": "literal", 'principal': principal_str, 'host': '*', 'operation': 'read', 'permission_type': 'ALLOW'}, acl_dict_list)
            c.delete(topic_str)

    # Brokers
    
    def test_brokers(self):
        c = Cluster(cluster_str)
        if "confluent.cloud" not in c.kafka_config_dict["bootstrap.servers"]: # cannot modify cluster properties of non-dedicated Confluent Cloud test cluster
            broker_dict = c.brokers()
            broker_int = list(broker_dict.keys())[0]
            broker_dict1 = c.brokers(f"{broker_int}")
            self.assertEqual(broker_dict, broker_dict1)
            broker_dict2 = c.brokers([broker_int])
            self.assertEqual(broker_dict1, broker_dict2)
            broker_int = list(broker_dict.keys())[0]
            old_background_threads_str = c.broker_config(broker_int)[broker_int]["background.threads"]
            c.set_broker_config(broker_int, {"background.threads": 5})
            new_background_threads_str = c.broker_config(broker_int)[broker_int]["background.threads"]
            self.assertEqual(new_background_threads_str, "5")
            c.set_broker_config(broker_int, {"background.threads": old_background_threads_str})

    # Groups

    def test_groups(self):
        c = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        c.create(topic_str)
        w = c.openw(topic_str)
        w.produce("message 1")
        w.produce("message 2")
        w.produce("message 3")
        w.close()
        #
        group_str = self.create_test_group_name()
        r = c.openr(topic_str, group=group_str)
        r.consume()
        #
        group_str_list1 = c.groups(["test*", "test_group*"])
        self.assertIn(group_str, group_str_list1)
        group_str_list2 = c.groups("test_group*")
        self.assertIn(group_str, group_str_list2)
        group_str_list3 = c.groups("test_group*", state_pattern=["stable"])
        self.assertIn(group_str, group_str_list3)
        group_str_state_str_dict = c.groups("test_group*", state_pattern="stab*", state=True)
        self.assertIn("stable", group_str_state_str_dict[group_str])
        group_str_list4 = c.groups(state_pattern="unknown", state=False)
        self.assertEqual(group_str_list4, [])
        #
        r.close()

    def test_describe_groups(self):
        c = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        c.create(topic_str)
        w = c.openw(topic_str)
        w.produce("message 1")
        w.produce("message 2")
        w.produce("message 3")
        w.close()
        #
        group_str = self.create_test_group_name()
        r = c.openr(topic_str, group=group_str)
        r.consume()
        #
        group_dict = c.describe_groups(group_str)[group_str]
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
        broker_dict = c.brokers()
        broker_int = list(broker_dict.keys())[0]
        self.assertEqual(group_dict["coordinator"]["id"], broker_int)
        self.assertEqual(group_dict["coordinator"]["id_string"], f"{broker_int}")
        #
        r.close()

    def test_delete_groups(self):
        c = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        c.create(topic_str)
        w = c.openw(topic_str)
        w.produce("message 1")
        w.produce("message 2")
        w.produce("message 3")
        w.close()
        #
        group_str = self.create_test_group_name()
        r = c.openr(topic_str, group=group_str)
        r.consume()
        #
        group_str_list = c.delete_groups(group_str, state_pattern=["empt*"])
        self.assertEqual(group_str_list, [])
        group_str_list = c.groups(group_str, state_pattern="*")
        self.assertEqual(group_str_list, [group_str])
        #
        r.close()

    def test_group_offsets_member_id(self):
        c = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        c.create(topic_str, partitions=2)
        w = c.openw(topic_str)
        w.produce("message 1", partition=0)
        w.produce("message 2", partition=1)
        w.close()
        #
        group_str = self.create_test_group_name()
        r = c.openr(topic_str, group=group_str, config={"enable.auto.commit": False})
        r.consume()
        r.commit()
        r.consume()
        r.commit()
        #
        group_str_topic_str_partition_int_offset_int_dict_dict_dict = c.group_offsets(group_str)
        self.assertIn(group_str, group_str_topic_str_partition_int_offset_int_dict_dict_dict)
        self.assertIn(topic_str, group_str_topic_str_partition_int_offset_int_dict_dict_dict[group_str])
        self.assertEqual(group_str_topic_str_partition_int_offset_int_dict_dict_dict[group_str][topic_str][0], 1)
        self.assertEqual(group_str_topic_str_partition_int_offset_int_dict_dict_dict[group_str][topic_str][1], 1)
        #
        member_id_str = r.memberid()
        self.assertEqual("rdkafka-", member_id_str[:8])
        #
        r.unsubscribe()
        r.close()

    def test_set_group_offsets(self):
        c = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        c.create(topic_str)
        w = c.openw(topic_str)
        w.produce("message 1")
        w.produce("message 2")
        w.produce("message 3")
        w.close()
        #
        group_str = self.create_test_group_name()
        r = c.openr(topic_str, group=group_str, config={"enable.auto.commit": False})
        group_str_topic_str_partition_int_offset_int_dict_dict_dict = c.set_group_offsets({group_str: {topic_str: {0: 2}}})
        self.assertEqual(group_str_topic_str_partition_int_offset_int_dict_dict_dict, {group_str: {topic_str: {0: 2}}})
        [message_dict] = r.consume()
        r.commit()
        self.assertEqual(message_dict["value"], "message 3")
        #
        r.close()

    # Topics

    def test_config_set_config(self):
        c = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        c.touch(topic_str)
        #
        c.set_config(topic_str, {"retention.ms": 4711})
        new_retention_ms_str = c.config(topic_str)[topic_str]["retention.ms"]
        self.assertEqual(new_retention_ms_str, "4711")

    def test_create_delete(self):
        c = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        c.touch(topic_str)
        topic_str_list = c.ls()
        self.assertIn(topic_str, topic_str_list)
        c.rm(topic_str)
        topic_str_list = c.ls()
        self.assertNotIn(topic_str, topic_str_list)

    def test_topics(self):
        c = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        old_topic_str_list = c.topics(["test_*"])
        self.assertNotIn(topic_str, old_topic_str_list)
        c.create(topic_str)
        new_topic_str_list = c.ls(["test_*"])
        self.assertIn(topic_str, new_topic_str_list)
        #
        w = c.openw(topic_str)
        w.produce("message 1", on_delivery=lambda kafkaError, message: print(kafkaError, message))
        w.produce("message 2")
        w.produce("message 3")
        w.close()
        #
        topic_str_size_int_dict_l = c.l(pattern=topic_str)
        topic_str_size_int_dict_ll = c.ll(pattern=topic_str)
        self.assertEqual(topic_str_size_int_dict_l, topic_str_size_int_dict_ll)
        size_int = topic_str_size_int_dict_l[topic_str]
        self.assertEqual(size_int, 3)
        topic_str_total_size_int_size_dict_tuple_dict = c.topics(pattern=topic_str, size=True, partitions=True)
        self.assertEqual(topic_str_total_size_int_size_dict_tuple_dict[topic_str][0], 3)
        self.assertEqual(topic_str_total_size_int_size_dict_tuple_dict[topic_str][1][0], 3)
        topic_str_total_size_int_size_dict_tuple_dict = c.topics(pattern=topic_str, size=False, partitions=True)
        self.assertEqual(topic_str_total_size_int_size_dict_tuple_dict[topic_str][0], 3)

    def test_offsets_for_times(self):
        c = Cluster(cluster_str)
        #
        c.verbose(1)
        #
        topic_str = self.create_test_topic_name()
        c.create(topic_str)
        w = c.openw(topic_str)
        w.produce("message 1")
        time.sleep(1)
        w.produce("message 2")
        w.close()
        #
        self.assertEqual(c.l(topic_str, partitions=True)[topic_str][1][0], 2)
        #
        group_str = self.create_test_group_name()
        r = c.openr(topic_str, group=group_str)
        message_dict_list = r.consume(n=2)
        r.close()
        message1_timestamp_int = message_dict_list[1]["timestamp"][1]
        message1_offset_int = message_dict_list[1]["offset"]
        #
        topic_str_partition_int_offset_int_dict_dict = c.offsets_for_times(topic_str, {0: message1_timestamp_int})
        found_message1_offset_int = topic_str_partition_int_offset_int_dict_dict[topic_str][0]
        self.assertEqual(message1_offset_int, found_message1_offset_int)

    def test_partitions_set_partitions(self):
        c = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        c.create(topic_str)
        num_partitions_int_1 = c.partitions(topic_str)[topic_str]
        self.assertEqual(num_partitions_int_1, 1)
        c.set_partitions(topic_str, 2)
        num_partitions_int_2 = c.partitions(topic_str)[topic_str]
        self.assertEqual(num_partitions_int_2, 2)

    def test_exists(self):
        c = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        self.assertFalse(c.exists(topic_str))
        c.create(topic_str)
        self.assertTrue(c.exists(topic_str))

    # Produce/Consume

    def test_produce_consume_bytes_str(self):
        c = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        c.create(topic_str)
        # Upon produce, the types "bytes" and "string" trigger the conversion of bytes, strings and dictionaries to bytes on Kafka.
        w = c.openw(topic_str, key_type="bytes", value_type="str")
        w.produce(self.snack_str_list, key=self.snack_str_list)
        w.close()
        self.assertEqual(c.size(topic_str)[topic_str][0], 3)
        #
        group_str = self.create_test_group_name()
        # Upon consume, the type "str" triggers the conversion into a string, and "bytes" into bytes.
        r = c.openr(topic_str, group=group_str, key_type="str", value_type="bytes")
        message_dict_list = r.consume(n=3)
        key_str_list = [message_dict["key"] for message_dict in message_dict_list]
        value_bytes_list = [message_dict["value"] for message_dict in message_dict_list]
        self.assertEqual(key_str_list, self.snack_str_list)
        self.assertEqual(value_bytes_list, self.snack_bytes_list)
        r.close()
    
    def test_produce_consume_json(self):
        c = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        c.create(topic_str)
        # Upon produce, the types "str" and "json" trigger the conversion of bytes, strings and dictionaries to bytes on Kafka.
        w = c.openw(topic_str, key_type="str", value_type="json")
        w.produce(self.snack_dict_list, key=self.snack_str_list)
        w.close()
        self.assertEqual(c.size(topic_str)[topic_str][0], 3)
        #
        group_str = self.create_test_group_name()
        # Upon consume, the type "json" triggers the conversion into a dictionary, and "str" into a string.
        r = c.openr(topic_str, group=group_str, key_type="json", value_type="str")
        message_dict_list = r.read(n=3)
        key_dict_list = [message_dict["key"] for message_dict in message_dict_list]
        value_str_list = [message_dict["value"] for message_dict in message_dict_list]
        self.assertEqual(key_dict_list, self.snack_dict_list)
        self.assertEqual(value_str_list, self.snack_str_list)
        r.close()

    def test_produce_consume_protobuf(self):
        c = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        c.create(topic_str)
        # Upon produce, the type "protobuf" (alias = "pb") triggers the conversion of bytes, strings and dictionaries into Protobuf-encoded bytes on Kafka.
        w = c.openw(topic_str, key_type="protobuf", value_type="pb", key_schema=self.protobuf_schema_str, value_schema=self.protobuf_schema_str)
        w.produce(self.snack_dict_list, key=self.snack_str_list)
        w.close()
        self.assertEqual(c.size(topic_str)[topic_str][0], 3)
        #
        group_str = self.create_test_group_name()
        # Upon consume, the type "protobuf" (alias = "pb") triggers the conversion into a dictionary.
        r = c.openr(topic_str, group=group_str, key_type="pb", value_type="protobuf")
        message_dict_list = r.read(n=3)
        key_dict_list = [message_dict["key"] for message_dict in message_dict_list]
        value_dict_list = [message_dict["value"] for message_dict in message_dict_list]
        self.assertEqual(key_dict_list, self.snack_dict_list)
        self.assertEqual(value_dict_list, self.snack_dict_list)
        r.close()

    def test_produce_consume_protobuf_avro(self):
        c = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        c.create(topic_str)
        # Upon produce, the type "protobuf" (alias = "pb") triggers the conversion of bytes, strings and dictionaries into Protobuf-encoded bytes on Kafka, and "avro" into Avro-encoded bytes.
        w = c.openw(topic_str, key_type="protobuf", value_type="avro", key_schema=self.protobuf_schema_str, value_schema=self.avro_schema_str)
        w.produce(self.snack_dict_list, key=self.snack_bytes_list)
        w.close()
        self.assertEqual(c.size(topic_str)[topic_str][0], 3)
        #
        group_str = self.create_test_group_name()
        # Upon consume, the types "protobuf" (alias = "pb") and "avro" trigger the conversion into a dictionary.
        r = c.openr(topic_str, group=group_str, key_type="pb", value_type="avro")
        message_dict_list = r.read(n=3)
        key_dict_list = [message_dict["key"] for message_dict in message_dict_list]
        value_dict_list = [message_dict["value"] for message_dict in message_dict_list]
        self.assertEqual(key_dict_list, self.snack_dict_list)
        self.assertEqual(value_dict_list, self.snack_dict_list)
        r.close()

    def test_produce_consume_str_jsonschema(self):
        c = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        c.create(topic_str)
        # Upon produce, the type "str" triggers the conversion of bytes, strings and dictionaries into bytes on Kafka, and "jsonschema" (alias = "json_sr") into Protobuf/Avro-encoded bytes on Kafka.
        w = c.openw(topic_str, key_type="str", value_type="jsonschema", value_schema=self.jsonschema_schema_str)
        w.produce(self.snack_dict_list, key=self.snack_str_list)
        w.close()
        self.assertEqual(c.size(topic_str)[topic_str][0], 3)
        #
        group_str = self.create_test_group_name()
        # Upon consume, the types "json" and "jsonschema" (alias = "json_sr") trigger the conversion into a dictionary.
        r = c.openr(topic_str, group=group_str, key_type="json", value_type="json_sr")
        message_dict_list = r.read(n=3)
        key_dict_list = [message_dict["key"] for message_dict in message_dict_list]
        value_dict_list = [message_dict["value"] for message_dict in message_dict_list]
        self.assertEqual(key_dict_list, self.snack_dict_list)
        self.assertEqual(value_dict_list, self.snack_dict_list)
        r.close()

    def test_consume_from_offsets(self):
        c = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        c.create(topic_str)
        w = c.openw(topic_str)
        w.produce("message 1")
        w.produce("message 2")
        w.produce("message 3")
        w.close()
        #
        group_str = self.create_test_group_name()
        r = c.openr(topic_str, group=group_str, offsets={0: 2})
        message_dict_list = r.consume()
        self.assertEqual(len(message_dict_list), 1)
        self.assertEqual(message_dict_list[0]["value"], "message 3")
        r.close()

    def test_commit(self):
        c = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        c.create(topic_str)
        w = c.openw(topic_str)
        w.produce("message 1")
        w.produce("message 2")
        w.produce("message 3")
        w.close()
        #
        group_str = self.create_test_group_name()
        r = c.openr(topic_str, group=group_str, config={"enable.auto.commit": "False"})
        r.consume()
        offsets_dict = r.offsets()
        self.assertEqual(offsets_dict[topic_str][0], OFFSET_INVALID)
        r.commit()
        offsets_dict1 = r.offsets()
        self.assertEqual(offsets_dict1[topic_str][0], 1)
        r.close()
    
    def test_cluster_settings(self):
        c = Cluster(cluster_str)
        #
        c.verbose(0)
        self.assertEqual(c.verbose(), 0)

    def test_configs(self):
        c = Cluster(cluster_str)
        #
        config_str_list1 = c.configs()
        self.assertIn("local", config_str_list1)
        config_str_list2 = c.configs("loc*")
        self.assertIn("local", config_str_list2)
        config_str_list3 = c.configs("this_pattern_shall_not_match_anything")
        self.assertEqual(config_str_list3, [])
        #
        config_str_config_dict_dict = c.configs(verbose=True)
        self.assertIn("local", config_str_config_dict_dict)
        self.assertEqual(True, config_str_config_dict_dict["local"]["kash"]["enable.auto.commit"])

    # Shell

    # Shell.cat -> Functional.map -> Functional.flatmap -> Functional.foldl -> ClusterReader.openr/KafkaReader.foldl/ClusterReader.close -> ClusterReader.consume
    def test_cat(self):
        c = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        c.create(topic_str)
        w = c.openw(topic_str)
        w.produce(self.snack_str_list)
        w.close()
        #
        group_str1 = self.create_test_group_name()
        (message_dict_list1, n_int1) = c.cat(topic_str, group=group_str1)
        self.assertEqual(3, len(message_dict_list1))
        self.assertEqual(3, n_int1)
        value_str_list1 = [message_dict["value"] for message_dict in message_dict_list1]
        self.assertEqual(value_str_list1, self.snack_str_list)
        #
        group_str2 = self.create_test_group_name()
        (message_dict_list2, n_int2) = c.cat(topic_str, group=group_str2, offsets={0:1}, n=1)
        self.assertEqual(1, len(message_dict_list2))
        self.assertEqual(1, n_int2)
        self.assertEqual(message_dict_list2[0]["value"], self.snack_str_list[1])

    # Shell.head -> Shell.cat
    def test_head(self):
        c = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        c.create(topic_str)
        w = c.openw(topic_str, value_type="avro", value_schema=self.avro_schema_str)
        w.produce(self.snack_str_list)
        w.close()
        #
        group_str1 = self.create_test_group_name()
        (message_dict_list1, n_int1) = c.head(topic_str, group=group_str1, value_type="avro", n=3)
        self.assertEqual(3, len(message_dict_list1))
        self.assertEqual(3, n_int1)
        value_dict_list1 = [message_dict["value"] for message_dict in message_dict_list1]
        self.assertEqual(value_dict_list1, self.snack_dict_list)
        #
        group_str2 = self.create_test_group_name()
        (message_dict_list2, n_int2) = c.head(topic_str, group=group_str2, offsets={0:1}, value_type="avro", n=1)
        self.assertEqual(1, len(message_dict_list2))
        self.assertEqual(1, n_int2)
        self.assertEqual(message_dict_list2[0]["value"], self.snack_dict_list[1])

    # Shell.tail -> Functional.map -> Functional.flatmap -> Functional.foldl -> ClusterReader.openr/KafkaReader.foldl/ClusterReader.close -> ClusterReader.consume
    def test_tail(self):
        c = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        c.create(topic_str)
        w = c.openw(topic_str, value_type="protobuf", value_schema=self.protobuf_schema_str)
        w.produce(self.snack_dict_list)
        w.close()
        #
        group_str1 = self.create_test_group_name()
        (message_dict_list1, n_int1) = c.tail(topic_str, group=group_str1, value_type="pb", n=3)
        self.assertEqual(3, len(message_dict_list1))
        self.assertEqual(3, n_int1)
        value_dict_list1 = [message_dict["value"] for message_dict in message_dict_list1]
        self.assertEqual(value_dict_list1, self.snack_dict_list)
        #
        group_str2 = self.create_test_group_name()
        (message_dict_list2, n_int2) = c.tail(topic_str, group=group_str2, value_type="pb", n=1)
        self.assertEqual(1, len(message_dict_list2))
        self.assertEqual(1, n_int2)
        self.assertEqual(message_dict_list2[0]["value"], self.snack_dict_list[2])

    # Shell.cp -> Functional.map_to -> Functional.flatmap_to -> ClusterReader.openw/Functional.foldl/ClusterReader.close -> ClusterReader.openr/KafkaReader.foldl/ClusterReader.close -> ClusterReader.consume
    def test_cp(self):
        c = Cluster(cluster_str)
        #
        topic_str1 = self.create_test_topic_name()
        c.create(topic_str1)
        w = c.openw(topic_str1, value_type="json_sr", value_schema=self.jsonschema_schema_str)
        w.produce(self.snack_bytes_list)
        w.close()
        topic_str2 = self.create_test_topic_name()
        #
        def map_ish(message_dict):
            message_dict["value"]["colour"] += "ish"
            return message_dict
        #
        group_str1 = self.create_test_group_name()
        (read_n_int, written_n_int) = c.cp(topic_str1, c, topic_str2, group=group_str1, source_value_type="jsonschema", target_value_type="json", write_batch_size=2, map_function=map_ish, n=3)
        self.assertEqual(3, read_n_int)
        self.assertEqual(3, written_n_int)
        #
        group_str2 = self.create_test_group_name()
        (message_dict_list2, n_int2) = c.cat(topic_str2, group=group_str2, value_type="json", n=1)
        self.assertEqual(1, len(message_dict_list2))
        self.assertEqual(1, n_int2)
        self.assertEqual(message_dict_list2[0]["value"], self.snack_ish_dict_list[0])

    def test_wc(self):
        c = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        c.create(topic_str)
        w = c.openw(topic_str, value_type="protobuf", value_schema=self.protobuf_schema_str)
        w.produce(self.snack_dict_list)
        w.close()
        #
        group_str1 = self.create_test_group_name()
        (num_messages_int, acc_num_words_int, acc_num_bytes_int) = c.wc(topic_str, group=group_str1, value_type="pb", n=2)
        self.assertEqual(2, num_messages_int)
        self.assertEqual(12, acc_num_words_int)
        self.assertEqual(110, acc_num_bytes_int)

    # Shell.diff -> Shell.diff_fun -> Functional.zipfoldl -> ClusterReader.openr/read/close
    def test_diff(self):
        c = Cluster(cluster_str)
        #
        topic_str1 = self.create_test_topic_name()
        c.create(topic_str1)
        w1 = c.openw(topic_str1, value_type="protobuf", value_schema=self.protobuf_schema_str)
        w1.produce(self.snack_str_list)
        w1.close()
        #
        topic_str2 = self.create_test_topic_name()
        c.create(topic_str2)
        w2 = c.openw(topic_str2, value_type="avro", value_schema=self.avro_schema_str)
        w2.produce(self.snack_ish_dict_list)
        w2.close()
        #
        group_str1 = self.create_test_group_name()
        group_str2 = self.create_test_group_name()
        (message_dict_message_dict_tuple_list, message_counter_int1, message_counter_int2) = c.diff(topic_str1, c, topic_str2, group1=group_str1, group2=group_str2, value_type1="pb", value_type2="avro", n=3)
        self.assertEqual(3, len(message_dict_message_dict_tuple_list))
        self.assertEqual(3, message_counter_int1)
        self.assertEqual(3, message_counter_int2)

    # Shell.diff -> Shell.diff_fun -> Functional.flatmap -> Functional.foldl -> ClusterReader.open/Kafka.foldl/ClusterReader.close -> ClusterReader.consume 
    def test_grep(self):
        c = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        c.create(topic_str)
        w = c.openw(topic_str, value_type="protobuf", value_schema=self.protobuf_schema_str)
        w.produce(self.snack_str_list)
        w.close()
        #
        group_str = self.create_test_group_name()
        (message_dict_message_dict_tuple_list, message_counter_int1, message_counter_int2) = c.grep(topic_str, ".*brown.*", group=group_str, value_type="pb", n=3)
        self.assertEqual(1, len(message_dict_message_dict_tuple_list))
        self.assertEqual(1, message_counter_int1)
        self.assertEqual(3, message_counter_int2)
    
    # Functional

    def test_foreach(self):
        c = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        c.create(topic_str)
        w = c.openw(topic_str, value_type="jsonschema", value_schema=self.jsonschema_schema_str)
        w.produce(self.snack_str_list)
        w.close()
        #
        colour_str_list = []
        c.foreach(topic_str, foreach_function=lambda message_dict: colour_str_list.append(message_dict["value"]["colour"]), value_type="jsonschema")
        self.assertEqual("brown", colour_str_list[0])
        self.assertEqual("white", colour_str_list[1])
        self.assertEqual("chocolate", colour_str_list[2])

    def test_filter(self):
        c = Cluster(cluster_str)
        #
        topic_str = self.create_test_topic_name()
        c.create(topic_str)
        w = c.openw(topic_str, value_type="avro", value_schema=self.avro_schema_str)
        w.produce(self.snack_str_list)
        w.close()
        #
        (message_dict_list, message_counter_int) = c.filter(topic_str, filter_function=lambda message_dict: message_dict["value"]["calories"] > 100, value_type="avro")
        self.assertEqual(2, len(message_dict_list))
        self.assertEqual(3, message_counter_int)

    def test_filter_to(self):
        c = Cluster(cluster_str)
        #
        topic_str1 = self.create_test_topic_name()
        c.create(topic_str1)
        w = c.openw(topic_str1, value_type="avro", value_schema=self.avro_schema_str)
        w.produce(self.snack_str_list)
        w.close()
        #
        topic_str2 = self.create_test_topic_name()
        #
        (read_n_int, written_n_int) = c.filter_to(topic_str1, c, topic_str2, filter_function=lambda message_dict: message_dict["value"]["calories"] > 100, source_value_type="avro", target_value_type="json")
        self.assertEqual(3, read_n_int)
        self.assertEqual(2, written_n_int)
        #
        group_str = self.create_test_group_name()
        (message_dict_list, n_int) = c.cat(topic_str2, group=group_str, value_type="json", n=2)
        self.assertEqual(2, len(message_dict_list))
        self.assertEqual(2, n_int)
        self.assertEqual(500.0, message_dict_list[0]["value"]["calories"])
        self.assertEqual(260.0, message_dict_list[1]["value"]["calories"])
