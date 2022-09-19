import filecmp
import os
import sys
import time
import unittest
import warnings
sys.path.insert(1, "..")
from kash import *

#cluster_str = "rp-dev"
#principal_str = "User:admin"
cluster_str = "local"
principal_str = None

def create_test_topic_name():
    return f"test_topic_{get_millis()}"


def create_test_group_name():
    return f"test_group_{get_millis()}"


class Test(unittest.TestCase):
    def setUp(self):
        warnings.filterwarnings(action="ignore", message="unclosed", category=ResourceWarning)
        #
        self.old_home_str = os.environ.get("KASHPY_HOME")
        os.environ["KASHPY_HOME"] = ".."
        # https://simon-aubury.medium.com/kafka-with-avro-vs-kafka-with-protobuf-vs-kafka-with-json-schema-667494cbb2af
        with open("./snacks_value.txt", "w") as textIOWrapper:
            textIOWrapper.writelines(['{"name": "cookie", "calories": 500.0, "colour": "brown"}\n', '{"name": "cake", "calories": 260.0, "colour": "white"}\n', '{"name": "timtam", "calories": 80.0, "colour": "chocolate"}\n'])
        with open("./snacks_key_value.txt", "w") as textIOWrapper:
            textIOWrapper.writelines(['{"name": "cookie_key", "calories": 500.0, "colour": "brown"}/{"name": "cookie_value", "calories": 500.0, "colour": "brown"}\n', '{"name": "cake_key", "calories": 260.0, "colour": "white"}/{"name": "cake_value", "calories": 260.0, "colour": "white"}\n', '{"name": "timtam_key", "calories": 80.0, "colour": "chocolate"}/{"name": "timtam_value", "calories": 80.0, "colour": "chocolate"}\n'])

    def tearDown(self):
        if self.old_home_str:
            os.environ["KASHPY_HOME"] = self.old_home_str
        #
        os.remove("./snacks_value.txt")
        os.remove("./snacks_key_value.txt")

    def test_create(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        time.sleep(1)
        topic_str_list = cluster.ls()
        self.assertIn(topic_str, topic_str_list)
        cluster.delete(topic_str)
        time.sleep(1)

    def test_topics(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        old_topic_str_list = cluster.topics()
        self.assertNotIn(topic_str, old_topic_str_list)
        cluster.create(topic_str)
        time.sleep(1)
        new_topic_str_list = cluster.ls()
        self.assertIn(topic_str, new_topic_str_list)
        cluster.produce(topic_str, "message 1")
        cluster.produce(topic_str, "message 2")
        cluster.produce(topic_str, "message 3")
        cluster.flush()
        topic_str_size_int_dict_l = cluster.l(pattern=topic_str)
        topic_str_size_int_dict_ll = cluster.ll(pattern=topic_str)
        self.assertEqual(topic_str_size_int_dict_l, topic_str_size_int_dict_ll)
        size_int = topic_str_size_int_dict_l[topic_str]
        self.assertEqual(size_int, 3)
        cluster.delete(topic_str)
        time.sleep(1)

    def test_config(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.mk(topic_str)
        time.sleep(1)
        cluster.set_config(topic_str, "retention.ms", 4711)
        new_retention_ms_str = cluster.config(topic_str)[topic_str]["retention.ms"]
        self.assertEqual(new_retention_ms_str, "4711")
        cluster.rm(topic_str)
        time.sleep(1)

    def test_describe(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        time.sleep(1)
        topic_dict = cluster.describe(topic_str)[topic_str]
        self.assertEqual(topic_dict["topic"], topic_str)
        self.assertEqual(topic_dict["partitions"][0]["id"], 0)
        cluster.delete(topic_str)
        time.sleep(1)

    def test_exists(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        self.assertFalse(cluster.exists(topic_str))
        cluster.create(topic_str)
        time.sleep(1)
        self.assertTrue(cluster.exists(topic_str))
        cluster.delete(topic_str)
        time.sleep(1)

    def test_partitions(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        time.sleep(1)
        num_partitions_int_1 = cluster.partitions(topic_str)[topic_str]
        self.assertEqual(num_partitions_int_1, 1)
        cluster.set_partitions(topic_str, 2)
        time.sleep(1)
        num_partitions_int_2 = cluster.partitions(topic_str)[topic_str]
        self.assertEqual(num_partitions_int_2, 2)
        cluster.delete(topic_str)
        time.sleep(1)

    def test_groups(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        time.sleep(1)
        cluster.produce(topic_str, "message 1")
        cluster.produce(topic_str, "message 2")
        cluster.produce(topic_str, "message 3")
        cluster.flush()
        group_str = create_test_group_name()
        cluster.subscribe(topic_str, group_str)
        cluster.consume()
        time.sleep(1)
        group_str_list = cluster.groups()
        self.assertIn(group_str, group_str_list)
        group_dict = cluster.describe_groups(group_str)[group_str]
        self.assertEqual(group_dict["id"], group_str)
        cluster.delete(topic_str)
        time.sleep(1)
    
    def test_brokers(self):
        cluster = Cluster(cluster_str)
        broker_dict = cluster.brokers()
        broker_int = list(broker_dict.keys())[0]
        old_log_retention_ms_str = cluster.broker_config(broker_int)[broker_int]["log.retention.ms"]
        cluster.set_broker_config(broker_int, "log.retention.ms", 4711)
        time.sleep(1)
        new_log_retention_ms_str = cluster.broker_config(broker_int)[broker_int]["log.retention.ms"]
        self.assertEqual(new_log_retention_ms_str, "4711")
        cluster.set_broker_config(broker_int, "log.retention.ms", old_log_retention_ms_str)        

    def test_acls(self):
        if principal_str:
            cluster = Cluster(cluster_str)
            topic_str = create_test_topic_name()
            cluster.create(topic_str)
            time.sleep(1)
            cluster.create_acl(restype="topic", name=topic_str, resource_pattern_type="literal", principal=principal_str, host="*", operation="read", permission_type="allow")
            time.sleep(1)
            acl_dict_list = cluster.acls()
            self.assertIn({"restype": "topic", "name": topic_str, "resource_pattern_type": "literal", 'principal': principal_str, 'host': '*', 'operation': 'read', 'permission_type': 'allow'}, acl_dict_list)
            cluster.delete_acl(restype="topic", name=topic_str, resource_pattern_type="literal", principal=principal_str, host="*", operation="read", permission_type="allow")
            time.sleep(1)
            self.assertIn({"restype": "topic", "name": topic_str, "resource_pattern_type": "literal", 'principal': principal_str, 'host': '*', 'operation': 'read', 'permission_type': 'allow'}, acl_dict_list)
            cluster.delete(topic_str)
            time.sleep(1)

    def test_produce_consume_bytes(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        time.sleep(1)
        cluster.cp("./snacks_value.txt", topic_str, target_value_type="str")
        self.assertEqual(cluster.size(topic_str)[topic_str][1], 3)
        cluster.cp(topic_str, "./snacks_value1.txt", source_value_type="str", batch_size=3)
        self.assertTrue(filecmp.cmp("./snacks_value.txt", "./snacks_value1.txt"))
        os.remove("./snacks_value1.txt")
        cluster.delete(topic_str)
        time.sleep(1)
        #
        topic_str_key_value = create_test_topic_name()
        cluster.create(topic_str_key_value)
        time.sleep(1)
        cluster.cp("./snacks_key_value.txt", topic_str_key_value, target_key_type="str", target_value_type="str", key_value_separator="/")
        self.assertEqual(cluster.size(topic_str_key_value)[topic_str_key_value][1], 3)
        cluster.cp(topic_str_key_value, "./snacks_key_value1.txt", source_key_type="str", source_value_type="str", key_value_separator="/")
        self.assertTrue(filecmp.cmp("./snacks_key_value.txt", "./snacks_key_value1.txt"))
        os.remove("./snacks_key_value1.txt")
        cluster.delete(topic_str_key_value)
        time.sleep(1)

    def test_produce_consume_string(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        time.sleep(1)
        cluster.cp("./snacks_value.txt", topic_str, target_value_type="str")
        self.assertEqual(cluster.size(topic_str)[topic_str][1], 3)
        cluster.cp(topic_str, "./snacks_value1.txt", source_value_type="str")
        self.assertTrue(filecmp.cmp("./snacks_value.txt", "./snacks_value1.txt"))
        os.remove("./snacks_value1.txt")
        cluster.delete(topic_str)
        time.sleep(1)
        #
        topic_str_key_value = create_test_topic_name()
        cluster.create(topic_str_key_value)
        time.sleep(1)
        cluster.cp("./snacks_key_value.txt", topic_str_key_value, target_key_type="str", target_value_type="str", key_value_separator="/")
        self.assertEqual(cluster.size(topic_str_key_value)[topic_str_key_value][1], 3)
        cluster.cp(topic_str_key_value, "./snacks_key_value1.txt", source_key_type="str", source_value_type="str", key_value_separator="/")
        self.assertTrue(filecmp.cmp("./snacks_key_value.txt", "./snacks_key_value1.txt"))
        os.remove("./snacks_key_value1.txt")
        cluster.delete(topic_str_key_value)
        time.sleep(1)
    
    def test_produce_consume_json(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        time.sleep(1)
        cluster.cp("./snacks_value.txt", topic_str, target_value_type="json")
        self.assertEqual(cluster.size(topic_str)[topic_str][1], 3)
        cluster.cp(topic_str, "./snacks_value1.txt", source_value_type="json")
        self.assertTrue(filecmp.cmp("./snacks_value.txt", "./snacks_value1.txt"))
        os.remove("./snacks_value1.txt")
        cluster.delete(topic_str)
        time.sleep(1)
        #
        topic_str_key_value = create_test_topic_name()
        cluster.create(topic_str_key_value)
        time.sleep(1)
        cluster.cp("./snacks_key_value.txt", topic_str_key_value, target_key_type="json", target_value_type="json", key_value_separator="/")
        self.assertEqual(cluster.size(topic_str_key_value)[topic_str_key_value][1], 3)
        cluster.cp(topic_str_key_value, "./snacks_key_value1.txt", source_key_type="json", source_value_type="json", key_value_separator="/")
        self.assertTrue(filecmp.cmp("./snacks_key_value.txt", "./snacks_key_value1.txt"))
        os.remove("./snacks_key_value1.txt")
        cluster.delete(topic_str_key_value)
        time.sleep(1)

    def test_produce_consume_protobuf(self):
        schema_str = 'message Snack { required string name = 1; required float calories = 2; optional string colour = 3; }'
        #
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        time.sleep(1)
        cluster.cp("./snacks_value.txt", topic_str, target_value_type="pb", target_value_schema=schema_str)
        self.assertEqual(cluster.size(topic_str)[topic_str][1], 3)
        cluster.cp(topic_str, "./snacks_value1.txt", source_value_type="pb")
        self.assertTrue(filecmp.cmp("./snacks_value.txt", "./snacks_value1.txt"))
        os.remove("./snacks_value1.txt")
        cluster.delete(topic_str)
        time.sleep(1)
        #
        topic_str_key_value = create_test_topic_name()
        cluster.create(topic_str_key_value)
        time.sleep(1)
        cluster.cp("./snacks_key_value.txt", topic_str_key_value, target_key_type="pb", target_value_type="pb", target_key_schema=schema_str, target_value_schema=schema_str, key_value_separator="/")
        self.assertEqual(cluster.size(topic_str_key_value)[topic_str_key_value][1], 3)
        cluster.cp(topic_str_key_value, "./snacks_key_value1.txt", source_key_type="pb", source_value_type="pb", key_value_separator="/")
        self.assertTrue(filecmp.cmp("./snacks_key_value.txt", "./snacks_key_value1.txt"))
        os.remove("./snacks_key_value1.txt")
        #
        cluster.create(f"{topic_str_key_value}_1")
        cp(cluster, topic_str_key_value, cluster, f"{topic_str_key_value}_1", transform=lambda x: x, keep_timestamps=False)
        cluster.cp(f"{topic_str_key_value}_1", "./snacks_key_value2.txt", source_key_type="pb", source_value_type="pb", key_value_separator="/")
        self.assertTrue(filecmp.cmp("./snacks_key_value.txt", "./snacks_key_value2.txt"))
        os.remove("./snacks_key_value2.txt")
        cluster.delete(topic_str_key_value)
        cluster.delete(f"{topic_str_key_value}_1")
        time.sleep(1)

    def test_produce_consume_avro(self):
        schema_str = '{ "type": "record", "name": "myrecord", "fields": [{"name": "name",  "type": "string" }, {"name": "calories", "type": "float" }, {"name": "colour", "type": "string" }] }'
        #
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        time.sleep(1)
        num_messages_int = cluster.cp("./snacks_value.txt", topic_str, target_value_type="avro", target_value_schema=schema_str)
        self.assertEqual(num_messages_int, 3)
        self.assertEqual(cluster.size(topic_str)[topic_str][1], 3)
        num_messages_int = cluster.cp(topic_str, "./snacks_value1.txt", source_value_type="avro")
        self.assertEqual(num_messages_int, 3)
        self.assertTrue(filecmp.cmp("./snacks_value.txt", "./snacks_value1.txt"))
        os.remove("./snacks_value1.txt")
        cluster.delete(topic_str)
        time.sleep(1)
        #
        topic_str_key_value = create_test_topic_name()
        cluster.create(topic_str_key_value)
        time.sleep(1)
        cluster.cp("./snacks_key_value.txt", topic_str_key_value, target_key_type="avro", target_value_type="avro", target_key_schema=schema_str, target_value_schema=schema_str, key_value_separator="/")
        self.assertEqual(cluster.size(topic_str_key_value)[topic_str_key_value][1], 3)
        cluster.cp(topic_str_key_value, "./snacks_key_value1.txt", source_key_type="avro", source_value_type="avro", key_value_separator="/")
        self.assertTrue(filecmp.cmp("./snacks_key_value.txt", "./snacks_key_value1.txt"))
        os.remove("./snacks_key_value1.txt")
        #
        cluster.create(f"{topic_str_key_value}_1")
        message_counter_int = cp(cluster, topic_str_key_value, cluster, f"{topic_str_key_value}_1")
        self.assertEqual(message_counter_int, 3)
        cluster.cp(f"{topic_str_key_value}_1", "./snacks_key_value2.txt", source_key_type="avro", source_value_type="avro", key_value_separator="/")
        self.assertTrue(filecmp.cmp("./snacks_key_value.txt", "./snacks_key_value2.txt"))
        os.remove("./snacks_key_value2.txt")
        cluster.delete(topic_str_key_value)
        cluster.delete(f"{topic_str_key_value}_1")
        time.sleep(1)

    def test_produce_consume_jsonschema(self):
        schema_str = '{ "title": "abc", "definitions" : { "record:myrecord" : { "type" : "object", "required" : [ "name", "calories" ], "additionalProperties" : false, "properties" : { "name" : {"type" : "string"}, "calories" : {"type" : "number"}, "colour" : {"type" : "string"} } } }, "$ref" : "#/definitions/record:myrecord" }'
        #
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        time.sleep(1)
        cluster.cp("./snacks_value.txt", topic_str, target_value_type="jsonschema", target_value_schema=schema_str)
        self.assertEqual(cluster.size(topic_str)[topic_str][1], 3)
        cluster.cp(topic_str, "./snacks_value1.txt", source_value_type="jsonschema")
        self.assertTrue(filecmp.cmp("./snacks_value.txt", "./snacks_value1.txt"))
        os.remove("./snacks_value1.txt")
        cluster.delete(topic_str)
        time.sleep(1)
        #
        topic_str_key_value = create_test_topic_name()
        cluster.create(topic_str_key_value)
        time.sleep(1)
        cluster.cp("./snacks_key_value.txt", topic_str_key_value, target_key_type="jsonschema", target_value_type="jsonschema", target_key_schema=schema_str, target_value_schema=schema_str, key_value_separator="/")
        self.assertEqual(cluster.size(topic_str_key_value)[topic_str_key_value][1], 3)
        cluster.cp(topic_str_key_value, "./snacks_key_value1.txt", source_key_type="jsonschema", source_value_type="jsonschema", key_value_separator="/")
        self.assertTrue(filecmp.cmp("./snacks_key_value.txt", "./snacks_key_value1.txt"))
        os.remove("./snacks_key_value1.txt")
        cluster.delete(topic_str_key_value)
        time.sleep(1)

    def test_offsets(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        time.sleep(1)
        cluster.produce(topic_str, "message 1")
        cluster.produce(topic_str, "message 2")
        cluster.produce(topic_str, "message 3")
        cluster.flush()
        cluster.cat(topic_str)
        cluster.subscribe(topic_str, offsets={0: 2})
        message_dict_list = cluster.consume()
        self.assertEqual(len(message_dict_list), 1)
        self.assertEqual(message_dict_list[0]["value"], "message 3")
        cluster.delete(topic_str)
        time.sleep(1)

    def test_commit(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        time.sleep(1)
        cluster.produce(topic_str, "message 1")
        cluster.produce(topic_str, "message 2")
        cluster.produce(topic_str, "message 3")
        cluster.flush()
        cluster.cat(topic_str, n=3)
        cluster.subscribe(topic_str, config={"enable.auto.commit": False})
        cluster.consume()
        time.sleep(1)
        offsets_dict = cluster.offsets()
        self.assertEqual(offsets_dict[0], "OFFSET_INVALID")
        cluster.commit()
        time.sleep(1)
        offsets_dict1 = cluster.offsets()
        self.assertEqual(offsets_dict1[0], 1)
        cluster.delete(topic_str)
        time.sleep(1)

    def test_errors(self):
        cluster = Cluster(cluster_str)
        cluster.cp("./abc", "./abc")
        cluster.consume("abc")
    
    def test_cluster_settings(self):
        cluster = Cluster(cluster_str)
        cluster.set_flush_timeout(1.5)
        self.assertEqual(cluster.flush_timeout(), 1.5)
        cluster.set_flush_num_messages(10001)
        self.assertEqual(cluster.flush_num_messages(), 10001)
        cluster.set_auto_offset_reset("latest")
        self.assertEqual(cluster.auto_offset_reset(), "latest")
        cluster.set_auto_commit(False)
        self.assertEqual(cluster.auto_commit(), False)
        cluster.set_session_timeout_ms(6000)
        self.assertEqual(cluster.session_timeout_ms(), 6000)
        cluster.set_verbose(0)
        self.assertEqual(cluster.verbose(), 0)
        cluster.set_progress_num_messages(1001)
        self.assertEqual(cluster.progress_num_messages(), 1001)

    def test_transforms_string(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        time.sleep(1)
        cluster.cp("./snacks_value.txt", topic_str, target_value_type="str")
        self.assertEqual(cluster.size(topic_str)[topic_str][1], 3)
        #
        def transform(message_dict):
            value_dict = json.loads(message_dict["value"])
            value_dict["colour"] += "ish"
            value_str = json.dumps(value_dict)
            message_dict["value"] = value_str
            return message_dict
        cluster.create(f"{topic_str}_1")
        cp(cluster, topic_str, cluster, f"{topic_str}_1", source_value_type="str", transform=transform)
        #
        cluster.subscribe(f"{topic_str}_1", value_type="str")
        message_dict_list = cluster.consume(n=3)
        for message_dict in message_dict_list:
            value_dict = json.loads(message_dict["value"])            
            self.assertRegex(value_dict["colour"], ".*ish")
        #
        cluster.delete(topic_str)
        cluster.delete(f"{topic_str}_1")
        time.sleep(1)

    def test_transforms_json(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        time.sleep(1)
        cluster.cp("./snacks_value.txt", topic_str, target_value_type="json")
        self.assertEqual(cluster.size(topic_str)[topic_str][1], 3)
        #
        def transform(message_dict):
            message_dict["value"]["colour"] += "ish"
            return message_dict
        cluster.create(f"{topic_str}_1")
        cp(cluster, topic_str, cluster, f"{topic_str}_1", source_value_type="json", transform=transform)
        #
        cluster.subscribe(f"{topic_str}_1", value_type="json")
        message_dict_list = cluster.consume(n=3)
        for message_dict in message_dict_list:
            self.assertRegex(message_dict["value"]["colour"], ".*ish")
        #
        cluster.delete(topic_str)
        cluster.delete(f"{topic_str}_1")
        time.sleep(1)

    def test_transforms_protobuf(self):
        schema_str = 'message Snack { required string name = 1; required float calories = 2; optional string colour = 3; }'
        #
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        time.sleep(1)
        cluster.cp("./snacks_key_value.txt", topic_str, target_key_type="pb", target_value_type="pb", target_key_schema=schema_str, target_value_schema=schema_str, key_value_separator="/")
        self.assertEqual(cluster.size(topic_str)[topic_str][1], 3)
        #
        def transform(message_dict):
            message_dict["key"]["colour"] += "ishy"
            message_dict["value"]["colour"] += "ish"
            return message_dict
        cluster.create(f"{topic_str}_1")
        cp(cluster, topic_str, cluster, f"{topic_str}_1", source_key_type="pb", source_value_type="pb", transform=transform)
        #
        cluster.subscribe(f"{topic_str}_1", key_type="pb", value_type="pb")
        message_dict_list = cluster.consume(n=3)
        for message_dict in message_dict_list:
            self.assertRegex(message_dict["key"]["colour"], ".*ishy")
            self.assertRegex(message_dict["value"]["colour"], ".*ish")
        #
        cluster.delete(topic_str)
        cluster.delete(f"{topic_str}_1")
        time.sleep(1)

    def test_transforms_avro(self):
        schema_str = '{ "type": "record", "name": "myrecord", "fields": [{"name": "name",  "type": "string" }, {"name": "calories", "type": "float" }, {"name": "colour", "type": "string" }] }'
        #
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        time.sleep(1)
        cluster.cp("./snacks_value.txt", topic_str, target_value_type="avro", target_value_schema=schema_str)
        self.assertEqual(cluster.size(topic_str)[topic_str][1], 3)
        #
        def transform(message_dict):
            message_dict["value"]["colour"] += "ish"
            return message_dict
        cluster.create(f"{topic_str}_1")
        cp(cluster, topic_str, cluster, f"{topic_str}_1", source_value_type="avro", transform=transform)
        #
        cluster.subscribe(f"{topic_str}_1", value_type="avro")
        message_dict_list = cluster.consume(n=3)
        for message_dict in message_dict_list:
            self.assertRegex(message_dict["value"]["colour"], ".*ish")
        #
        cluster.delete(topic_str)
        cluster.delete(f"{topic_str}_1")
        time.sleep(1)

    def test_transforms_jsonschema(self):
        schema_str = '{ "title": "abc", "definitions" : { "record:myrecord" : { "type" : "object", "required" : [ "name", "calories" ], "additionalProperties" : false, "properties" : { "name" : {"type" : "string"}, "calories" : {"type" : "number"}, "colour" : {"type" : "string"} } } }, "$ref" : "#/definitions/record:myrecord" }'
        #
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        time.sleep(1)
        cluster.cp("./snacks_value.txt", topic_str, target_value_type="jsonschema", target_value_schema=schema_str)
        self.assertEqual(cluster.size(topic_str)[topic_str][1], 3)
        #
        def transform(message_dict):
            message_dict["value"]["colour"] += "ish"
            return message_dict
        cluster.create(f"{topic_str}_1")
        cp(cluster, topic_str, cluster, f"{topic_str}_1", source_value_type="jsonschema", transform=transform)
        #
        cluster.subscribe(f"{topic_str}_1", value_type="jsonschema")
        message_dict_list = cluster.consume(n=3)
        for message_dict in message_dict_list:
            self.assertRegex(message_dict["value"]["colour"], ".*ish")
        #
        cluster.delete(topic_str)
        cluster.delete(f"{topic_str}_1")
        time.sleep(1)

    def test_transforms_bytes(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        time.sleep(1)
        cluster.cp("./snacks_value.txt", topic_str, target_value_type="str")
        self.assertEqual(cluster.size(topic_str)[topic_str][1], 3)
        #
        def transform(message_dict):
            value_bytearray = bytearray(message_dict["value"])
            value_bytearray[10] = ord("X")
            message_dict["value"] = bytes(value_bytearray)
            return message_dict
        cluster.create(f"{topic_str}_1")
        cp(cluster, topic_str, cluster, f"{topic_str}_1", source_value_type="bytes", transform=transform)
        #
        cluster.subscribe(f"{topic_str}_1", value_type="bytes")
        message_dict_list = cluster.consume(n=3)
        for message_dict in message_dict_list:            
            value_bytes = message_dict["value"]
            self.assertEqual(value_bytes[10], ord("X"))     
        #
        cluster.delete(topic_str)
        cluster.delete(f"{topic_str}_1")
        time.sleep(1)

    def test_grep(self):
        schema_str = '{ "type": "record", "name": "myrecord", "fields": [{"name": "name",  "type": "string" }, {"name": "calories", "type": "float" }, {"name": "colour", "type": "string" }] }'
        #
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        time.sleep(1)
        cluster.cp("./snacks_value.txt", topic_str, target_value_type="avro", target_value_schema=schema_str)
        self.assertEqual(cluster.size(topic_str)[topic_str][1], 3)
        #
        message_dict_list = cluster.grep(topic_str, lambda message_dict: message_dict["value"]["name"] == "cake", value_type="avro")
        self.assertEqual(len(message_dict_list), 1)
        #
        message_dict_list = cluster.grep(topic_str, lambda message_dict: message_dict["value"]["name"] == "cake", value_type="avro", offsets={0: 2})
        self.assertEqual(len(message_dict_list), 0)
        #
        cluster.delete(topic_str)
        time.sleep(1)

    def test_offsets_for_times(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        time.sleep(1)
        cluster.produce(topic_str, "message 1")
        time.sleep(1)
        cluster.produce(topic_str, "message 2")
        cluster.flush()
        self.assertEqual(cluster.size(topic_str)[topic_str][1], 2)
        #
        cluster.subscribe(topic_str)
        message_dict_list = cluster.consume(n=2)
        message1_timestamp_int = message_dict_list[1]["timestamp"][1]
        message1_offset_int = message_dict_list[1]["offset"]
        #
        partition_int_offset_int_dict = cluster.offsets_for_times(topic_str, {0: message1_timestamp_int})
        found_message1_offset_int = partition_int_offset_int_dict[0]
        self.assertEqual(message1_offset_int, found_message1_offset_int)
        #
        cluster.delete(topic_str)
        time.sleep(1)
    
    def test_replicate_change_schema(self):
        avro_schema_str = '{ "type": "record", "name": "myrecord", "fields": [{"name": "name",  "type": "string" }, {"name": "calories", "type": "float" }, {"name": "colour", "type": "string" }] }'
        #
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        time.sleep(1)
        cluster.cp("./snacks_value.txt", topic_str, target_value_type="avro", target_value_schema=avro_schema_str)
        self.assertEqual(cluster.size(topic_str)[topic_str][1], 3)
        #
        protobuf_schema_str = 'message Snack { required string name = 1; required float calories = 2; optional string colour = 3; }'
        cluster.create(f"{topic_str}_1")
        cluster.cp(topic_str, f"{topic_str}_1", source_value_type="avro", target_value_type="pb", target_value_schema=protobuf_schema_str)
        #
        cluster.cp(f"{topic_str}_1", "./snacks_value1.txt", source_value_type="pb")
        self.assertTrue(filecmp.cmp("./snacks_value.txt", "./snacks_value1.txt"))
        os.remove("./snacks_value1.txt")
        #
        cluster.delete(topic_str)
        cluster.delete(f"{topic_str}_1")
        time.sleep(1)

    def test_flush_timeout_bug(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        time.sleep(1)
        #
        for i in range(3):
            print(i)
            cluster.cp("./snacks_value.txt", topic_str, target_value_type="str")
            self.assertEqual(cluster.size(topic_str)[topic_str][1], 3)
            cluster.cp(topic_str, "./snacks_value1.txt", source_value_type="str")
            cluster.cp("./snacks_value1.txt", f"{topic_str}_1", target_value_type="str")
            self.assertEqual(cluster.size(f"{topic_str}_1")[f"{topic_str}_1"][1], 3)
            #
            cluster.delete(topic_str)
            cluster.delete(f"{topic_str}_1")
            time.sleep(1)
