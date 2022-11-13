import filecmp
import os
import sys
import time
import unittest
import warnings
sys.path.insert(1, "..")
from kashpy.kash import *

#cluster_str = "rp-dev"
#cluster_str_without_kash = "rp-dev_without_kash"
#principal_str = "User:admin"
cluster_str = "local"
cluster_str_without_kash = "local_without_kash"
principal_str = None
#cluster_str = "ccloud"
#cluster_str_without_kash = "ccloud_without_kash"
#principal_str = "User:admin"


def count_lines(path_str):
    def count_generator(reader):
        bytes = reader(1024 * 1024)
        while bytes:
            yield bytes
            bytes = reader(1024 * 1024)
    #
    with open(path_str, "rb") as bufferedReader:
        c_generator = count_generator(bufferedReader.raw.read)
        # count each \n
        count_int = sum(buffer.count(b'\n') for buffer in c_generator)
    #
    return count_int


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
        with open("./snacks_value_no_newline.txt", "w") as textIOWrapper:
            textIOWrapper.writelines(['{"name": "cookie", "calories": 500.0, "colour": "brown"}\n', '{"name": "cake", "calories": 260.0, "colour": "white"}\n', '{"name": "timtam", "calories": 80.0, "colour": "chocolate"}'])
        with open("./snacks_key_value.txt", "w") as textIOWrapper:
            textIOWrapper.writelines(['{"name": "cookie_key", "calories": 500.0, "colour": "brown"}/{"name": "cookie_value", "calories": 500.0, "colour": "brown"}\n', '{"name": "cake_key", "calories": 260.0, "colour": "white"}/{"name": "cake_value", "calories": 260.0, "colour": "white"}\n', '{"name": "timtam_key", "calories": 80.0, "colour": "chocolate"}/{"name": "timtam_value", "calories": 80.0, "colour": "chocolate"}\n'])

    def tearDown(self):
        if self.old_home_str:
            os.environ["KASHPY_HOME"] = self.old_home_str
        #
        os.remove("./snacks_value.txt")
        os.remove("./snacks_value_no_newline.txt")
        os.remove("./snacks_key_value.txt")

    def test_create(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        topic_str_list = cluster.ls()
        self.assertIn(topic_str, topic_str_list)
        cluster.delete(topic_str)

    def test_topics(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        old_topic_str_list = cluster.topics(["test_*"])
        self.assertNotIn(topic_str, old_topic_str_list)
        cluster.create(topic_str)
        new_topic_str_list = cluster.ls(["test_*"])
        self.assertIn(topic_str, new_topic_str_list)
        cluster.produce(topic_str, "message 1", on_delivery=lambda kafkaError, message: print(kafkaError, message))
        cluster.produce(topic_str, "message 2")
        cluster.produce(topic_str, "message 3")
        cluster.flush()
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
        cluster.delete(topic_str)

    def test_config(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        cluster.set_config(topic_str, "retention.ms", 4711)
        new_retention_ms_str = cluster.config(topic_str)[topic_str]["retention.ms"]
        self.assertEqual(new_retention_ms_str, "4711")
        cluster.rm(topic_str)

    def test_describe(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        topic_dict = cluster.describe(topic_str)[topic_str]
        self.assertEqual(topic_dict["topic"], topic_str)
        self.assertEqual(topic_dict["partitions"][0]["id"], 0)
        cluster.delete(topic_str)

    def test_exists(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        self.assertFalse(cluster.exists(topic_str))
        cluster.create(topic_str)
        self.assertTrue(cluster.exists(topic_str))
        cluster.delete(topic_str)

    def test_partitions(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        num_partitions_int_1 = cluster.partitions(topic_str)[topic_str]
        self.assertEqual(num_partitions_int_1, 1)
        cluster.set_partitions(topic_str, 2)
        num_partitions_int_2 = cluster.partitions(topic_str)[topic_str]
        self.assertEqual(num_partitions_int_2, 2)
        cluster.delete(topic_str)

    def test_groups(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        cluster.produce(topic_str, "message 1")
        cluster.produce(topic_str, "message 2")
        cluster.produce(topic_str, "message 3")
        cluster.flush()
        group_str = create_test_group_name()
        cluster.subscribe(topic_str, group_str)
        cluster.consume()
        group_str_list = cluster.groups(["test*", "test_group*"])
        self.assertIn(group_str, group_str_list)
        group_str_list = cluster.groups("test_group*")
        self.assertIn(group_str, group_str_list)
        group_dict = cluster.describe_groups(group_str)[group_str]
        self.assertEqual(group_dict["id"], group_str)
        cluster.delete(topic_str)
    
    def test_brokers(self):
        cluster = Cluster(cluster_str)
        if "confluent.cloud" not in cluster.config_dict["bootstrap.servers"]:
            broker_dict = cluster.brokers()
            broker_dict1 = cluster.brokers("0")
            self.assertEqual(broker_dict, broker_dict1)
            broker_dict2 = cluster.brokers([0])
            self.assertEqual(broker_dict1, broker_dict2)
            broker_int = list(broker_dict.keys())[0]
            old_background_threads_str = cluster.broker_config(broker_int)[broker_int]["background.threads"]
            cluster.set_broker_config(broker_int, "background.threads", 5)
            new_background_threads_str = cluster.broker_config(broker_int)[broker_int]["background.threads"]
            self.assertEqual(new_background_threads_str, "5")
            cluster.set_broker_config(broker_int, "background.threads", old_background_threads_str)

    def test_acls(self):
        if principal_str:
            cluster = Cluster(cluster_str)
            topic_str = create_test_topic_name()
            cluster.create(topic_str)
            cluster.create_acl(restype="topic", name=topic_str, resource_pattern_type="literal", principal=principal_str, host="*", operation="read", permission_type="allow")
            acl_dict_list = cluster.acls()
            self.assertIn({"restype": "topic", "name": topic_str, "resource_pattern_type": "literal", 'principal': principal_str, 'host': '*', 'operation': 'read', 'permission_type': 'allow'}, acl_dict_list)
            cluster.delete_acl(restype="topic", name=topic_str, resource_pattern_type="literal", principal=principal_str, host="*", operation="read", permission_type="allow")
            self.assertIn({"restype": "topic", "name": topic_str, "resource_pattern_type": "literal", 'principal': principal_str, 'host': '*', 'operation': 'read', 'permission_type': 'allow'}, acl_dict_list)
            cluster.delete(topic_str)
    
    def test_produce_consume_bytes(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        cluster.cp("./snacks_value.txt", topic_str, target_value_type="str")
        self.assertEqual(cluster.size(topic_str)[topic_str][0], 3)
        cluster.cp(topic_str, "./snacks_value1.txt", source_value_type="str", batch_size=3, n=3)
        self.assertTrue(filecmp.cmp("./snacks_value.txt", "./snacks_value1.txt"))
        os.remove("./snacks_value1.txt")
        cluster.delete(topic_str)
        #
        topic_str_key_value = create_test_topic_name()
        cluster.create(topic_str_key_value)
        cluster.cp("./snacks_key_value.txt", topic_str_key_value, target_key_type="str", target_value_type="str", key_value_separator="/")
        self.assertEqual(cluster.size(topic_str_key_value)[topic_str_key_value][0], 3)
        cluster.cp(topic_str_key_value, "./snacks_key_value1.txt", source_key_type="str", source_value_type="str", key_value_separator="/", n=3)
        self.assertTrue(filecmp.cmp("./snacks_key_value.txt", "./snacks_key_value1.txt"))
        os.remove("./snacks_key_value1.txt")
        cluster.delete(topic_str_key_value)

    def test_produce_consume_string(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        cluster.cp("./snacks_value_no_newline.txt", topic_str, target_value_type="str", bufsize=150)
        self.assertEqual(cluster.size(topic_str)[topic_str][0], 3)
        cluster.cp(topic_str, "./snacks_value1.txt", source_value_type="str", n=3)
        self.assertTrue(filecmp.cmp("./snacks_value.txt", "./snacks_value1.txt"))
        os.remove("./snacks_value1.txt")
        cluster.delete(topic_str)
        #
        topic_str_key_value = create_test_topic_name()
        cluster.create(topic_str_key_value)
        cluster.cp("./snacks_key_value.txt", topic_str_key_value, target_key_type="str", target_value_type="str", key_value_separator="/")
        self.assertEqual(cluster.size(topic_str_key_value)[topic_str_key_value][0], 3)
        cluster.cp(topic_str_key_value, "./snacks_key_value1.txt", source_key_type="str", source_value_type="str", key_value_separator="/", n=3)
        self.assertTrue(filecmp.cmp("./snacks_key_value.txt", "./snacks_key_value1.txt"))
        os.remove("./snacks_key_value1.txt")
        cluster.delete(topic_str_key_value)
    
    def test_produce_consume_json(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        cluster.cp("./snacks_value.txt", topic_str, target_value_type="json")
        self.assertEqual(cluster.size(topic_str)[topic_str][0], 3)
        cluster.cp(topic_str, "./snacks_value1.txt", source_value_type="json", n=3)
        self.assertTrue(filecmp.cmp("./snacks_value.txt", "./snacks_value1.txt"))
        os.remove("./snacks_value1.txt")
        cluster.delete(topic_str)
        #
        topic_str_key_value = create_test_topic_name()
        cluster.create(topic_str_key_value)
        cluster.cp("./snacks_key_value.txt", topic_str_key_value, target_key_type="json", target_value_type="json", key_value_separator="/")
        self.assertEqual(cluster.size(topic_str_key_value)[topic_str_key_value][0], 3)
        cluster.cp(topic_str_key_value, "./snacks_key_value1.txt", source_key_type="json", source_value_type="json", key_value_separator="/", n=3)
        self.assertTrue(filecmp.cmp("./snacks_key_value.txt", "./snacks_key_value1.txt"))
        os.remove("./snacks_key_value1.txt")
        cluster.delete(topic_str_key_value)

    def test_produce_consume_protobuf(self):
        schema_str = 'message Snack { required string name = 1; required float calories = 2; optional string colour = 3; }'
        #
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        cluster.cp("./snacks_value.txt", topic_str, target_value_type="pb", target_value_schema=schema_str)
        self.assertEqual(cluster.size(topic_str)[topic_str][0], 3)
        cluster.cp(topic_str, "./snacks_value1.txt", source_value_type="pb", n=3)
        self.assertTrue(filecmp.cmp("./snacks_value.txt", "./snacks_value1.txt"))
        os.remove("./snacks_value1.txt")
        cluster.delete(topic_str)
        #
        topic_str_key_value = create_test_topic_name()
        cluster.create(topic_str_key_value)
        cluster.cp("./snacks_key_value.txt", topic_str_key_value, target_key_type="pb", target_value_type="pb", target_key_schema=schema_str, target_value_schema=schema_str, key_value_separator="/")
        self.assertEqual(cluster.size(topic_str_key_value)[topic_str_key_value][0], 3)
        cluster.cp(topic_str_key_value, "./snacks_key_value1.txt", source_key_type="pb", source_value_type="pb", key_value_separator="/", n=3)
        self.assertTrue(filecmp.cmp("./snacks_key_value.txt", "./snacks_key_value1.txt"))
        os.remove("./snacks_key_value1.txt")
        #
        cluster.create(f"{topic_str_key_value}_1")
        cp(cluster, topic_str_key_value, cluster, f"{topic_str_key_value}_1", keep_timestamps=False)
        cluster.cp(f"{topic_str_key_value}_1", "./snacks_key_value2.txt", source_key_type="pb", source_value_type="pb", key_value_separator="/", n=3)
        self.assertTrue(filecmp.cmp("./snacks_key_value.txt", "./snacks_key_value2.txt"))
        os.remove("./snacks_key_value2.txt")
        cluster.delete(topic_str_key_value)
        cluster.delete(f"{topic_str_key_value}_1")

    def test_produce_consume_avro(self):
        schema_str = '{ "type": "record", "name": "myrecord", "fields": [{"name": "name",  "type": "string" }, {"name": "calories", "type": "float" }, {"name": "colour", "type": "string" }] }'
        #
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        (num_lines_int, num_messages_int) = cluster.cp("./snacks_value.txt", topic_str, target_value_type="avro", target_value_schema=schema_str)
        self.assertEqual(num_lines_int, 3)
        self.assertEqual(num_messages_int, 3)
        self.assertEqual(cluster.size(topic_str)[topic_str][0], 3)
        (message_counter_int, line_counter_int) = cluster.cp(topic_str, "./snacks_value1.txt", source_value_type="avro", n=3)
        self.assertEqual(message_counter_int, 3)
        self.assertEqual(line_counter_int, 3)
        self.assertTrue(filecmp.cmp("./snacks_value.txt", "./snacks_value1.txt"))
        os.remove("./snacks_value1.txt")
        cluster.delete(topic_str)
        #
        topic_str_key_value = create_test_topic_name()
        cluster.create(topic_str_key_value)
        cluster.cp("./snacks_key_value.txt", topic_str_key_value, target_key_type="avro", target_value_type="avro", target_key_schema=schema_str, target_value_schema=schema_str, key_value_separator="/")
        self.assertEqual(cluster.size(topic_str_key_value)[topic_str_key_value][0], 3)
        cluster.cp(topic_str_key_value, "./snacks_key_value1.txt", source_key_type="avro", source_value_type="avro", key_value_separator="/", n=3)
        self.assertTrue(filecmp.cmp("./snacks_key_value.txt", "./snacks_key_value1.txt"))
        os.remove("./snacks_key_value1.txt")
        #
        cluster.create(f"{topic_str_key_value}_1")
        (consumed_message_counter_int, produced_message_counter_int) = cp(cluster, topic_str_key_value, cluster, f"{topic_str_key_value}_1")
        self.assertEqual(consumed_message_counter_int, 3)
        self.assertEqual(produced_message_counter_int, 3)
        cluster.cp(f"{topic_str_key_value}_1", "./snacks_key_value2.txt", source_key_type="avro", source_value_type="avro", key_value_separator="/", n=3)
        self.assertTrue(filecmp.cmp("./snacks_key_value.txt", "./snacks_key_value2.txt"))
        os.remove("./snacks_key_value2.txt")
        cluster.delete(topic_str_key_value)
        cluster.delete(f"{topic_str_key_value}_1")

    def test_produce_consume_jsonschema(self):
        schema_str = '{ "title": "abc", "definitions" : { "record:myrecord" : { "type" : "object", "required" : [ "name", "calories" ], "additionalProperties" : false, "properties" : { "name" : {"type" : "string"}, "calories" : {"type" : "number"}, "colour" : {"type" : "string"} } } }, "$ref" : "#/definitions/record:myrecord" }'
        #
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        cluster.cp("./snacks_value.txt", topic_str, target_value_type="jsonschema", target_value_schema=schema_str)
        self.assertEqual(cluster.size(topic_str)[topic_str][0], 3)
        cluster.cp(topic_str, "./snacks_value1.txt", source_value_type="jsonschema", n=3)
        self.assertTrue(filecmp.cmp("./snacks_value.txt", "./snacks_value1.txt"))
        os.remove("./snacks_value1.txt")
        cluster.delete(topic_str)
        #
        topic_str_key_value = create_test_topic_name()
        cluster.create(topic_str_key_value)
        cluster.cp("./snacks_key_value.txt", topic_str_key_value, target_key_type="jsonschema", target_value_type="jsonschema", target_key_schema=schema_str, target_value_schema=schema_str, key_value_separator="/")
        self.assertEqual(cluster.size(topic_str_key_value)[topic_str_key_value][0], 3)
        cluster.cp(topic_str_key_value, "./snacks_key_value1.txt", source_key_type="jsonschema", source_value_type="jsonschema", key_value_separator="/", n=3)
        self.assertTrue(filecmp.cmp("./snacks_key_value.txt", "./snacks_key_value1.txt"))
        os.remove("./snacks_key_value1.txt")
        cluster.delete(topic_str_key_value)

    def test_offsets(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
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

    def test_offsets_for_times(self):
        cluster = Cluster(cluster_str)
        cluster.verbose(1)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        cluster.produce(topic_str, "message 1")
        time.sleep(1)
        cluster.produce(topic_str, "message 2")
        cluster.flush()
        self.assertEqual(cluster.l(topic_str, partitions=True)[topic_str][1][0], 2)
        #
        cluster.subscribe(topic_str)
        message_dict_list = cluster.consume(n=2)
        message1_timestamp_int = message_dict_list[1]["timestamp"][1]
        message1_offset_int = message_dict_list[1]["offset"]
        #
        topic_str_partition_int_offset_int_dict_dict = cluster.offsets_for_times(topic_str, {0: message1_timestamp_int})
        found_message1_offset_int = topic_str_partition_int_offset_int_dict_dict[topic_str][0]
        self.assertEqual(message1_offset_int, found_message1_offset_int)
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
    
    def test_recreate(self):
        cluster = Cluster(cluster_str_without_kash)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        cluster.cp("./snacks_value.txt", topic_str, target_value_type="str")
        self.assertEqual(cluster.size(topic_str)[topic_str][0], 3)
        #
        for i in range(3):
            print(i)
            (num_consumed_messages_int1, num_produced_messages_int1), (num_consumed_messages_int2, num_produced_messages_int2) = cluster.recreate(topic_str)
            self.assertEqual(num_consumed_messages_int1, 3)
            self.assertEqual(num_produced_messages_int1, 3)
            self.assertEqual(num_consumed_messages_int2, 3)
            self.assertEqual(num_produced_messages_int2, 3)
            #
            cluster.cp(topic_str, "./snacks_value1.txt", source_value_type="str", batch_size=3, n=3)
            self.assertTrue(filecmp.cmp("./snacks_value.txt", "./snacks_value1.txt"))
            #
            os.remove("./snacks_value1.txt")
        #
        cluster.delete(topic_str)

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
