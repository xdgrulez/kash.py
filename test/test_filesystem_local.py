import os
import sys
import tempfile
import unittest
import warnings
sys.path.insert(1, ".")
from kashpy.filesystem.local.local import *
from kashpy.helpers import *

config_str = "local"

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
        self.headers_str_bytes_tuple_list = [("header_field1", b"header_value1"), ("header_field2", b"header_value2")]
        self.headers_str_bytes_dict = {"header_field1": b"header_value1", "header_field2": b"header_value2"}
        self.headers_str_str_tuple_list = [("header_field1", "header_value1"), ("header_field2", "header_value2")]
        self.headers_str_str_dict = {"header_field1": "header_value1", "header_field2": "header_value2"}
        #
        self.file_str_list = []
        #
        self.path_str = f"{tempfile.gettempdir()}/kash.py/test/local"
        os.makedirs(self.path_str, exist_ok=True)
        #
        print("Test:", self._testMethodName)

    def tearDown(self):
        l = self.get_local()
        for file_str in self.file_str_list:
            l.delete(file_str)

    def create_test_file_name(self):
        while True:
            file_str = f"test_file_{get_millis()}"
            #
            if file_str not in self.file_str_list:
                self.file_str_list.append(file_str)
                break
        #
        return file_str

    def get_local(self):
        l = Local(config_str)
        l.root_dir(self.path_str)
        return l

    ### LocalAdmin

    def test_create_delete(self):
        l = self.get_local()
        #
        file_str = self.create_test_file_name()
        w = l.openw(file_str, overwrite=True)
        w.close()
        file_str_list = l.ls()
        self.assertIn(file_str, file_str_list)
        l.rm(file_str)
        file_str_list = l.ls()
        self.assertNotIn(file_str, file_str_list)

    def test_files(self):
        l = self.get_local()
        #
        file_str = self.create_test_file_name()
        old_file_str_list = l.ls(["test_*"])
        self.assertNotIn(file_str, old_file_str_list)
        l.touch(file_str)
        new_file_str_list = l.ls(["test_*"])
        self.assertIn(file_str, new_file_str_list)
        #
        w = l.openw(file_str, overwrite=False)
        w.write("message 1")
        w.write("message 2")
        w.write("message 3")
        w.close()
        #
        file_str_total_size_int_dict_l = l.l(pattern=file_str)
        file_str_total_size_int_dict_ll = l.ll(pattern=file_str)
        self.assertEqual(file_str_total_size_int_dict_l, file_str_total_size_int_dict_ll)
        total_size_int = file_str_total_size_int_dict_l[file_str]
        self.assertEqual(total_size_int, 3)
        file_str_size_int_filesize_int_tuple_dict = l.files(pattern=file_str, size=True, filesize=True)
        self.assertEqual(file_str_size_int_filesize_int_tuple_dict[file_str][0], 3)
        self.assertEqual(file_str_size_int_filesize_int_tuple_dict[file_str][1], 243)
        file_str_filesize_int_dict = l.l(pattern=file_str, size=False, filesize=True)
        self.assertEqual(file_str_filesize_int_dict[file_str], 243)

    def test_exists(self):
        l = self.get_local()
        #
        file_str = self.create_test_file_name()
        self.assertFalse(l.exists(file_str))
        l.create(file_str)
        self.assertTrue(l.exists(file_str))

    # Write/Read

    def test_write_read_bytes_str(self):
        l = self.get_local()
        #
        file_str = self.create_test_file_name()
        print(file_str)
        l.create(file_str)
        # Upon write, the type "bytes" triggers conversion into bytes, "str" into a string.
        w = l.openw(file_str, key_type="bytes", value_type="str")
        w.write(self.snack_str_list, key=self.snack_str_list, headers=self.headers_str_bytes_tuple_list)
        w.close()
        self.assertEqual(l.l(file_str)[file_str], 3)
        #
        # Upon read, the type "str" triggers conversion into a string, "bytes" into bytes.
        r = l.openr(file_str, key_type="str", value_type="bytes")
        message_dict_list = r.read(n=3)
        key_str_list = [message_dict["key"] for message_dict in message_dict_list]
        value_bytes_list = [message_dict["value"] for message_dict in message_dict_list]
        headers_list = [message_dict["headers"] for message_dict in message_dict_list]
        self.assertEqual(key_str_list, self.snack_str_list)
        self.assertEqual(value_bytes_list, self.snack_bytes_list)
        self.assertEqual(headers_list[0], self.headers_str_bytes_tuple_list)
        r.close()
    
    def test_write_read_str_json(self):
        l = self.get_local()
        #
        file_str = self.create_test_file_name()
        l.create(file_str)
        # Upon write, the type "str" triggers conversion into a string, "json" into a dictionary.
        w = l.openw(file_str, key_type="str", value_type="json")
        w.write(self.snack_dict_list, key=self.snack_str_list, headers=self.headers_str_bytes_dict)
        w.close()
        self.assertEqual(l.l(file_str, filesize=True)[file_str][0], 3)
        #
        # Upon read, the type "json" triggers the conversion into a dictionary, and "str" into a string.
        r = l.openr(file_str, key_type="json", value_type="str")
        message_dict_list = r.read(n=3)
        key_dict_list = [message_dict["key"] for message_dict in message_dict_list]
        value_str_list = [message_dict["value"] for message_dict in message_dict_list]
        headers_list = [message_dict["headers"] for message_dict in message_dict_list]
        self.assertEqual(key_dict_list, self.snack_dict_list)
        self.assertEqual(value_str_list, self.snack_str_list)
        self.assertEqual(headers_list[1], self.headers_str_bytes_tuple_list)
        r.close()

    def test_configs(self):
        l = self.get_local()
        #
        config_str_list1 = l.configs()
        self.assertIn("local", config_str_list1)
        config_str_list2 = l.configs("loc*")
        self.assertIn("local", config_str_list2)
        config_str_list3 = l.configs("this_pattern_shall_not_match_anything")
        self.assertEqual(config_str_list3, [])
        #
        config_str_config_dict_dict = l.configs(verbose=True)
        self.assertIn("local", config_str_config_dict_dict)
        self.assertEqual(".", config_str_config_dict_dict["local"]["local"]["root.dir"])

    # Shell

    # Shell.cat -> Functional.map -> Functional.flatmap -> Functional.foldl -> ClusterReader.openr/KafkaReader.foldl/ClusterReader.close -> ClusterReader.consume
    def test_cat(self):
        c = Local(config_str)
        #
        file_str = self.create_test_file_name()
        c.create(file_str)
        w = c.openw(file_str)
        w.produce(self.snack_str_list)
        w.close()
        #
        group_str1 = self.create_test_group_name()
        (message_dict_list1, n_int1) = c.cat(file_str, group=group_str1)
        self.assertEqual(3, len(message_dict_list1))
        self.assertEqual(3, n_int1)
        value_str_list1 = [message_dict["value"] for message_dict in message_dict_list1]
        self.assertEqual(value_str_list1, self.snack_str_list)
        #
        group_str2 = self.create_test_group_name()
        (message_dict_list2, n_int2) = c.cat(file_str, group=group_str2, offsets={0:1}, n=1)
        self.assertEqual(1, len(message_dict_list2))
        self.assertEqual(1, n_int2)
        self.assertEqual(message_dict_list2[0]["value"], self.snack_str_list[1])

    # Shell.head -> Shell.cat
    def test_head(self):
        c = Local(config_str)
        #
        file_str = self.create_test_file_name()
        c.create(file_str)
        w = c.openw(file_str, value_type="avro", value_schema=self.avro_schema_str)
        w.produce(self.snack_str_list)
        w.close()
        #
        group_str1 = self.create_test_group_name()
        (message_dict_list1, n_int1) = c.head(file_str, group=group_str1, value_type="avro", n=3)
        self.assertEqual(3, len(message_dict_list1))
        self.assertEqual(3, n_int1)
        value_dict_list1 = [message_dict["value"] for message_dict in message_dict_list1]
        self.assertEqual(value_dict_list1, self.snack_dict_list)
        #
        group_str2 = self.create_test_group_name()
        (message_dict_list2, n_int2) = c.head(file_str, group=group_str2, offsets={0:1}, value_type="avro", n=1)
        self.assertEqual(1, len(message_dict_list2))
        self.assertEqual(1, n_int2)
        self.assertEqual(message_dict_list2[0]["value"], self.snack_dict_list[1])

    # Shell.tail -> Functional.map -> Functional.flatmap -> Functional.foldl -> ClusterReader.openr/KafkaReader.foldl/ClusterReader.close -> ClusterReader.consume
    def test_tail(self):
        c = Local(config_str)
        #
        file_str = self.create_test_file_name()
        c.create(file_str)
        w = c.openw(file_str, value_type="protobuf", value_schema=self.protobuf_schema_str)
        w.produce(self.snack_dict_list)
        w.close()
        #
        group_str1 = self.create_test_group_name()
        (message_dict_list1, n_int1) = c.tail(file_str, group=group_str1, value_type="pb", n=3)
        self.assertEqual(3, len(message_dict_list1))
        self.assertEqual(3, n_int1)
        value_dict_list1 = [message_dict["value"] for message_dict in message_dict_list1]
        self.assertEqual(value_dict_list1, self.snack_dict_list)
        #
        group_str2 = self.create_test_group_name()
        (message_dict_list2, n_int2) = c.tail(file_str, group=group_str2, value_type="pb", n=1)
        self.assertEqual(1, len(message_dict_list2))
        self.assertEqual(1, n_int2)
        self.assertEqual(message_dict_list2[0]["value"], self.snack_dict_list[2])

    # Shell.cp -> Functional.map_to -> Functional.flatmap_to -> ClusterReader.openw/Functional.foldl/ClusterReader.close -> ClusterReader.openr/KafkaReader.foldl/ClusterReader.close -> ClusterReader.consume
    def test_cp(self):
        c = Local(config_str)
        #
        file_str1 = self.create_test_file_name()
        c.create(file_str1)
        w = c.openw(file_str1, value_type="json_sr", value_schema=self.jsonschema_schema_str)
        w.produce(self.snack_bytes_list)
        w.close()
        file_str2 = self.create_test_file_name()
        #
        def map_ish(message_dict):
            message_dict["value"]["colour"] += "ish"
            return message_dict
        #
        group_str1 = self.create_test_group_name()
        (read_n_int, written_n_int) = c.cp(file_str1, c, file_str2, group=group_str1, source_value_type="jsonschema", target_value_type="json", write_batch_size=2, map_function=map_ish, n=3)
        self.assertEqual(3, read_n_int)
        self.assertEqual(3, written_n_int)
        #
        group_str2 = self.create_test_group_name()
        (message_dict_list2, n_int2) = c.cat(file_str2, group=group_str2, value_type="json", n=1)
        self.assertEqual(1, len(message_dict_list2))
        self.assertEqual(1, n_int2)
        self.assertEqual(message_dict_list2[0]["value"], self.snack_ish_dict_list[0])

    def test_wc(self):
        c = Local(config_str)
        #
        file_str = self.create_test_file_name()
        c.create(file_str)
        w = c.openw(file_str, value_type="protobuf", value_schema=self.protobuf_schema_str)
        w.produce(self.snack_dict_list)
        w.close()
        #
        group_str1 = self.create_test_group_name()
        (num_messages_int, acc_num_words_int, acc_num_bytes_int) = c.wc(file_str, group=group_str1, value_type="pb", n=2)
        self.assertEqual(2, num_messages_int)
        self.assertEqual(12, acc_num_words_int)
        self.assertEqual(110, acc_num_bytes_int)

    # Shell.diff -> Shell.diff_fun -> Functional.zipfoldl -> ClusterReader.openr/read/close
    def test_diff(self):
        c = Local(config_str)
        #
        file_str1 = self.create_test_file_name()
        c.create(file_str1)
        w1 = c.openw(file_str1, value_type="protobuf", value_schema=self.protobuf_schema_str)
        w1.produce(self.snack_str_list)
        w1.close()
        #
        file_str2 = self.create_test_file_name()
        c.create(file_str2)
        w2 = c.openw(file_str2, value_type="avro", value_schema=self.avro_schema_str)
        w2.produce(self.snack_ish_dict_list)
        w2.close()
        #
        group_str1 = self.create_test_group_name()
        group_str2 = self.create_test_group_name()
        (message_dict_message_dict_tuple_list, message_counter_int1, message_counter_int2) = c.diff(file_str1, c, file_str2, group1=group_str1, group2=group_str2, value_type1="pb", value_type2="avro", n=3)
        self.assertEqual(3, len(message_dict_message_dict_tuple_list))
        self.assertEqual(3, message_counter_int1)
        self.assertEqual(3, message_counter_int2)

    # Shell.diff -> Shell.diff_fun -> Functional.flatmap -> Functional.foldl -> ClusterReader.open/Kafka.foldl/ClusterReader.close -> ClusterReader.consume 
    def test_grep(self):
        c = Local(config_str)
        #
        file_str = self.create_test_file_name()
        c.create(file_str)
        w = c.openw(file_str, value_type="protobuf", value_schema=self.protobuf_schema_str)
        w.produce(self.snack_str_list)
        w.close()
        #
        group_str = self.create_test_group_name()
        (message_dict_message_dict_tuple_list, message_counter_int1, message_counter_int2) = c.grep(file_str, ".*brown.*", group=group_str, value_type="pb", n=3)
        self.assertEqual(1, len(message_dict_message_dict_tuple_list))
        self.assertEqual(1, message_counter_int1)
        self.assertEqual(3, message_counter_int2)
    
    # Functional

    def test_foreach(self):
        c = Local(config_str)
        #
        file_str = self.create_test_file_name()
        c.create(file_str)
        w = c.openw(file_str, value_type="jsonschema", value_schema=self.jsonschema_schema_str)
        w.produce(self.snack_str_list)
        w.close()
        #
        colour_str_list = []
        c.foreach(file_str, foreach_function=lambda message_dict: colour_str_list.append(message_dict["value"]["colour"]), value_type="jsonschema")
        self.assertEqual("brown", colour_str_list[0])
        self.assertEqual("white", colour_str_list[1])
        self.assertEqual("chocolate", colour_str_list[2])

    def test_filter(self):
        c = Local(config_str)
        #
        file_str = self.create_test_file_name()
        c.create(file_str)
        w = c.openw(file_str, value_type="avro", value_schema=self.avro_schema_str)
        w.produce(self.snack_str_list)
        w.close()
        #
        (message_dict_list, message_counter_int) = c.filter(file_str, filter_function=lambda message_dict: message_dict["value"]["calories"] > 100, value_type="avro")
        self.assertEqual(2, len(message_dict_list))
        self.assertEqual(3, message_counter_int)

    def test_filter_to(self):
        c = Local(config_str)
        #
        file_str1 = self.create_test_file_name()
        c.create(file_str1)
        w = c.openw(file_str1, value_type="avro", value_schema=self.avro_schema_str)
        w.produce(self.snack_str_list)
        w.close()
        #
        file_str2 = self.create_test_file_name()
        #
        (read_n_int, written_n_int) = c.filter_to(file_str1, c, file_str2, filter_function=lambda message_dict: message_dict["value"]["calories"] > 100, source_value_type="avro", target_value_type="json")
        self.assertEqual(3, read_n_int)
        self.assertEqual(2, written_n_int)
        #
        group_str = self.create_test_group_name()
        (message_dict_list, n_int) = c.cat(file_str2, group=group_str, value_type="json", n=2)
        self.assertEqual(2, len(message_dict_list))
        self.assertEqual(2, n_int)
        self.assertEqual(500.0, message_dict_list[0]["value"]["calories"])
        self.assertEqual(260.0, message_dict_list[1]["value"]["calories"])
