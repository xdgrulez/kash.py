from enum import auto
import os
import sys
import time
import unittest
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

class TestAdminClient(unittest.TestCase):
    def setUp(self):
        self.old_home_str = os.environ.get("KASHPY_HOME")
        os.environ["KASHPY_HOME"] = ".."

    def tearDown(self):
        if self.old_home_str:
            os.environ["KASHPY_HOME"] = self.old_home_str

    def test_create(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        time.sleep(1)
        topic_str_list = cluster.ls()
        self.assertIn(topic_str, topic_str_list)
        cluster.delete(topic_str)

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
        topic_str_size_int_dict_l = cluster.l(topic=topic_str)
        topic_str_size_int_dict_ll = cluster.ll(topic=topic_str)
        self.assertEqual(topic_str_size_int_dict_l, topic_str_size_int_dict_ll)
        size_int = topic_str_size_int_dict_l[topic_str]
        self.assertEqual(size_int, 3)
        cluster.delete(topic_str)

    def test_config(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.mk(topic_str)
        time.sleep(1)
        cluster.set_config(topic_str, "retention.ms", 4711)
        new_retention_ms_str = cluster.config(topic_str)["retention.ms"]
        self.assertEqual(new_retention_ms_str, "4711")
        cluster.rm(topic_str)

    def test_describe(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        time.sleep(1)
        topic_dict = cluster.describe(topic_str)
        self.assertEqual(topic_dict["topic"], topic_str)
        self.assertEqual(topic_dict["partitions"][0]["id"], 0)
        cluster.delete(topic_str)

    def test_exists(self):
        cluster = Cluster(cluster_str)
        broker_dict = cluster.brokers()
        broker_int = list(broker_dict.keys())[0]
        auto_create_topics_enable_str = cluster.broker_config(broker_int)["auto.create.topics.enable"]
        topic_str = create_test_topic_name()
        if auto_create_topics_enable_str == "true":
            self.assertTrue(cluster.exists(topic_str))
        else:
            self.assertFalse(cluster.exists(topic_str))

    def test_partitions(self):
        cluster = Cluster(cluster_str)
        topic_str = create_test_topic_name()
        cluster.create(topic_str)
        time.sleep(1)
        num_partitions_int_1 = cluster.partitions(topic_str)
        self.assertEqual(num_partitions_int_1, 1)
        cluster.set_partitions(topic_str, 2)
        time.sleep(1)
        num_partitions_int_2 = cluster.partitions(topic_str)
        self.assertEqual(num_partitions_int_2, 2)
        cluster.delete(topic_str)

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
        group_dict = cluster.describe_group(group_str)
        self.assertEqual(group_dict["id"], group_str)
        cluster.delete(topic_str)
    
    def test_brokers(self):
        cluster = Cluster(cluster_str)
        broker_dict = cluster.brokers()
        broker_int = list(broker_dict.keys())[0]
        old_log_retention_ms_str = cluster.broker_config(broker_int)["log.retention.ms"]
        cluster.set_broker_config(broker_int, "log.retention.ms", 4711)
        time.sleep(1)
        new_log_retention_ms_str = cluster.broker_config(broker_int)["log.retention.ms"]
        self.assertEqual(new_log_retention_ms_str, "4711")
        cluster.set_broker_config(broker_int, "log.retention.ms", old_log_retention_ms_str)        

    def test_acls(self):
        if principal_str:
            cluster = Cluster(cluster_str)
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
