import unittest
from kash import *

class TestAdminClient(unittest.TestCase):
    def runTest(self):
        cluster = Cluster("local")
        topic_str = f"test_topic_{get_millis()}"
        cluster.create(topic_str, operation_timeout=1.0)
        topic_str_list = cluster.ls()
        self.assertIn("test_topic", topic_str_list)
        cluster.delete(topic_str, operation_timeout=1.0)

unittest.main()
