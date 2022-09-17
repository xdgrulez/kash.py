import time
import unittest
from kash import *

class TestAdminClient(unittest.TestCase):
    def testCreate(self):
        cluster = Cluster("local")
        topic_str = f"test_topic_{get_millis()}"
        cluster.create(topic_str)
        time.sleep(1)
        topic_str_list = cluster.ls()
        self.assertIn(topic_str, topic_str_list)
        cluster.delete(topic_str)
        time.sleep(1)

unittest.main()
