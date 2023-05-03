from kashpy.storage import Storage

from fnmatch import fnmatch

class Kafka(Storage):
    def __init__(self):
        pass

    def size(self, pattern_str_or_str_list, timeout=-1.0):
        """List topics, their total sizes and the sizes of their partitions.

        List topics on the cluster whose names match the pattern pattern_str, their total sizes and the sizes of their partitions.

        Args:
            pattern_str_or_str_list (:obj:`str` | :obj:`list(str)`): The pattern or a list of patterns for selecting those topics which shall be listed.
            timeout (:obj:`float`, optional): The timeout (in seconds) for the internally used get_watermark_offsets() method calls from confluent_kafka.Consumer. Defaults to -1.0 (infinite=no timeout).

        Returns:
            :obj:`dict(str, tuple(int, dict(int, int)))`: Dictionary of strings (topic name) and pairs of integers (total size of the topic) and dictionaries of integers (partition) and integers (size of the partition).

        Examples:
            List all topics of the cluster, their total sizes and the sizes of their partitions::

                c.size("\*")

            List those topics whose name end with "test" or whose name is "bla", their total sizes and the sizes of their partitions and time out the internally used get_watermark_offsets() method after one second::

                c.size(["\*test", "bla"], timeout=1.0)
        """
        topic_str_partition_int_tuple_dict_dict = self.watermarks(pattern_str_or_str_list, timeout=timeout)
        #
        topic_str_total_size_int_size_dict_tuple_dict = {}
        for topic_str, partition_int_tuple_dict in topic_str_partition_int_tuple_dict_dict.items():
            size_dict = {partition_int: partition_int_tuple_dict[partition_int][1]-partition_int_tuple_dict[partition_int][0] for partition_int in partition_int_tuple_dict.keys()}
            #
            total_size_int = 0
            for offset_int_tuple in partition_int_tuple_dict.values():
                partition_size_int = offset_int_tuple[1] - offset_int_tuple[0]
                total_size_int += partition_size_int
            #
            topic_str_total_size_int_size_dict_tuple_dict[topic_str] = (total_size_int, size_dict)
        return topic_str_total_size_int_size_dict_tuple_dict

    def topics(self, pattern=None, size=False, partitions=False):
        """List topics on the cluster.

        List topics on the cluster. Optionally return only those topics whose names match bash-like patterns. Optionally return the total sizes of the topics and the sizes of their individual partitions.

        Args:
            pattern (:obj:`str` | :obj:`list(str)`, optional): The pattern or list of patterns for selecting those topics which shall be listed. Defaults to None.
            size (:obj:`bool`, optional): Return the total sizes of the topics if set to True. Defaults to False.
            partitions (:obj:`bool`, optional): Return the sizes of the individual partitions of the topics if set to True. Defaults to False.

        Returns:
            :obj:`list(str)` | :obj:`dict(str, int)` | :obj:`dict(str, dict(int, int))` | :obj:`dict(str, tuple(int, dict(int, int)))`: List of strings if size=False and partitions=False; dictionary of strings (topic name) and integers (total size of the topic) if size=True and partitions=False; dictionary of strings (topic name) and dictionaries of integers (partition) and integers (size of the partition) if size=False and partitions=True; dictionary of strings (topic name) and pairs of integers (total size of the topic) and dictionaries of integers (partition) and integers (size of the partition) if size=True and partitions=True.

        Examples:
            List all topics of the cluster::

                c.topics()

            List all the topics of the cluster and their total sizes::

                c.topics(size=True)

            List all the topics of the cluster and the sizes of their individual partitions::

                c.topics(partitions=True)

            List all the topics of the cluster, their total sizes and the sizes of their individual partitions::

                c.topics(size=True, partitions=True)

            List all those topics of the cluster whose name starts with "test"::

                c.topics("test*")

            List all those topics of the cluster whose name starts with "test" or "bla"::

                c.topics(["test*", "bla*"])
        """
        pattern_str_or_str_list = pattern
        size_bool = size
        partitions_bool = partitions
        #
        if size_bool:
            topic_str_total_size_int_size_dict_tuple_dict = self.size(pattern_str_or_str_list)
            if partitions_bool:
                return topic_str_total_size_int_size_dict_tuple_dict
            else:
                topic_str_size_int_dict = {topic_str: topic_str_total_size_int_size_dict_tuple_dict[topic_str][0] for topic_str in topic_str_total_size_int_size_dict_tuple_dict}
                return topic_str_size_int_dict
        else:
            if partitions_bool:
                topic_str_total_size_int_size_dict_tuple_dict = self.size(pattern_str_or_str_list)
                topic_str_size_dict_dict = {topic_str: topic_str_total_size_int_size_dict_tuple_dict[topic_str][1] for topic_str in topic_str_total_size_int_size_dict_tuple_dict}
                return topic_str_size_dict_dict
            else:
                topic_str_list = self.list_topics()
                if pattern_str_or_str_list is not None:
                    if isinstance(pattern_str_or_str_list, str):
                        pattern_str_or_str_list = [pattern_str_or_str_list]
                    topic_str_list = [topic_str for topic_str in topic_str_list if any(fnmatch(topic_str, pattern_str) for pattern_str in pattern_str_or_str_list)]
                topic_str_list.sort()
                return topic_str_list

    ls = topics
    """List topics on the cluster (shortcut for topics()).

    List topics on the cluster. Optionally return only those topics whose names match bash-like patterns. Optionally return the total sizes of the topics and the sizes of their individual partitions.

    Args:
        pattern (:obj:`str`, optional): The pattern or list of patterns for selecting those topics which shall be listed. Defaults to None.
        size (:obj:`bool`, optional): Return the total sizes of the topics if set to True. Defaults to False.
        partitions (:obj:`bool`, optional): Return the sizes of the individual partitions of the topics if set to True. Defaults to False.

    Returns:
        :obj:`list(str)` | :obj:`dict(str, int)` | :obj:`dict(str, dict(int, int))` | :obj:`dict(str, tuple(int, dict(int, int)))`: List of strings if size=False and partitions=False; dictionary of strings (topic name) and integers (total size of the topic) if size=True and partitions=False; dictionary of strings (topic name) and dictionaries of integers (partition) and integers (size of the partition) if size=False and partitions=True; dictionary of strings (topic name) and pairs of integers (total size of the topic) and dictionaries of integers (partition) and integers (size of the partition) if size=True and partitions=True.

    Examples:
        List all topics of the cluster::

            c.ls()

        List all the topics of the cluster and their total sizes::

            c.ls(size=True)

        List all the topics of the cluster and the sizes of their individual partitions::

            c.ls(partitions=True)

        List all the topics of the cluster, their total sizes and the sizes of their individual partitions::

            c.ls(size=True, partitions=True)

        List all those topics of the cluster whose name starts with "test"::

            c.ls("test*")

        List all those topics of the cluster whose name starts with "test" or "bla"::

            c.ls(["test*", "bla*"])
    """

    def l(self, pattern=None, size=True, partitions=False):
        """List topics on the cluster (shortcut for topics(size=True), a la the "l" alias in bash).

        List topics on the cluster. Optionally return only those topics whose names match bash-like patterns. Optionally return the total sizes of the topics and the sizes of their individual partitions.

        Args:
            pattern (:obj:`str`, optional): The pattern or list of patterns for selecting those topics which shall be listed. Defaults to None.
            size (:obj:`bool`, optional): Return the total sizes of the topics if set to True. Defaults to True.
            partitions (:obj:`bool`, optional): Return the sizes of the individual partitions of the topics if set to True. Defaults to False.

        Returns:
            :obj:`list(str)` | :obj:`dict(str, int)` | :obj:`dict(str, dict(int, int))` | :obj:`dict(str, tuple(int, dict(int, int)))`: List of strings if size=False and partitions=False; dictionary of strings (topic name) and integers (total size of the topic) if size=True and partitions=False; dictionary of strings (topic name) and dictionaries of integers (partition) and integers (size of the partition) if size=False and partitions=True; dictionary of strings (topic name) and pairs of integers (total size of the topic) and dictionaries of integers (partition) and integers (size of the partition) if size=True and partitions=True.

        Examples:
            List all the topics of the cluster and their total sizes::

                c.l()

            List all the topics of the cluster::

                c.l(size=False)

            List all the topics of the cluster, their total sizes and the sizes of their individual partitions::

                c.l(partitions=True)

            List all the topics of the cluster and the sizes of their individual partitions::

                c.l(size=False, partitions=True)

            List all those topics of the cluster whose name starts with "test" and their total sizes::

                c.l("test*")

            List all those topics of the cluster whose name starts with "test" or "bla"::

                c.l(["test*", "bla*"])
        """
        return self.topics(pattern=pattern, size=size, partitions=partitions)

    ll = l
    """List topics on the cluster (shortcut for topics(size=True), a la the "ll" alias in bash).

    List topics on the cluster. Optionally return only those topics whose names match bash-like patterns. Optionally return the total sizes of the topics and the sizes of their individual partitions.

    Args:
        pattern (:obj:`str`, optional): The pattern or list of patterns for selecting those topics which shall be listed. Defaults to None.
        size (:obj:`bool`, optional): Return the total sizes of the topics if set to True. Defaults to True.
        partitions (:obj:`bool`, optional): Return the sizes of the individual partitions of the topics if set to True. Defaults to False.

    Returns:
        :obj:`list(str)` | :obj:`dict(str, int)` | :obj:`dict(str, dict(int, int))` | :obj:`dict(str, tuple(int, dict(int, int)))`: List of strings if size=False and partitions=False; dictionary of strings (topic name) and integers (total size of the topic) if size=True and partitions=False; dictionary of strings (topic name) and dictionaries of integers (partition) and integers (size of the partition) if size=False and partitions=True; dictionary of strings (topic name) and pairs of integers (total size of the topic) and dictionaries of integers (partition) and integers (size of the partition) if size=True and partitions=True.

    Examples:
        List all the topics of the cluster and their total sizes::

            c.ll()

        List all the topics of the cluster::

            c.ll(size=False)

        List all the topics of the cluster, their total sizes and the sizes of their individual partitions::

            c.ll(partitions=True)

        List all the topics of the cluster and the sizes of their individual partitions::

            c.ll(size=False, partitions=True)

        List all those topics of the cluster whose name starts with "test" and their total sizes::

            c.ll("test*")

        List all those topics of the cluster whose name starts with "test" or "bla" and their total sizes::

            c.ll(["test*", "bla*"])
    """

    def exists(self, topic_str):
        """Test whether a topic exists on the cluster.

        Test whether a topic exists on the cluster.

        Args:
            topic_str (:obj:`str`): A topic.

        Returns:
            :obj:`bool`: True if the topic topic_str exists, False otherwise.

        Examples:
            Test whether the topic "test" exists on the cluster::
            
                c.exists("test")
        """
        return self.topics(topic_str) != []

#

    def read(self, topics=None, group=None, offsets=None, config={}, key_type="str", value_type="str", n=1, sub=None):
        try:
            subscription_id_int = self.get_subscription_id(sub)
        except:
            _, _, subscription_id_int = self.subscribe(topics, group, offsets, config, key_type, value_type)
        #
        return self.consume(n, sub=subscription_id_int)

    # Consumer

    def read(self, topics=None, group=None, offsets=None, config={}, key_type="str", value_type="str", n=1, sub=None):
        try:
            subscription_id_int = self.get_subscription_id(sub)
        except:
            _, _, subscription_id_int = self.subscribe(topics, group, offsets, config, key_type, value_type)
        #
        return self.consume(n, sub=subscription_id_int)
