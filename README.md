# kash.py

*kash.py* is a Kafka shell based on Python, or, in other words, a Python-based client library for Kafka based on [confluent-kafka-python](https://github.com/confluentinc/confluent-kafka-python) by Magnus Edenhill, which is itself based on the native Kafka client library [librdkafka](https://github.com/edenhill/librdkafka) by the same author.

The idea behind *kash.py* is to make it as easy as possible to interact with Kafka using Python, without having to know any of the implementation details of the underlying [confluent-kafka-python](https://github.com/confluentinc/confluent-kafka-python) module. To this end, not only are the functions/methods are simpler to use, but also all classes are converted to simple Python types like tuples and dictionaries.

*kash.py* has been built for Kafka users of all kinds:
* For *developers and devops engineers* to view and manipulate Kafka topics using familiar shell syntax (you have *ls*, *touch*, *rm*, *cp*, *cat*, *grep*, *wc* etc.), interactively or non-interactively.
* For *data scientists* to bridge the gap between batch and stream processing, using functions to upload/download files to/from topics, and even functional abstractions a la Databricks/Apache Spark (there are various *foldl*s, *flatmap*s and *map*s for you to explore).

*kash.py* supports *Avro*, *Protobuf* and *JSONSchema*, Confluent Cloud, Redpanda, etc...and it will give you *Kafka superpowers* of a kind you have never experienced before. Honestly :)

Check out the full [kashpy package documentation](https://github.com/xdgrulez/kash.py/blob/main/docs/_build/markdown/source/kashpy.md) if you are interested in seeing the entire functionality of *kash.py*.

## Installation

Just write...
```
pip install kashpy
```
...and off you go.

## Configuration

*kash.py* makes use of configuration files suffixed ``.conf`` which are being searched for in one of these two folders: ``clusters_unsecured`` or ``clusters_secured``. A barebones configuration file looks like this (including Schema Registry):

```
[kafka]
bootstrap.servers=localhost:9092

[schema_registry]
schema.registry.url=http://localhost:8081
```

You can also set some of the defaults of *kash.py* in the ``[kash]`` section like this:

```
[kash]
flush.num.messages=10000
flush.timeout=-1.0
retention.ms=-1
consume.timeout=1.0
auto.offset.reset=earliest
enable.auto.commit=true
session.timeout.ms=10000
progress.num.messages=1000
block.num.retries.int=50
block.interval=0.1
```

You can find an in-depth explanation of these settings in the [kashpy package documentation](https://github.com/xdgrulez/kash.py/blob/main/docs/_build/markdown/source/kashpy.md), including example configuration files for connecting to [Confluent Cloud](https://www.confluent.io/confluent-cloud/) etc.

## Getting Started

For interactive use, e.g. using your local cluster configured in the file "clusters_unsecured/local.conf", just do the following to list the topics:
```
$ python3
>>> from kashpy.kash import *
>>> c = Cluster("local")
>>> c.ls()
['__consumer_offsets', '_schemas']
>>>
```

## Tutorial 1 (single-cluster)

This is the first tutorial, showcasing the single cluster capabilities of *kash.py* in interactive mode.

Let's start Python, import kash.py and create a `Cluster` object `c`:
```
$ python3
>>> from kashpy.kash import *
>>> c = Cluster("local")
>>>
```

List the topics on the cluster:
```
>>> c.ls()
['__consumer_offsets', '_schemas']
>>>
```

Create a new topic "snacks":
```
>>> c.touch("snacks")
'snacks'
>>> 
```

List the topics on the cluster again:
```
>>> c.ls()
['__consumer_offsets', '_schemas', 'snacks']
>>>
```

Upload the following local file "snacks.txt" to the topic "snacks" (examples inspired by the great blog [Kafka with AVRO vs., Kafka with Protobuf vs., Kafka with JSON Schema](https://simon-aubury.medium.com/kafka-with-avro-vs-kafka-with-protobuf-vs-kafka-with-json-schema-667494cbb2af) by Simon Aubury):
```
{"name": "cookie", "calories": 500.0, "colour": "brown"}
{"name": "cake", "calories": 260.0, "colour": "white"}
{"name": "timtam", "calories": 80.0, "colour": "chocolate"}
```

```
>>> c.cp("./snacks.txt", "snacks")
(3, 3)
>>>
```

Show the contents of topic "snacks":
```
>>> c.cat("snacks")
{'headers': None, 'partition': 0, 'offset': 0, 'timestamp': (1, 1664989815680), 'key': None, 'value': '{"name": "cookie", "calories": 500.0, "colour": "brown"}'}
{'headers': None, 'partition': 0, 'offset': 1, 'timestamp': (1, 1664989815680), 'key': None, 'value': '{"name": "cake", "calories": 260.0, "colour": "white"}'}
{'headers': None, 'partition': 0, 'offset': 2, 'timestamp': (1, 1664989815680), 'key': None, 'value': '{"name": "timtam", "calories": 80.0, "colour": "chocolate"}'}
3
>>> 
```

Count the number of messages, words and bytes of the topic "snacks":
```
>>> c.wc("snacks")
(3, 18, 169)
>>>
```

Find those messages whose values matches the regular expression `".*cake.*"`:
```
>>> c.grep("snacks", ".*cake.*")
Found matching message on partition 0, offset 1.
([{'headers': None, 'partition': 0, 'offset': 1, 'timestamp': (1, 1664989815680), 'key': None, 'value': '{"name": "cake", "calories": 260.0, "colour": "white"}'}], 1, 3)
>>>
```

Create a new topic "snacks_protobuf":
```
>>> c.touch("snacks_protobuf")
'snacks_protobuf'
>>>
```

Copy the topic "snacks" onto another topic "snacks_protobuf" using Protobuf (and storing the schema in the Schema Registry):

```
>>> c.cp("snacks", "snacks_protobuf", target_value_type="protobuf", target_value_schema='message Snack { required string name = 1; required float calories = 2; optional string colour = 3; }')
(3, 3)
>>>
```

Show the contents of topic "snacks_protobuf" (showing the values directly as "bytes"):

```
>>> c.cat("snacks_protobuf", value_type="bytes")
{'headers': None, 'partition': 0, 'offset': 0, 'timestamp': (1, 1664989815680), 'key': None, 'value': b'\x00\x00\x00\x00\x03\x00\n\x06cookie\x15\x00\x00\xfaC\x1a\x05brown'}
{'headers': None, 'partition': 0, 'offset': 1, 'timestamp': (1, 1664989815680), 'key': None, 'value': b'\x00\x00\x00\x00\x03\x00\n\x04cake\x15\x00\x00\x82C\x1a\x05white'}
{'headers': None, 'partition': 0, 'offset': 2, 'timestamp': (1, 1664989815680), 'key': None, 'value': b'\x00\x00\x00\x00\x03\x00\n\x06timtam\x15\x00\x00\xa0B\x1a\tchocolate'}
>>>
```

Show the contents of the topic "snacks_protobuf" again (decoding the values using Protobuf and the Schema Registry):
```
>>> c.cat("snacks_protobuf", value_type="protobuf")
{'headers': None, 'partition': 0, 'offset': 0, 'timestamp': (1, 1664989815680), 'key': None, 'value': {'name': 'cookie', 'calories': 500.0, 'colour': 'brown'}}
{'headers': None, 'partition': 0, 'offset': 1, 'timestamp': (1, 1664989815680), 'key': None, 'value': {'name': 'cake', 'calories': 260.0, 'colour': 'white'}}
{'headers': None, 'partition': 0, 'offset': 2, 'timestamp': (1, 1664989815680), 'key': None, 'value': {'name': 'timtam', 'calories': 80.0, 'colour': 'chocolate'}}
3
>>>
```

Get a diff of the two topics "snacks" and "snacks_protobuf", comparing the dictionaries obtained by converting the strings in "snacks" to JSONs and consequently Python dictionaries, and converting the Protobuf payload in "snacks_protobuf" to Python dictionaries as well:

```
>>> c.diff("snacks", "snacks_protobuf", value_type1="json", value_type2="protobuf")
([], 3, 3)
>>>
```

Now we are getting functional - using a *foldl* operation to sum up the calories of the messages in "snacks":
```
>>> c.foldl("snacks", lambda acc, x: acc + x["value"]["calories"], 0, value_type="json")
(840.0, 3)
>>>
```

We can also use a *flatmap* operation to get a list of all messages in "snacks" where each message is duplicated:
```
>>> c.flatmap("snacks", lambda x: [x, x])
([{'headers': None, 'partition': 0, 'offset': 0, 'timestamp': (1, 1664989815680), 'key': None, 'value': '{"name": "cookie", "calories": 500.0, "colour": "brown"}'}, {'headers': None, 'partition': 0, 'offset': 0, 'timestamp': (1, 1664989815680), 'key': None, 'value': '{"name": "cookie", "calories": 500.0, "colour": "brown"}'}, {'headers': None, 'partition': 0, 'offset': 1, 'timestamp': (1, 1664989815680), 'key': None, 'value': '{"name": "cake", "calories": 260.0, "colour": "white"}'}, {'headers': None, 'partition': 0, 'offset': 1, 'timestamp': (1, 1664989815680), 'key': None, 'value': '{"name": "cake", "calories": 260.0, "colour": "white"}'}, {'headers': None, 'partition': 0, 'offset': 2, 'timestamp': (1, 1664989815680), 'key': None, 'value': '{"name": "timtam", "calories": 80.0, "colour": "chocolate"}'}, {'headers': None, 'partition': 0, 'offset': 2, 'timestamp': (1, 1664989815680), 'key': None, 'value': '{"name": "timtam", "calories": 80.0, "colour": "chocolate"}'}], 3)
>>>
```

Next, we use a *map* operation to add the suffix "ish" to all the colours:
```
>>> def map_function(x):
...   x["value"]["colour"] += "ish"
...   return x
... 
>>> c.map("snacks", map_function, value_type="json")
([{'headers': None, 'partition': 0, 'offset': 0, 'timestamp': (1, 1664989815680), 'key': None, 'value': {'name': 'cookie', 'calories': 500.0, 'colour': 'brownish'}}, {'headers': None, 'partition': 0, 'offset': 1, 'timestamp': (1, 1664989815680), 'key': None, 'value': {'name': 'cake', 'calories': 260.0, 'colour': 'whiteish'}}, {'headers': None, 'partition': 0, 'offset': 2, 'timestamp': (1, 1664989815680), 'key': None, 'value': {'name': 'timtam', 'calories': 80.0, 'colour': 'chocolateish'}}], 3)
>>>
```

And last, but not least, we copy the topic "snacks" back to a local file "snacks1.txt" while both duplicating the messages and adding the suffix "ish" to all the colours:
```
>>> def flatmap_function(x):
...   x["value"]["colour"] += "ish"
...   return [x, x]
... 
>>> c.flatmap_to_file("snacks", "./snacks1.txt", flatmap_function, value_type="json")
(3, 6)
>>>
```

The resulting file "snacks1.txt" looks like this:
```
{"name": "cookie", "calories": 500.0, "colour": "brownish"}
{"name": "cookie", "calories": 500.0, "colour": "brownish"}
{"name": "cake", "calories": 260.0, "colour": "whiteish"}
{"name": "cake", "calories": 260.0, "colour": "whiteish"}
{"name": "timtam", "calories": 80.0, "colour": "chocolateish"}
{"name": "timtam", "calories": 80.0, "colour": "chocolateish"}
```

## Tutorial 2 (cross-cluster)

This is the second tutorial, showcasing the cross-cluster capabilities of *kash.py* in interactive mode.

Again, let's start Python, import kash.py and create a `Cluster` object `c1`:
```
$ python3
>>> from kashpy.kash import *
>>> c1 = Cluster("local")
>>>
```

Create a new topic "snacks1" on cluster c1:
```
>>> c1.touch("snacks1")
'snacks1'
>>>
```

Upload the following local file "snacks.txt" to the topic "snacks1" on cluster c1:
```
{"name": "cookie", "calories": 500.0, "colour": "brown"}
{"name": "cake", "calories": 260.0, "colour": "white"}
{"name": "timtam", "calories": 80.0, "colour": "chocolate"}
```

```
>>> c1.cp("./snacks.txt", "snacks1")
(3, 3)
```

Create a new `Cluster` object c2:
```
>>> c2 = Cluster("ccloud")
>>>
```

Create a new topic "snacks2" on cluster 2:
```
>>> c2.touch("snacks2")
'snacks2'
>>>
```

Copy the topic "snacks1" from cluster c1 to topic "snacks2" on cluster c2:
```
>>> cp(c1, "snacks1", c2, "snacks2")
(3, 3)
>>>
```

Get the diff from topic "snacks1" on cluster c1 to a topic "snacks2" on cluster c2:
```
>>> diff(c1, "snacks1", c2, "snacks2")
([], 3, 3)
>>>
```

Now let's venture into the cross-cluster functional programming domain... let's copy topic "snacks1" on cluster c1 to a topic "snacks2_duplicate" on cluster c2, while duplicating each message using a *flatmap* operation:
```
>>> flatmap(c1, "snacks1", c2, "snacks2_duplicate", lambda x: [x, x])
(3, 6)
>>>
```

Next, we use a cross-cluster *map* operation to add the suffix "ish" to all the colours and produce the resulting messages to topic "snacks2_ish" on cluster c2:
```
>>> def map_function(x):
...   x["value"]["colour"] += "ish"
...   return x
... 
>>> map(c1, "snacks1", c2, "snacks2_ish", map_function, source_value_type="json")
(3, 3)
>>>
```

Now for the most advanced operation of the tutorials: A *zip* of topic "snacks1" on cluster c1 and topic "snacks2_ish" on cluster c2 followed by a *foldl* accumulating pairs of those messages from both topics where the colour of the message from topic "snacks2_ish" ends with "eish":

```
>>> def zip_foldl_function(acc, x1, x2):
...   return acc + [(x1, x2)] if x2["value"]["colour"].endswith("eish") else acc
... 
>>> zip_foldl(c1, "snacks1", c2, "snacks2_ish", zip_foldl_function, [], value_type2="json")
([({'headers': None, 'partition': 0, 'offset': 1, 'timestamp': (1, 1664989815680), 'key': None, 'value': b'{"name": "cake", "calories": 260.0, "colour": "white"}'}, {'headers': None, 'partition': 0, 'offset': 1, 'timestamp': (1, 1664989815680), 'key': None, 'value': {'name': 'cake', 'calories': 260.0, 'colour': 'whiteish'}}), ({'headers': None, 'partition': 0, 'offset': 2, 'timestamp': (1, 1664989815680), 'key': None, 'value': b'{"name": "timtam", "calories": 80.0, "colour": "chocolate"}'}, {'headers': None, 'partition': 0, 'offset': 2, 'timestamp': (1, 1664989815680), 'key': None, 'value': {'name': 'timtam', 'calories': 80.0, 'colour': 'chocolateish'}})], 3, 3)
>>>
```
