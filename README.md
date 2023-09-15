Check out the successor of *kash.py* - [kafi](https://github.com/xdgrulez/kafi) - including support for Confluent REST Proxy, S3, Azure Blob Storage, Pandas etc.

# kash.py

*kash.py* is a Kafka shell based on Python, or, in other words, a Python-based client library for Kafka based on [confluent-kafka-python](https://github.com/confluentinc/confluent-kafka-python) by Magnus Edenhill, which is itself based on the native Kafka client library [librdkafka](https://github.com/edenhill/librdkafka) by the same author.

The idea behind *kash.py* is to make it as easy as possible to interact with Kafka using Python, without having to know any of the implementation details of the underlying [confluent-kafka-python](https://github.com/confluentinc/confluent-kafka-python) module. To this end, not only are the functions/methods are simpler to use, but also all classes are converted to simple Python types like tuples and dictionaries. As a result, development chores that took numerous of lines of boilerplate code before can be formulated as one-liners, both in interactive mode using the Python REPL, or within convenient scripts.

*kash.py* has been built for Kafka users of all kinds:
* For *developers and devops engineers* to view and manipulate Kafka topics using familiar shell syntax (you have *ls*, *touch*, *rm*, *cp*, *cat*, *grep*, *wc* etc.).
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

*kash.py* makes use of configuration files suffixed `.yaml` which are being searched for in the folder `clusters`, starting 1) from the directory in the `KASHPY_HOME` environment variable, or, if that environment variable is not set, 2) from the current directory.

For the YAML-based configuration files sporting environment variable interpolation, *kash.py* makes use of the ingenious little library [Piny](https://github.com/pilosus/piny) from Vitaly Samigullin (see also his illustrative [blog](https://blog.pilosus.org/posts/2019/06/07/application-configs-files-or-environment-variables-actually-both/) about the genesis of Piny).

A barebones configuration file looks like this (including Schema Registry):

```
kafka:
  bootstrap.servers: localhost:9092

schema_registry:
  schema.registry.url: http://localhost:8081
```

You can also set some of the defaults of *kash.py* in the `kash` section like this:

```
kash:
  flush.num.messages: 10000
  flush.timeout: -1.0
  retention.ms: -1
  consume.timeout: 1.0
  auto.offset.reset: earliest
  enable.auto.commit: true
  session.timeout.ms: 10000
  progress.num.messages: 1000
  block.num.retries.int: 50
  block.interval: 0.1
```

You can find an in-depth explanation of these settings in the [kashpy package documentation](https://github.com/xdgrulez/kash.py/blob/main/docs/_build/markdown/source/kashpy.md), including example configuration files for connecting to [Confluent Cloud](https://www.confluent.io/confluent-cloud/), [Redpanda](https://redpanda.com/) etc.

## Kafka Made Simple

For interactive use, e.g. using your local cluster configured in the file `clusters/local.yaml`, just do the following to list the topics:
```
$ python3
>>> from kashpy.kash import *
>>> c = Cluster("local")
>>> c.ls()
['__consumer_offsets', '_schemas']
>>>
```

Or, if you'd like to replicate the topic `test` holding 1000 messages from a Kafka cluster `local` on your local machine to a Kafka cluster `ccloud` on Confluent Cloud:
```
>>> c_local = Cluster("local")
>>> c_ccloud = Cluster("ccloud")
>>> c.cp(c_local, "test", c_ccloud, "test")
(1000, 1000)
>>>
```

In the following, we go a bit deeper in two short tutorials; the first demonstrating how *kash.py* helps you to fulfill tasks on a single cluster, and the second how to make use of your newly obtained Kafka superpowers across clusters.

## Tutorial 1 (single cluster)

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

Create a new topic `snacks`:
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

Upload the following local file `snacks.txt` to the topic `snacks` (examples inspired by the great blog [Kafka with AVRO vs., Kafka with Protobuf vs., Kafka with JSON Schema](https://simon-aubury.medium.com/kafka-with-avro-vs-kafka-with-protobuf-vs-kafka-with-json-schema-667494cbb2af) by Simon Aubury):
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

Show the contents of topic `snacks`:
```
>>> c.cat("snacks")
{'headers': None, 'partition': 0, 'offset': 0, 'timestamp': (1, 1678915066323), 'key': None, 'value': '{"name": "cookie", "calories": 500.0, "colour": "brown"}'}
{'headers': None, 'partition': 0, 'offset': 1, 'timestamp': (1, 1678915066323), 'key': None, 'value': '{"name": "cake", "calories": 260.0, "colour": "white"}'}
{'headers': None, 'partition': 0, 'offset': 2, 'timestamp': (1, 1678915066323), 'key': None, 'value': '{"name": "timtam", "calories": 80.0, "colour": "chocolate"}'}
3
>>> 
```

If you'd like to see the output of the values of the messages in an indented fashion, you can tell `kash.py` 1) that you'd like to pretty print the messages and 2) that the values have the type `json`:
```
>>> c.cat("snacks", foreach_function=ppretty, value_type="json")
{
  "headers": null,
  "partition": 0,
  "offset": 0,
  "timestamp": [
    1,
    1678915066323
  ],
  "key": null,
  "value": {
    "name": "cookie",
    "calories": 500.0,
    "colour": "brown"
  }
}
{
  "headers": null,
  "partition": 0,
  "offset": 1,
  "timestamp": [
    1,
    1678915066323
  ],
  "key": null,
  "value": {
    "name": "cake",
    "calories": 260.0,
    "colour": "white"
  }
}
{
  "headers": null,
  "partition": 0,
  "offset": 2,
  "timestamp": [
    1,
    1678915066323
  ],
  "key": null,
  "value": {
    "name": "timtam",
    "calories": 80.0,
    "colour": "chocolate"
  }
}
3
>>> 
```

Count the number of messages, words and bytes of the topic `snacks`:
```
>>> c.wc("snacks")
(3, 18, 169)
>>>
```

Find those messages whose values matches the regular expression `.*cake.*`:
```
>>> c.grep("snacks", ".*cake.*")
Found matching message on partition 0, offset 1.
([{'headers': None, 'partition': 0, 'offset': 1, 'timestamp': (1, 1664989815680), 'key': None, 'value': '{"name": "cake", "calories": 260.0, "colour": "white"}'}], 1, 3)
>>>
```

Filter the messages to only keep those where the ``colour`` is ``brown``:
```
>>> c.filter("snacks", lambda x: x["value"]["colour"] == "brown", value_type="json")
([{'headers': None, 'partition': 0, 'offset': 0, 'timestamp': (1, 1666090118747), 'key': None, 'value': {'name': 'cookie', 'calories': 500.0, 'colour': 'brown'}}], 3)
>>>
```

Create a new topic `snacks_protobuf`:
```
>>> c.touch("snacks_protobuf")
'snacks_protobuf'
>>>
```

Copy the topic `snacks` onto another topic `snacks_protobuf` using Protobuf (and storing the schema in the Schema Registry):

```
>>> c.cp("snacks", "snacks_protobuf", target_value_type="protobuf", target_value_schema='message Snack { required string name = 1; required float calories = 2; optional string colour = 3; }')
(3, 3)
>>>
```

Show the contents of topic `snacks_protobuf` (showing the values directly as `bytes`):

```
>>> c.cat("snacks_protobuf", value_type="bytes")
{'headers': None, 'partition': 0, 'offset': 0, 'timestamp': (1, 1664989815680), 'key': None, 'value': b'\x00\x00\x00\x00\x03\x00\n\x06cookie\x15\x00\x00\xfaC\x1a\x05brown'}
{'headers': None, 'partition': 0, 'offset': 1, 'timestamp': (1, 1664989815680), 'key': None, 'value': b'\x00\x00\x00\x00\x03\x00\n\x04cake\x15\x00\x00\x82C\x1a\x05white'}
{'headers': None, 'partition': 0, 'offset': 2, 'timestamp': (1, 1664989815680), 'key': None, 'value': b'\x00\x00\x00\x00\x03\x00\n\x06timtam\x15\x00\x00\xa0B\x1a\tchocolate'}
>>>
```

Show the contents of the topic `snacks_protobuf` again (decoding the values using Protobuf and the Schema Registry):
```
>>> c.cat("snacks_protobuf", value_type="protobuf")
{'headers': None, 'partition': 0, 'offset': 0, 'timestamp': (1, 1664989815680), 'key': None, 'value': {'name': 'cookie', 'calories': 500.0, 'colour': 'brown'}}
{'headers': None, 'partition': 0, 'offset': 1, 'timestamp': (1, 1664989815680), 'key': None, 'value': {'name': 'cake', 'calories': 260.0, 'colour': 'white'}}
{'headers': None, 'partition': 0, 'offset': 2, 'timestamp': (1, 1664989815680), 'key': None, 'value': {'name': 'timtam', 'calories': 80.0, 'colour': 'chocolate'}}
3
>>>
```

Again, `kash.py` can give you a prettified indented output:
```
>>> c.cat("snacks_protobuf", foreach_function=ppretty, value_type="protobuf")
{
  "headers": null,
  "partition": 0,
  "offset": 0,
  "timestamp": [
    1,
    1678915066323
  ],
  "key": null,
  "value": {
    "name": "cookie",
    "calories": 500.0,
    "colour": "brown"
  }
}
{
  "headers": null,
  "partition": 0,
  "offset": 1,
  "timestamp": [
    1,
    1678915066323
  ],
  "key": null,
  "value": {
    "name": "cake",
    "calories": 260.0,
    "colour": "white"
  }
}
{
  "headers": null,
  "partition": 0,
  "offset": 2,
  "timestamp": [
    1,
    1678915066323
  ],
  "key": null,
  "value": {
    "name": "timtam",
    "calories": 80.0,
    "colour": "chocolate"
  }
}
3
>>>
```

Get a diff of the two topics `snacks` and `snacks_protobuf`, comparing the dictionaries obtained by converting the string payload in `snacks` to Python dictionaries, and the Protobuf payload in `snacks_protobuf` to Python dictionaries as well:
```
>>> c.diff("snacks", "snacks_protobuf", value_type1="json", value_type2="protobuf")
([], 3, 3)
>>>
```

Now we are getting functional - using a *foldl* operation to sum up the calories of the messages in `snacks`:
```
>>> c.foldl("snacks", lambda acc, x: acc + x["value"]["calories"], 0, value_type="json")
(840.0, 3)
>>>
```

We can also use a *flatmap* operation to get a list of all messages in `snacks` where each message is duplicated:
```
>>> c.flatmap("snacks", lambda x: [x, x])
([{'headers': None, 'partition': 0, 'offset': 0, 'timestamp': (1, 1664989815680), 'key': None, 'value': '{"name": "cookie", "calories": 500.0, "colour": "brown"}'}, {'headers': None, 'partition': 0, 'offset': 0, 'timestamp': (1, 1664989815680), 'key': None, 'value': '{"name": "cookie", "calories": 500.0, "colour": "brown"}'}, {'headers': None, 'partition': 0, 'offset': 1, 'timestamp': (1, 1664989815680), 'key': None, 'value': '{"name": "cake", "calories": 260.0, "colour": "white"}'}, {'headers': None, 'partition': 0, 'offset': 1, 'timestamp': (1, 1664989815680), 'key': None, 'value': '{"name": "cake", "calories": 260.0, "colour": "white"}'}, {'headers': None, 'partition': 0, 'offset': 2, 'timestamp': (1, 1664989815680), 'key': None, 'value': '{"name": "timtam", "calories": 80.0, "colour": "chocolate"}'}, {'headers': None, 'partition': 0, 'offset': 2, 'timestamp': (1, 1664989815680), 'key': None, 'value': '{"name": "timtam", "calories": 80.0, "colour": "chocolate"}'}], 3)
>>>
```

Next, we use a *map* operation to add the suffix `ish` to all the colours:
```
>>> def map_function(x):
...   x["value"]["colour"] += "ish"
...   return x
... 
>>> c.map("snacks", map_function, value_type="json")
([{'headers': None, 'partition': 0, 'offset': 0, 'timestamp': (1, 1664989815680), 'key': None, 'value': {'name': 'cookie', 'calories': 500.0, 'colour': 'brownish'}}, {'headers': None, 'partition': 0, 'offset': 1, 'timestamp': (1, 1664989815680), 'key': None, 'value': {'name': 'cake', 'calories': 260.0, 'colour': 'whiteish'}}, {'headers': None, 'partition': 0, 'offset': 2, 'timestamp': (1, 1664989815680), 'key': None, 'value': {'name': 'timtam', 'calories': 80.0, 'colour': 'chocolateish'}}], 3)
>>>
```

And last, but not least, we copy the topic `snacks` back to a local file `snacks1.txt` while both duplicating the messages and adding the suffix `ish` to all the colours:
```
>>> def flatmap_function(x):
...   x["value"]["colour"] += "ish"
...   return [x, x]
... 
>>> c.flatmap_to_file("snacks", "./snacks1.txt", flatmap_function, value_type="json")
(3, 6)
>>>
```

The resulting file `snacks1.txt` looks like this:
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

First, let's start Python, import kash.py and let's see what clusters we have defined in our `clusters` directory:
```
$ python3
>>> from kashpy.kash import *
>>> clusters()
['ccloud', 'local', 'redpanda']
```

Next, we create a `Cluster` object `c1`:
```
>>> c1 = Cluster("local")
>>>
```

Create a new topic `snacks1` on cluster c1:
```
>>> c1.touch("snacks1")
'snacks1'
>>>
```

Upload the following local file `snacks.txt` to the topic `snacks1` on cluster c1:
```
{"name": "cookie", "calories": 500.0, "colour": "brown"}
{"name": "cake", "calories": 260.0, "colour": "white"}
{"name": "timtam", "calories": 80.0, "colour": "chocolate"}
```

```
>>> c1.cp("./snacks.txt", "snacks1")
(3, 3)
```

Create a new `Cluster` object `c2`:
```
>>> c2 = Cluster("ccloud")
>>>
```

Create a new topic `snacks2` on `c2`:
```
>>> c2.touch("snacks2")
'snacks2'
>>>
```

Copy the topic `snacks1` from cluster `c1` to topic `snacks2` on cluster `c2`:
```
>>> cp(c1, "snacks1", c2, "snacks2")
(3, 3)
>>>
```

Get the diff from topic `snacks1` on cluster `c1` to a topic `snacks2` on cluster `c2`:
```
>>> diff(c1, "snacks1", c2, "snacks2")
([], 3, 3)
>>>
```

Now let's venture into the cross-cluster functional programming domain... let's copy topic `snacks1` on cluster `c1` to a topic  `snacks2_duplicate` on cluster `c2`, while duplicating each message using a *flatmap* operation:
```
>>> flatmap(c1, "snacks1", c2, "snacks2_duplicate", lambda x: [x, x])
(3, 6)
>>>
```

Next, we use a cross-cluster *map* operation to add the suffix `ish` to all the colours and produce the resulting messages to topic `snacks2_ish` on cluster `c2`:
```
>>> def map_function(x):
...   x["value"]["colour"] += "ish"
...   return x
... 
>>> map(c1, "snacks1", c2, "snacks2_ish", map_function, source_value_type="json")
(3, 3)
>>>
```

Now for the most advanced operation of the tutorials: A *zip* of topic `snacks1` on cluster `c1` and topic `snacks2_ish` on cluster `c2` followed by a *foldl* accumulating pairs of those messages from both topics where the colour of the message from topic `snacks2_ish` ends with `eish`:

```
>>> def zip_foldl_function(acc, x1, x2):
...   return acc + [(x1, x2)] if x2["value"]["colour"].endswith("eish") else acc
... 
>>> zip_foldl(c1, "snacks1", c2, "snacks2_ish", zip_foldl_function, [], value_type2="json")
([({'headers': None, 'partition': 0, 'offset': 1, 'timestamp': (1, 1664989815680), 'key': None, 'value': b'{"name": "cake", "calories": 260.0, "colour": "white"}'}, {'headers': None, 'partition': 0, 'offset': 1, 'timestamp': (1, 1664989815680), 'key': None, 'value': {'name': 'cake', 'calories': 260.0, 'colour': 'whiteish'}}), ({'headers': None, 'partition': 0, 'offset': 2, 'timestamp': (1, 1664989815680), 'key': None, 'value': b'{"name": "timtam", "calories": 80.0, "colour": "chocolate"}'}, {'headers': None, 'partition': 0, 'offset': 2, 'timestamp': (1, 1664989815680), 'key': None, 'value': {'name': 'timtam', 'calories': 80.0, 'colour': 'chocolateish'}})], 3, 3)
>>>
```

## kash.py and Functional Programming

As already hinted at in the tutorials above, the more advanced functionality of *kash.py* rests on a *Functional Programming* (*FP*) backbone. If you intend to use *kash.py* to ease your everyday tasks, you don't have to be aware of that - the bash-like abstractions on top of the functional backbone should already help you out a lot. However, if you are familiar with FP or you would like to become familiar with it, this functional backbone of *kash.py* can actually let you wield even stronger *Kafka superpowers*.

### Single Cluster

#### Kafka Topic to Python Datatype

We start with functional backbone of those functions which consume a Kafka topic and return a Python datatype. Those functions are based on the `foldl` ("fold left") function. `foldl` consumes messages from a topic and then, for each message, calls a function that takes this message and an accumulator of any type, and returns the updated accumulator. `foldl` can be used e.g. to aggregate information from topics, such as the sum of calories of the messages of the topic `snacks` in the following example:
```
>>> c.foldl("snacks", lambda acc, x: acc + x["value"]["calories"], 0, value_type="json")
(840.0, 3)
>>>
```

`foldl` is the basis for `flatmap` which also consumes messages from a topic, but then calls a simpler function that takes just this message and returns a list of anything (including the empty list). `flatmap` can be used e.g. to duplicate the messages of a topic, as in the following example:
```
>>> c.flatmap("snacks", lambda x: [x, x])
([{'headers': None, 'partition': 0, 'offset': 0, 'timestamp': (1, 1664989815680), 'key': None, 'value': '{"name": "cookie", "calories": 500.0, "colour": "brown"}'}, {'headers': None, 'partition': 0, 'offset': 0, 'timestamp': (1, 1664989815680), 'key': None, 'value': '{"name": "cookie", "calories": 500.0, "colour": "brown"}'}, {'headers': None, 'partition': 0, 'offset': 1, 'timestamp': (1, 1664989815680), 'key': None, 'value': '{"name": "cake", "calories": 260.0, "colour": "white"}'}, {'headers': None, 'partition': 0, 'offset': 1, 'timestamp': (1, 1664989815680), 'key': None, 'value': '{"name": "cake", "calories": 260.0, "colour": "white"}'}, {'headers': None, 'partition': 0, 'offset': 2, 'timestamp': (1, 1664989815680), 'key': None, 'value': '{"name": "timtam", "calories": 80.0, "colour": "chocolate"}'}, {'headers': None, 'partition': 0, 'offset': 2, 'timestamp': (1, 1664989815680), 'key': None, 'value': '{"name": "timtam", "calories": 80.0, "colour": "chocolate"}'}], 3)
>>>
```

`flatmap`, in turn, is the basis for `map` which again consumes messages from a topic, but then calls a function that takes just this message and returns anything. `map` can be used e.g. to modify the messages of a topic, as in the following example:
```
>>> def map_function(x):
...   x["value"]["colour"] += "ish"
...   return x
... 
>>> c.map("snacks", map_function, value_type="json")
([{'headers': None, 'partition': 0, 'offset': 0, 'timestamp': (1, 1664989815680), 'key': None, 'value': {'name': 'cookie', 'calories': 500.0, 'colour': 'brownish'}}, {'headers': None, 'partition': 0, 'offset': 1, 'timestamp': (1, 1664989815680), 'key': None, 'value': {'name': 'cake', 'calories': 260.0, 'colour': 'whiteish'}}, {'headers': None, 'partition': 0, 'offset': 2, 'timestamp': (1, 1664989815680), 'key': None, 'value': {'name': 'timtam', 'calories': 80.0, 'colour': 'chocolateish'}}], 3)
>>>
```

Filter the messages to only keep those where the ``colour`` is ``brown``:
```
>>> c.filter("snacks", lambda x: x["value"]["colour"] == "brown", value_type="json")
([{'headers': None, 'partition': 0, 'offset': 0, 'timestamp': (1, 1666090118747), 'key': None, 'value': {'name': 'cookie', 'calories': 500.0, 'colour': 'brown'}}], 3)
>>>
```

Here is an overview of the relations between those functions of *kash.py* which consume messages from Kafka topics and return a Python datatype (or nothing):

* `foldl`

    * `flatmap`, `cat`, `foreach`, `wc`

        * `map`, `filter`, `grep`, `grep_fun`

As explained above, `foldl` is the basis of it all, `flatmap` is based on `foldl`and `map` is based on `flatmap`. In addition, `cat`, `foreach` and `wc` are also based on `foldl`, and `filter` and `grep` and `grep_fun` are based on `flatmap`.

#### Kafka Topic to Local File

We turn to those functions consuming messages from a Kafka topic and writing them to a local file, transforming or even removing messages. Here, the basis function is `flatmap_to_file`. Here is an example where the flatmap function is the identity function. Hence, the messages from the topic `snacks` are written to the local file `./snacks.txt`:

```
>>> c.flatmap_to_file("snacks", "./snacks.txt", lambda x: [x])
(3, 3)
>>> 
```

`map_to_file` is based on `flatmap_to_file` and can be used to transform each message before it is written to the local file:
```
>>> def map_function(x):
...   x["value"]["colour"] += "ish"
...   return x
... 
>>> c.map_to_file("snacks", "./snacks.txt", map_function)
(3, 3)
>>> 
```

`filter_to_file` is also based on `flatmap_to_file` and writes only those messages to the local file which fulfil a condition, for example:
```
>>> def filter_function(message_dict):
...   return "cake" in message_dict["value"]
... 
>>> c.filter_to_file("snacks", "./snacks.txt", filter_function)
(3, 1)

```

Here is an overview of the relations between those functions of *kash.py* which consume messages from Kafka topics and write them to a local file:

* `flatmap_to_file`

    * `map_to_file`, `filter_to_file`, `cp/download`

Again, as explained above, `map_to_file` and `filter_to_file` are based on `flatmap_to_file`, as well as `cp` (with the first not being a file and the second argument being a file marked by it containing a slash `/`) and `download`.

#### Local File to Kafka Topic

The last set of functional functions are those reading lines/messages from a local file and producing them to a Kafka topic. Here, the basis function is `flatmap_from_file`. Here is an example where the flatmap function is the identity function. Hence, the messages from the local file `./snacks.txt` are produced into the topic `snacks`:

```
>>> c.flatmap_from_file("./snacks.txt", "snacks", lambda x: [x])
(3, 3)
>>> 
```

`map_from_file` is based on `flatmap_from_file` and can be used to transform each line/message before it is written to the local file:
```
>>> c.map_to_file("snacks", "./snacks.txt", lambda x: x)
(3, 3)
>>> 
```

`filter_from_file` is also based on `flatmap_from_file` and produces only those messages to the Kafka topic which fulfil a condition. In the example below we produce only those messages which have the string `cake` in their value:
```
>>> def filter_function(xy):
...   x, y = xy
...   return "cake" in y
... 
>>> c.filter_from_file("./snacks.txt", "snacks", filter_function)
(3, 1)
>>> 
```

Here is an overview of the relations between those functions of *kash.py* which read lines/messages from a file and produce them to a Kafka topic:

* `flatmap_from_file`

    * `map_from_file`, `filter_from_file`, `cp/download`

Again, as explained above, `map_from_file` and `filter_from_file` are based on `flatmap_from_file`, as well as `cp` (with the first argument being a file marked by it containing a slash `/` and the second not a file) and `upload`.

There is also a function `foldl_from_file`. Here is an example where we use it to sum up the calories of the lines/messages from the file `./snacks.txt`:
```
>> foldl_from_file("./snacks.txt", lambda acc, x: acc + json.loads(x)["calories"], 0)
(840.0, 3)
>>> 
```

### Cross-Cluster

#### Kafka Topic to Kafka Topic

*kash.py* also offers functional functions to use across clusters, based on `flatmap`. In the example below, we use `flatmap` to copy the topic  `snacks1` on cluster `c1` to a topic  `snacks2_duplicate` on cluster `c2`, while duplicating each message using a *flatmap* operation:
```
>>> flatmap(c1, "snacks1", c2, "snacks2_duplicate", lambda x: [x, x])
(3, 6)
>>>
```

`map` is based on `flatmap`. In the following example, we use a cross-cluster `map` to add the suffix `ish` to all the colours and produce the resulting messages to topic `snacks2_ish` on cluster `c2`:
```
>>> def map_function(x):
...   x["value"]["colour"] += "ish"
...   return x
... 
>>> map(c1, "snacks1", c2, "snacks2_ish", map_function, source_value_type="json")
(3, 3)
>>>
```

The function `filter` is also based on `flatmap` and, in the example below, is used to only keep those messages where the ``colour`` is ``brown``:
```
>>> c.filter("snacks", lambda x: x["value"]["colour"] == "brown", value_type="json")
([{'headers': None, 'partition': 0, 'offset': 0, 'timestamp': (1, 1666090118747), 'key': None, 'value': {'name': 'cookie', 'calories': 500.0, 'colour': 'brown'}}], 3)
>>>
```
Here is an overview of the relations between those cross-cluster functions of *kash.py* which consume messages from a topic on one cluster and produce them in another (or equally named) topic on another (or the same) cluster:

* `flatmap`

    * `map`, `filter`, `cp`

#### Consuming Two Topics At the Same Time

The `diff` and `diff_fun` functions are based on the functional abstraction `zip_foldl`. As an example, 
we show the *zip* of topic `snacks1` on cluster `c1` and topic `snacks2_ish` on cluster `c2` followed by a *foldl* accumulating pairs of those messages from both topics where the colour of the message from topic `snacks2_ish` ends with `eish`:

```
>>> def zip_foldl_function(acc, x1, x2):
...   return acc + [(x1, x2)] if x2["value"]["colour"].endswith("eish") else acc
... 
>>> zip_foldl(c1, "snacks1", c2, "snacks2_ish", zip_foldl_function, [], value_type2="json")
([({'headers': None, 'partition': 0, 'offset': 1, 'timestamp': (1, 1664989815680), 'key': None, 'value': b'{"name": "cake", "calories": 260.0, "colour": "white"}'}, {'headers': None, 'partition': 0, 'offset': 1, 'timestamp': (1, 1664989815680), 'key': None, 'value': {'name': 'cake', 'calories': 260.0, 'colour': 'whiteish'}}), ({'headers': None, 'partition': 0, 'offset': 2, 'timestamp': (1, 1664989815680), 'key': None, 'value': b'{"name": "timtam", "calories": 80.0, "colour": "chocolate"}'}, {'headers': None, 'partition': 0, 'offset': 2, 'timestamp': (1, 1664989815680), 'key': None, 'value': {'name': 'timtam', 'calories': 80.0, 'colour': 'chocolateish'}})], 3, 3)
>>>
```

Here is the overview of the relations between those functions of *kash.py* which consume messages from two topics (on the same cluster or on different clusters):

* `zip_foldl`

    * `diff/diff_fun`
