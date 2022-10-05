# kash.py

*kash.py* is a Python-based client library for Kafka based on [confluent-kafka-python](https://github.com/confluentinc/confluent-kafka-python) by Magnus Edenhill, which is itself based on the native Kafka client library [librdkafka](https://github.com/edenhill/librdkafka) by the same author.

*kash.py* has been built for Kafka users of all kinds:
* For *developers and devops engineers* to view and manipulate Kafka topics using familiar shell syntax (you have *ls*, *touch*, *rm*, *cp*, *cat*, *grep*, *wc* etc.), interactively or non-interactively.
* For *data scientists* to bridge the gap between batch and stream processing, using functions to upload/download files to/from topics, and even functional abstractions a la Databricks/Apache Spark (there are various *foldl*s, *flatmap*s and *map*s for you to explore).

## Tutorial

This is a tutorial showcasing *kash.py* in interactive mode.

Start Python and Import kash.py:
```
$ python3
>> from kash import *
>> c = Cluster("local")
>>
```

List topics:
```
>> c.ls()
['__consumer_offsets', '_schemas']
>>
```

Create a new topic "snacks":
```
>> c.touch("snacks")
'snacks'
>>> 
```

Upload the following local file "snacks.txt" to the topic "snacks" (examples inspired by the great blog [Kafka with AVRO vs., Kafka with Protobuf vs., Kafka with JSON Schema](https://simon-aubury.medium.com/kafka-with-avro-vs-kafka-with-protobuf-vs-kafka-with-json-schema-667494cbb2af) by Simon Aubury):
```
{"name": "cookie", "calories": 500.0, "colour": "brown"}
{"name": "cake", "calories": 260.0, "colour": "white"}
{"name": "timtam", "calories": 80.0, "colour": "chocolate"}
```

```
>> c.cp("./snacks.txt", "snacks")
(3, 3)
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

Count the number of messages, words and bytes in the topic "snacks":
```
>>> c.wc("snacks")
(3, 18, 169)
>>>
```

Find those messages matching the regular expression `".*cake.*"`:
```
>> c.grep("snacks", ".*cake.*")
Found matching message on partition 0, offset 1.
([{'headers': None, 'partition': 0, 'offset': 1, 'timestamp': (1, 1664989815680), 'key': None, 'value': '{"name": "cake", "calories": 260.0, "colour": "white"}'}], 1, 3)
>>
```

Create a new topic "snacks_protobuf":
```
>> c.touch("snacks_protobuf")
'snacks_protobuf'
>>
```

Copy the topic "snacks" onto another topic "snacks_protobuf" using Protobuf:

```
>> c.cp("snacks", "snacks_protobuf", target_value_type="protobuf", target_value_schema='message Snack { required string name = 1; required float calories = 2; optional string colour = 3; }')
(3, 3)
>>>
```

Show the contents of topic "snacks_protobuf" (showing the values directly as "bytes"):

```
>> c.cat("snacks_protobuf", value_type="bytes")
{'headers': None, 'partition': 0, 'offset': 0, 'timestamp': (1, 1664989815680), 'key': None, 'value': b'\x00\x00\x00\x00\x03\x00\n\x06cookie\x15\x00\x00\xfaC\x1a\x05brown'}
{'headers': None, 'partition': 0, 'offset': 1, 'timestamp': (1, 1664989815680), 'key': None, 'value': b'\x00\x00\x00\x00\x03\x00\n\x04cake\x15\x00\x00\x82C\x1a\x05white'}
{'headers': None, 'partition': 0, 'offset': 2, 'timestamp': (1, 1664989815680), 'key': None, 'value': b'\x00\x00\x00\x00\x03\x00\n\x06timtam\x15\x00\x00\xa0B\x1a\tchocolate'}
>>>
```

Show the contents of the topic "snacks_protobuf" again (decoding the values using Protobuf):
```
>>> c.cat("snacks_protobuf", value_type="protobuf")
{'headers': None, 'partition': 0, 'offset': 0, 'timestamp': (1, 1664989815680), 'key': None, 'value': {'name': 'cookie', 'calories': 500.0, 'colour': 'brown'}}
{'headers': None, 'partition': 0, 'offset': 1, 'timestamp': (1, 1664989815680), 'key': None, 'value': {'name': 'cake', 'calories': 260.0, 'colour': 'white'}}
{'headers': None, 'partition': 0, 'offset': 2, 'timestamp': (1, 1664989815680), 'key': None, 'value': {'name': 'timtam', 'calories': 80.0, 'colour': 'chocolate'}}
3
>>>
```

Do a diff of the two topics "snacks" and "snacks_protobuf":

```
>>> c1 = Cluster("local")
>>> diff(c, "snacks", c1, "snacks_protobuf", value_type1="json", value_type2="protobuf")
([], 3, 3)
>>>
```

Now we are getting functional - using a *foldl* operation to sum up the calories of the messages in "snacks":
```
>>> c.foldl("snacks", lambda acc, x: acc + x["value"]["calories"], 0, value_type="json")
(840.0, 3)
```

We can also use a *flatmap* operation to get a list of all messages in "snacks" duplicated:
```
>>> c.flatmap("snacks", lambda x: [x, x])
([{'headers': None, 'partition': 0, 'offset': 0, 'timestamp': (1, 1664989815680), 'key': None, 'value': '{"name": "cookie", "calories": 500.0, "colour": "brown"}'}, {'headers': None, 'partition': 0, 'offset': 0, 'timestamp': (1, 1664989815680), 'key': None, 'value': '{"name": "cookie", "calories": 500.0, "colour": "brown"}'}, {'headers': None, 'partition': 0, 'offset': 1, 'timestamp': (1, 1664989815680), 'key': None, 'value': '{"name": "cake", "calories": 260.0, "colour": "white"}'}, {'headers': None, 'partition': 0, 'offset': 1, 'timestamp': (1, 1664989815680), 'key': None, 'value': '{"name": "cake", "calories": 260.0, "colour": "white"}'}, {'headers': None, 'partition': 0, 'offset': 2, 'timestamp': (1, 1664989815680), 'key': None, 'value': '{"name": "timtam", "calories": 80.0, "colour": "chocolate"}'}, {'headers': None, 'partition': 0, 'offset': 2, 'timestamp': (1, 1664989815680), 'key': None, 'value': '{"name": "timtam", "calories": 80.0, "colour": "chocolate"}'}], 3)
```

Next, we use a *map* operation to add the suffix "ish" to all the colours:
```
>>> def map_function(x):
...   x["value"]["colour"] += "ish"
...   return x
... 
>>> c.map("snacks", map_function, value_type="json")
([{'headers': None, 'partition': 0, 'offset': 0, 'timestamp': (1, 1664989815680), 'key': None, 'value': {'name': 'cookie', 'calories': 500.0, 'colour': 'brownish'}}, {'headers': None, 'partition': 0, 'offset': 1, 'timestamp': (1, 1664989815680), 'key': None, 'value': {'name': 'cake', 'calories': 260.0, 'colour': 'whiteish'}}, {'headers': None, 'partition': 0, 'offset': 2, 'timestamp': (1, 1664989815680), 'key': None, 'value': {'name': 'timtam', 'calories': 80.0, 'colour': 'chocolateish'}}], 3)
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

There is more for you to explore - just browse the full [kash.py documentation](https://github.com/xdgrulez/kash.py/blob/main/docs/_build/markdown/source/kash.md) to see all the functionality of *kash.py*.

## Installation

Just install the dependencies...
```
pip install -r requirements.txt
```

...and off you go. For interactive use, e.g. using your local cluster configured in the file "clusters_unsecured/local.conf", just do the following to list the topics:
```
$ python3
>> from kash import *
>> c = Cluster("local")
>> c.ls()
['__consumer_offsets', '_schemas']
>>
```

## Documentation

Check out the full [kash.py documentation](https://github.com/xdgrulez/kash.py/blob/main/docs/_build/markdown/source/kash.md).
