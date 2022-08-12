# kash.py - the Kafka Shell for Python

kash.py is a Kafka shell based on Python 3.6+ and the [confluent_kafka](https://github.com/confluentinc/confluent-kafka-python) library from Confluent.

kash.py is an indirect successor of [Streampunk](https://github.com/xdgrulez/streampunk) - but should have an edge over it in at least two aspects:
* unlike Streampunk, it does not have a dependency on GraalVM + the Kafka Java client libraries and should thus be much easier to set up
* unlike Streampunk, a polyglot shell, it is optimized solely for Python and thus easier to use for Python users
