CURRENT_TIME = 0

RD_KAFKA_PARTITION_UA = -1

    # Schema Registry helper methods (inside the Cluster class to do caching etc.)

    def post_schema(self, schema_str, schema_type_str, topic_str, key_bool):
        key_or_value_str = "key" if key_bool else "value"
        #
        schema_registry_url_str = self.schema_registry_config_dict["schema.registry.url"]
        url_str = f"{schema_registry_url_str}/subjects/{topic_str}-{key_or_value_str}/versions?normalize=true"
        headers_dict = {"Accept": "application/vnd.schemaregistry.v1+json", "Content-Type": "application/vnd.schemaregistry.v1+json"}
        if "basic.auth.user.info" in self.schema_registry_config_dict:
            user_password_str = self.schema_registry_config_dict["basic.auth.user.info"]
            user_str_password_str_tuple = tuple(user_password_str.split(":"))
        else:
            user_str_password_str_tuple = None
        schema_dict = {"schema": schema_str, "schemaType": schema_type_str}
        response = requests.post(url_str, headers=headers_dict, json=schema_dict, auth=user_str_password_str_tuple)
        response_dict = response.json()
        schema_id_int = response_dict["id"]
        return schema_id_int

    def schema_str_to_generalizedProtocolMessageType(self, schema_str, topic_str, key_bool):
        schema_id_int = self.post_schema(schema_str, "PROTOBUF", topic_str, key_bool)
        #
        generalizedProtocolMessageType = self.schema_id_int_and_schema_str_to_generalizedProtocolMessageType(schema_id_int, schema_str)
        return generalizedProtocolMessageType

    def schema_id_int_and_schema_str_to_generalizedProtocolMessageType(self, schema_id_int, schema_str):
        path_str = f"/{tempfile.gettempdir()}/kash.py/clusters/{self.cluster_str}"
        os.makedirs(path_str, exist_ok=True)
        file_str = f"schema_{schema_id_int}.proto"
        file_path_str = f"{path_str}/{file_str}"
        with open(file_path_str, "w") as textIOWrapper:
            textIOWrapper.write(schema_str)
        #
        import grpc_tools.protoc
        grpc_tools.protoc.main(["protoc", f"-I{path_str}", f"--python_out={path_str}", f"{file_str}"])
        #
        sys.path.insert(1, path_str)
        schema_module = importlib.import_module(f"schema_{schema_id_int}_pb2")
        schema_name_str = list(schema_module.DESCRIPTOR.message_types_by_name.keys())[0]
        generalizedProtocolMessageType = getattr(schema_module, schema_name_str)
        return generalizedProtocolMessageType


# Producer

    def write(self, value, key=None, key_type="str", value_type="str", key_schema=None, value_schema=None, partition=RD_KAFKA_PARTITION_UA, timestamp=CURRENT_TIME, headers=None, on_delivery=None):
        return self.produce(self.topic_str, value, key, key_type, value_type, key_schema, value_schema, partition, timestamp, headers, on_delivery)

    #

    def produce(self, topic_str, value, key=None, key_type="str", value_type="str", key_schema=None, value_schema=None, partition=RD_KAFKA_PARTITION_UA, timestamp=CURRENT_TIME, headers=None, on_delivery=None):
        """Produce a message to a topic.

        Produce a message to a topic. The key and the value of the message can be either bytes, a string, a dictionary, or a schema-based format supported by the Confluent Schema Registry (currently Avro, Protobuf or JSONSchema).

        Args:
            topic_str (:obj:`str`): The topic to produce to.
            value (:obj:`bytes` | :obj:`str` |  :obj:`dict`): The value of the message to be produced.
            key (:obj:`bytes` | :obj:`str` |  :obj:`dict`, optional): The key of the message to be produced. Defaults to None.
            key_type (:obj:`str`, optional): The key type ("bytes", "str", "json", "avro", "protobuf" or "pb", or "jsonschema"). Defaults to "str".
            value_type (:obj:`str`, optional): The value type ("bytes", "str", "json", "avro", "protobuf" or "pb", or "jsonschema"). Defaults to "str".
            key_schema (:obj:`str`, optional): The schema of the key of the message to be produced (if key_type is either "avro", "protobuf" or "pb", or "jsonschema"). Defaults to None.
            value_schema (:obj:`str`, optional): The schema of the value of the message to be produced (if key_type is either "avro", "protobuf" or "pb", or "jsonschema"). Defaults to None.
            partition (:obj:`int`, optional): The partition to produce to. Defaults to RD_KAFKA_PARTITION_UA = -1, i.e., the partition is selected by configured built-in partitioner.
            timestamp (:obj:`int`, optional): Message timestamp (CreateTime) in milliseconds since epoch UTC. Defaults to CURRENT_TIME = 0.
            headers (:obj:`dict` | :obj:`list`, optional): Message headers to set on the message. The header key must be a string while the value must be binary, unicode or None. Accepts a list of (key,value) or a dict. Defaults to None.
            on_delivery (:obj:`function`, optional): Delivery report callback to call (from poll() or flush()) on successful or failed delivery. Passed on to confluent_kafka.Producer.produce(). Takes confluent_kafka.kafkaError and confluent_kafka.Message objects and returns nothing.

        Returns:
            :obj:`tuple(bytes | str, bytes | str)`: Pair of bytes or string and bytes or string (=the key and the value of the produced message).

        Examples:
            Produce a message with value = "value 1" and key = None to the topic "test"::

                c.produce("test", "value 1")

            Produce a message with value = "value 1" and key = "key 1" to the topic "test"::

                c.produce("test", "value 1", key="key 1")

            Produce a message with value = "value 1" and key = "key 1" to partition 0 of the topic "test"::

                c.produce("test", "value 1", key="key 1", partition=0)

            Produce a message with value = "value 1" and key = "key 1" to the topic "test", set the timestamp of this message to 1664902656169::

                c.produce("test", "value 1", key="key 1", timestamp=1664902656169)

            Produce a message with value = "value 1" and key = None to the topic "test", set the headers of this message to {"bla": "blups"}::

                c.produce("test", "value 1", headers={"bla": "blups"})

            Produce a message with value = {'name': 'cookie', 'calories': 500.0, 'colour': 'brown'} and key = None to the topic "test", using JSON without schema::

                c.produce("test", {'name': 'cookie', 'calories': 500.0, 'colour': 'brown'}, value_type="json")

            Produce a message with value = {'name': 'cookie', 'calories': 500.0, 'colour': 'brown'} and key = None to the topic "test", using Avro with the schema '{ "type": "record", "name": "myrecord", "fields": [{"name": "name",  "type": "string" }, {"name": "calories", "type": "float" }, {"name": "colour", "type": "string" }] }'::

                c.produce("test", {'name': 'cookie', 'calories': 500.0, 'colour': 'brown'}, value_type="avro", value_schema='{ "type": "record", "name": "myrecord", "fields": [{"name": "name",  "type": "string" }, {"name": "calories", "type": "float" }, {"name": "colour", "type": "string" }] }')

            Produce a message with value = {'name': 'cookie', 'calories': 500.0, 'colour': 'brown'} and key = None to the topic "test", using Protobuf with the schema 'message Snack { required string name = 1; required float calories = 2; optional string colour = 3; }'::

                c.produce("test", {'name': 'cookie', 'calories': 500.0, 'colour': 'brown'}, value_type="protobuf", value_schema='message Snack { required string name = 1; required float calories = 2; optional string colour = 3; }')

            Produce a message with value = {'name': 'cookie', 'calories': 500.0, 'colour': 'brown'} and key = None to the topic "test", using JSONSchema with the schema '{ "title": "abc", "definitions" : { "record:myrecord" : { "type" : "object", "required" : [ "name", "calories" ], "additionalProperties" : false, "properties" : { "name" : {"type" : "string"}, "calories" : {"type" : "number"}, "colour" : {"type" : "string"} } } }, "$ref" : "#/definitions/record:myrecord" }'::

                c.produce("test", {'name': 'cookie', 'calories': 500.0, 'colour': 'brown'}, value_type="jsonschema", value_schema='{ "title": "abc", "definitions" : { "record:myrecord" : { "type" : "object", "required" : [ "name", "calories" ], "additionalProperties" : false, "properties" : { "name" : {"type" : "string"}, "calories" : {"type" : "number"}, "colour" : {"type" : "string"} } } }, "$ref" : "#/definitions/record:myrecord" }')
        """
        key_type_str = key_type
        value_type_str = value_type
        key_schema_str = key_schema
        value_schema_str = value_schema
        partition_int = partition
        timestamp_int = timestamp
        headers_dict_or_list = headers
        #

        def serialize(key_bool):
            type_str = key_type_str if key_bool else value_type_str
            schema_str = key_schema_str if key_bool else value_schema_str
            payload = key if key_bool else value
            messageField = MessageField.KEY if key_bool else MessageField.VALUE
            #

            def payload_to_payload_dict(payload):
                if isinstance(payload, str) or isinstance(payload, bytes):
                    payload_dict = json.loads(payload)
                else:
                    payload_dict = payload
                return payload_dict
            #
            if type_str.lower() == "json":
                if isinstance(payload, dict):
                    payload_str_or_bytes = json.dumps(payload)
                else:
                    payload_str_or_bytes = payload
            elif type_str.lower() in ["pb", "protobuf"]:
                generalizedProtocolMessageType = self.schema_str_to_generalizedProtocolMessageType(schema_str, topic_str, key_bool)
                protobufSerializer = ProtobufSerializer(generalizedProtocolMessageType, self.schemaRegistryClient, {"use.deprecated.format": False})
                payload_dict = payload_to_payload_dict(payload)
                protobuf_message = generalizedProtocolMessageType()
                ParseDict(payload_dict, protobuf_message)
                payload_str_or_bytes = protobufSerializer(protobuf_message, SerializationContext(topic_str, messageField))
            elif type_str.lower() == "avro":
                avroSerializer = AvroSerializer(self.schemaRegistryClient, schema_str)
                payload_dict = payload_to_payload_dict(payload)
                payload_str_or_bytes = avroSerializer(payload_dict, SerializationContext(topic_str, messageField))
            elif type_str.lower() == "jsonschema":
                jSONSerializer = JSONSerializer(schema_str, self.schemaRegistryClient)
                payload_dict = payload_to_payload_dict(payload)
                payload_str_or_bytes = jSONSerializer(payload_dict, SerializationContext(topic_str, messageField))
            else:
                payload_str_or_bytes = payload
            return payload_str_or_bytes
        #
        key_str_or_bytes = serialize(key_bool=True)
        value_str_or_bytes = serialize(key_bool=False)
        #
        self.producer.produce(topic_str, value_str_or_bytes, key_str_or_bytes, partition=partition_int, timestamp=timestamp_int, headers=headers_dict_or_list, on_delivery=on_delivery)
        #
        self.produced_messages_counter_int += 1
        #
        return key_str_or_bytes, value_str_or_bytes

    def flush(self):
        """Wait for all messages in the Producer queue to be delivered.

        Wait for all messages in the Producer queue to be delivered. Uses the "flush.timeout" setting from the cluster configuration file ("kash"-section).
        """
        self.producer.flush(self.kash_dict["flush.timeout"])
        return self.topic_str
