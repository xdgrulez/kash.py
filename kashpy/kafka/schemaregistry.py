from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

class SchemaRegistry:
    def __init__(self, schema_registry_config_dict, kash_config_dict):
        self.schema_registry_config_dict = schema_registry_config_dict
        self.kash_config_dict = kash_config_dict
        #
        if self.schema_registry_config_dict is not None:
            self.schemaRegistryClient = self.get_schemaRegistryClient()
        else:
            self.schemaRegistryClient = None

    def get_schemaRegistryClient(self):
        dict = {}
        #
        dict["url"] = self.schema_registry_config_dict["schema.registry.url"]
        if "basic.auth.user.info" in self.schema_registry_config_dict:
            dict["basic.auth.user.info"] = self.schema_registry_config_dict["basic.auth.user.info"]
        #
        schemaRegistryClient = SchemaRegistryClient(dict)
        return schemaRegistryClient

    def get_schema(self, schema_id):
        # No additional caching necessary here:
        # get_schema(schema_id)[source]
        # Fetches the schema associated with schema_id from the Schema Registry. The result is cached so subsequent attempts will not require an additional round-trip to the Schema Registry.
        schema_id_int = schema_id
        #
        schema = self.schemaRegistryClient.get_schema(schema_id_int)
        schema_dict = schema_to_schema_dict(schema)
        #
        return schema_dict

    def create_schema_dict(self, schema_str, schema_type_str):
        schema = Schema(schema_str, schema_type_str) # TODO: support references
        schema_dict = schema_to_schema_dict(schema)
        #
        return schema_dict

    def create_subject_name_str(self, topic_str, key_bool):
        key_or_value_str = "key" if key_bool else "value"
        #
        subject_name_str = f"{topic_str}-{key_or_value_str}"
        #
        return subject_name_str

    def register_schema(self, subject_name, schema, normalize=False):
        subject_name_str = subject_name
        schema_dict = schema
        normalize_bool = normalize
        #
        schema1 = Schema(schema_dict["schema_str"], schema_dict["schema_type"]) # TODO: support references
        #
        schema_id_int = self.schemaRegistryClient.register_schema(subject_name_str, schema1, normalize_schemas=normalize_bool)
        return schema_id_int

    def lookup_schema(self, subject_name, schema, normalize_schemas=False):
        subject_name_str = subject_name
        schema_dict = schema
        normalize_schemas_bool = normalize_schemas
        #
        schema1 = Schema(schema_dict["schema_str"], schema_dict["schema_type"]) # TODO: support references
        #
        registeredSchema = self.schemaRegistryClient.lookup_schema(subject_name_str, schema1, normalize_schemas=normalize_schemas_bool)
        registeredSchema_dict = registeredSchema_to_registeredSchema_dict(registeredSchema)
        #
        return registeredSchema_dict

    def get_subjects(self):
        subject_name_str_list = self.schemaRegistryClient.get_subjects()
        #
        return subject_name_str_list

    def delete_subject(self, subject_name, permanent=False):
        subject_name_str = subject_name
        permanent_bool = permanent
        #
        schema_id_int_list = self.schemaRegistryClient.delete_subject(subject_name_str, permanent_bool)
        #
        return schema_id_int_list

    def get_latest_version(self, subject_name):
        subject_name_str = subject_name
        #
        registeredSchema = self.schemaRegistryClient.get_latest_version(subject_name_str)
        registeredSchema_dict = registeredSchema_to_registeredSchema_dict(registeredSchema)
        #
        return registeredSchema_dict

    def get_version(self, subject_name, version):
        subject_name_str = subject_name
        version_int = version
        #
        registeredSchema = self.schemaRegistryClient.get_version(subject_name_str, version_int)
        registeredSchema_dict = registeredSchema_to_registeredSchema_dict(registeredSchema)
        #
        return registeredSchema_dict

    def get_versions(self, subject_name):
        subject_name_str = subject_name
        #
        schema_id_int_list = self.schemaRegistryClient.get_versions(subject_name_str)
        #
        return schema_id_int_list

    def delete_version(self, subject_name, version):
        subject_name_str = subject_name
        version_int = version
        #
        schema_id_int = self.schemaRegistryClient.delete_version(subject_name_str, version_int)
        #
        return schema_id_int

    def set_compatibility(self, subject_name, level):
        subject_name_str = subject_name
        level_str = level
        #
        set_level_str = self.schemaRegistryClient.set_compatibility(subject_name_str, level_str)
        #
        return set_level_str

    def get_compatibility(self, subject_name):
        subject_name_str = subject_name
        #
        level_str = self.schemaRegistryClient.get_compatibility(subject_name_str)
        #
        return level_str

    def test_compatibility(self, subject_name, schema, version="latest"):
        subject_name_str = subject_name
        schema_dict = schema
        version_str = version
        #
        schema1 = Schema(schema_dict["schema_str"], schema_dict["schema_type"]) # TODO: support references
        #
        compatible_bool = self.schemaRegistryClient.test_compatibility(subject_name_str, schema1, version_str)
        #
        return compatible_bool

#

def registeredSchema_to_registeredSchema_dict(registeredSchema):
    registeredSchema_dict = {"schema_id": registeredSchema.schema_id,
                             "schema": schema_to_schema_dict(registeredSchema.schema),
                             "subject": registeredSchema.subject,
                             "version": registeredSchema.version}
    #
    return registeredSchema_dict

def schema_to_schema_dict(schema):
    schema_dict = {"schema_str": schema.schema_str,
                   "schema_type": schema.schema_type} # TODO: support references
    #
    return schema_dict
