from confluent_kafka.schema_registry import SchemaRegistryClient

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
