class StorageReader():
    def __init__(self, storage_obj, *resources, **kwargs):
        self.storage_obj = storage_obj
        #
        self.resource_str_list = list(resources)
        #
        self.resource_str_offsets_dict_dict = self.get_resource_str_offsets_dict_dict(self.resource_str_list, **kwargs)
        #
        (self.key_type_str, self.value_type_str) = self.storage_obj.get_key_value_type_tuple(**kwargs)
        #
        (self.key_type_dict, self.value_type_dict) = self.get_key_value_type_dict_tuple(self.key_type_str, self.value_type_str, self.resource_str_list)
    
    #

    def get_resource_str_offsets_dict_dict(self, resource_str_list, **kwargs):
        if "offsets" in kwargs and kwargs["offsets"] is not None:
            offsets_dict = kwargs["offsets"]
            str_or_int = list(offsets_dict.keys())[0]
            if isinstance(str_or_int, int):
                resource_str_offsets_dict_dict = {resource_str: offsets_dict for resource_str in resource_str_list}
            else:
                resource_str_offsets_dict_dict = offsets_dict
        elif "offset" in kwargs and kwargs["offset"] is not None:
            resource_str_offsets_dict_dict = {resource_str: {0: kwargs["offset"]} for resource_str in resource_str_list}
        else:
            resource_str_offsets_dict_dict = None
        #
        if resource_str_offsets_dict_dict is not None:
            offset_int_list = sum({resource_str: list(offsets_dict.values()) for resource_str, offsets_dict in resource_str_offsets_dict_dict.items()}.values(), [])
            if any(offset_int < 0 for offset_int in offset_int_list):
                resource_str_partition_int_size_int_dict_dict = self.storage_obj.ls(resource_str_list, partitions=True)
                resource_str_offsets_dict_dict = {resource_str: {partition_int: (resource_str_partition_int_size_int_dict_dict[resource_str][partition_int] + offset_int if offset_int < 0 else offset_int) for partition_int, offset_int in offsets_dict.items()} for resource_str, offsets_dict in resource_str_offsets_dict_dict.items()}
        #
        return resource_str_offsets_dict_dict

    def get_key_value_type_dict_tuple(self, key_type, value_type, resource_str_list):
        if isinstance(key_type, dict):
            key_type_dict = key_type
        else:
            key_type_dict = {resource_str: key_type for resource_str in resource_str_list}
        if isinstance(value_type, dict):
            value_type_dict = value_type
        else:
            value_type_dict = {resource_str: value_type for resource_str in resource_str_list}
        #
        return (key_type_dict, value_type_dict)
