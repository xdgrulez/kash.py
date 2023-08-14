import time

# Constants

ALL_MESSAGES = -1

#

class Functional:
    def foldl(self, resource, foldl_function, initial_acc, n=ALL_MESSAGES, **kwargs):
        verbose_int = self.verbose()
        progress_num_messages_int = self.progress_num_messages()
        #
        reader = self.openr(resource, **kwargs)
        #
        def foldl_function1(acc_progress_message_counter_int_tuple, message_dict):
            (acc, progress_message_counter_int) = acc_progress_message_counter_int_tuple
            #
            acc = foldl_function(acc, message_dict)
            #
            progress_message_counter_int += 1
            if verbose_int > 0 and progress_message_counter_int % progress_num_messages_int == 0:
                print(f"Read: {progress_message_counter_int}")
            #
            return (acc, progress_message_counter_int)
        #
        result_progress_message_counter_int_tuple = reader.foldl(foldl_function1, (initial_acc, 0), n, **kwargs)
        #
        reader.close()
        #
        return result_progress_message_counter_int_tuple

    #

    def flatmap(self, resource, flatmap_function, n=ALL_MESSAGES, **kwargs):
        def foldl_function(list, message_dict):
            list += flatmap_function(message_dict)
            #
            return list
        #
        return self.foldl(resource, foldl_function, [], n, **kwargs)

    def map(self, resource, map_function, n=ALL_MESSAGES, **kwargs):
        def flatmap_function(message_dict):
            return [map_function(message_dict)]
        #
        return self.flatmap(resource, flatmap_function, n, **kwargs)

    def filter(self, resource, filter_function, n=ALL_MESSAGES, **kwargs):
        def flatmap_function(message_dict):
            return [message_dict] if filter_function(message_dict) else []
        #
        return self.flatmap(resource, flatmap_function, n, **kwargs)

    def foreach(self, resource, foreach_function, n=ALL_MESSAGES, **kwargs):
        def foldl_function(_, message_dict):
            foreach_function(message_dict)
        #
        self.foldl(resource, foldl_function, None, n, **kwargs)

    #

    def flatmap_to(self, resource, target_storage, target_resource, flatmap_function, n=ALL_MESSAGES, **kwargs):
        progress_num_messages_int = self.progress_num_messages()
        verbose_int = self.verbose()
        #
        def write_batch(batch_message_dict_list):
            value_list = [message_dict["value"] for message_dict in batch_message_dict_list]
            #
            key_list = [message_dict["key"] for message_dict in batch_message_dict_list]
            if "preserve_partition" in kwargs and kwargs["preserve_partition"] == True:
                partition_list = [message_dict["partition"] for message_dict in batch_message_dict_list]
            else:
                partition_list = None
            if "preserve_timestamp" in kwargs and kwargs["preserve_timestamp"] == True:
                timestamp_list = [message_dict["timestamp"][1] for message_dict in batch_message_dict_list]
            else:
                timestamp_list = None
            headers_list = [message_dict["headers"] for message_dict in batch_message_dict_list]
            target_writer.write(value_list, key=key_list, partition=partition_list, timestamp=timestamp_list, headers=headers_list)
        #

        def foldl_function(write_batch_size_int_batch_message_dict_list_progress_message_counter_int_tuple, message_dict):
            (write_batch_size_int, batch_message_dict_list, progress_message_counter_int) = write_batch_size_int_batch_message_dict_list_progress_message_counter_int_tuple
            #
            message_dict_list = flatmap_function(message_dict)
            #
            batch_message_dict_list += message_dict_list
            #
            if len(batch_message_dict_list) >= write_batch_size_int:
                write_batch(batch_message_dict_list)
                #
                progress_message_counter_int += len(batch_message_dict_list)
                if verbose_int > 0 and progress_message_counter_int % progress_num_messages_int == 0:
                    print(f"Written: {progress_message_counter_int}")
                #
                return (write_batch_size_int, [], progress_message_counter_int)
            else:
                return (write_batch_size_int, batch_message_dict_list, progress_message_counter_int)

        source_kwargs = kwargs.copy()
        if "source_key_type" in kwargs:
            source_kwargs["key_type"] = kwargs["source_key_type"]
        if "source_value_type" in kwargs:
            source_kwargs["value_type"] = kwargs["source_value_type"]
        if "source_type" in kwargs:
            source_kwargs["type"] = kwargs["source_type"]
        if "source_key_schema" in kwargs:
            source_kwargs["key_schema"] = kwargs["source_key_schema"]
        if "source_value_schema" in kwargs:
            source_kwargs["value_schema"] = kwargs["source_value_schema"]
        if "source_key_schema_id" in kwargs:
            source_kwargs["key_schema_id"] = kwargs["source_key_schema_id"]
        if "source_value_schema_id" in kwargs:
            source_kwargs["value_schema_id"] = kwargs["source_value_schema_id"]
        #
        target_kwargs = kwargs.copy()
        if "target_key_type" in kwargs:
            target_kwargs["key_type"] = kwargs["target_key_type"]
        if "target_value_type" in kwargs:
            target_kwargs["value_type"] = kwargs["target_value_type"]
        if "target_type" in kwargs:
            target_kwargs["type"] = kwargs["target_type"]
        if "target_key_schema" in kwargs:
            target_kwargs["key_schema"] = kwargs["target_key_schema"]
        if "target_value_schema" in kwargs:
            target_kwargs["value_schema"] = kwargs["target_value_schema"]
        if "target_key_schema_id" in kwargs:
            target_kwargs["key_schema_id"] = kwargs["target_key_schema_id"]
        if "target_value_schema_id" in kwargs:
            target_kwargs["value_schema_id"] = kwargs["target_value_schema_id"]
        #
        write_batch_size_int = kwargs["write_batch_size"] if "write_batch_size" in kwargs else target_storage.write_batch_size()
        #
        target_writer = target_storage.openw(target_resource, **target_kwargs)
        #
        (write_batch_size_int_batch_message_dict_list_progress_message_counter_int_tuple, read_message_counter_int) = self.foldl(resource, foldl_function, (write_batch_size_int, [], 0), n, **source_kwargs)
        (_, batch_message_dict_list, written_progress_message_counter_int) = write_batch_size_int_batch_message_dict_list_progress_message_counter_int_tuple
        #
        if len(batch_message_dict_list) > 0:
            write_batch(batch_message_dict_list)
            written_progress_message_counter_int += len(batch_message_dict_list)
        #
        target_writer.close()
        #
        return (read_message_counter_int, written_progress_message_counter_int)

    def map_to(self, resource, target_storage, target_resource, map_function, n=ALL_MESSAGES, **kwargs):
        def flatmap_function(message_dict):
            return [map_function(message_dict)]
        #
        return self.flatmap_to(resource, target_storage, target_resource, flatmap_function, n, **kwargs)

    def filter_to(self, resource, target_storage, target_resource, filter_function, n=ALL_MESSAGES, **kwargs):
        def flatmap_function(message_dict):
            return [message_dict] if filter_function(message_dict) else []
        #
        return self.flatmap_to(resource, target_storage, target_resource, flatmap_function, n, **kwargs)

    #

    def zip_foldl(self, resource1, storage2, resource2, zip_foldl_function, initial_acc, n=ALL_MESSAGES, **kwargs):
        n_int = n
        #
        read_batch_size_int = kwargs["read_batch_size"] if "read_batch_size" in kwargs else self.read_batch_size()
        if read_batch_size_int > n_int:
            read_batch_size_int = n_int
        #
        break_function = kwargs["break_function"] if "break_function" in kwargs else lambda _, _1: False
        #
        kwargs1 = kwargs.copy()
        kwargs1["group"] = kwargs1["group1"] if "group1" in kwargs1 else None
        kwargs1["offsets"] = kwargs1["offsets1"] if "offsets1" in kwargs1 else None
        kwargs1["key_type"] = kwargs1["key_type1"] if "key_type1" in kwargs1 else "bytes"
        kwargs1["value_type"] = kwargs1["value_type1"] if "value_type1" in kwargs1 else "bytes"
        kwargs1["type"] = kwargs1["type1"] if "type1" in kwargs1 else "bytes"
        #
        kwargs2 = kwargs.copy()
        kwargs2["group"] = kwargs2["group2"] if "group2" in kwargs2 else None
        kwargs2["offsets"] = kwargs2["offsets2"] if "offsets2" in kwargs2 else None
        kwargs2["key_type"] = kwargs2["key_type2"] if "key_type2" in kwargs2 else "bytes"
        kwargs2["value_type"] = kwargs2["value_type2"] if "value_type2" in kwargs2 else "bytes"
        kwargs2["type"] = kwargs2["type2"] if "type2" in kwargs2 else "bytes"
        #
        reader1 = self.openr(resource1, **kwargs1)
        reader2 = storage2.openr(resource2, **kwargs2)
        #
        message_counter_int1 = 0
        message_counter_int2 = 0
        acc = initial_acc
        break_bool = False
        while True:
            message_dict_list1 = []
            while True:
                message_dict_list1 += reader1.read(n=read_batch_size_int)
                if not message_dict_list1 or read_batch_size_int == ALL_MESSAGES or len(message_dict_list1) == read_batch_size_int:
                    break
            if not message_dict_list1:
                break
            num_messages_int1 = len(message_dict_list1)
            message_counter_int1 += num_messages_int1
            if self.verbose() > 0 and message_counter_int1 % self.progress_num_messages() == 0:
                print(f"Read (storage 1): {message_counter_int1}")
            #
            read_batch_size_int2 = num_messages_int1 if num_messages_int1 < read_batch_size_int else read_batch_size_int
            message_dict_list2 = []
            while True:
                message_dict_list2 += reader2.read(n=read_batch_size_int2)
                if not message_dict_list2 or read_batch_size_int2 == ALL_MESSAGES or len(message_dict_list2) == read_batch_size_int2:
                    break
            if not message_dict_list2:
                break
            num_messages_int2 = len(message_dict_list2)
            message_counter_int2 += num_messages_int2
            if self.verbose() > 0 and message_counter_int2 % self.progress_num_messages() == 0:
                print(f"Read (storage 2): {message_counter_int2}")
            #
            if num_messages_int1 != num_messages_int2:
                break
            #
            for message_dict1, message_dict2 in zip(message_dict_list1, message_dict_list2):
                if break_function(message_dict1, message_dict2):
                    break_bool = True
                    break
                acc = zip_foldl_function(acc, message_dict1, message_dict2)
            #
            if break_bool:
                break
            #
            if n_int != ALL_MESSAGES:
                if message_counter_int1 >= n_int:
                    break
        #
        reader1.close()
        reader2.close()
        return acc, message_counter_int1, message_counter_int2
