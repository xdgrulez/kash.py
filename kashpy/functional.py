# Constants

ALL_MESSAGES = -1

#

class Functional:
    def foldl(self, resource, foldl_function, initial_acc, n=ALL_MESSAGES, break_function=lambda _, _1: False, key_type="str", value_type="str"):
        reader = self.openr(resource, key_type, value_type)
        #
        def foldl_function1(acc, message_dict):
            acc = foldl_function(acc, message_dict)
            #
            return acc
        #
        result = reader.foldl(foldl_function1, initial_acc, n, break_function)
        #
        reader.close()
        #
        return result

    #

    def flatmap(self, resource, flatmap_function, n=ALL_MESSAGES, break_function=lambda _, _1: False, key_type="str", value_type="str"):
        def foldl_function(list, message_dict):
            list += flatmap_function(message_dict)
            #
            return list
        #
        return self.foldl(resource, foldl_function, [], n, break_function, key_type, value_type)

    def map(self, resource, map_function, n=ALL_MESSAGES, break_function=lambda _, _1: False, key_type="str", value_type="str"):
        def flatmap_function(message_dict):
            return [map_function(message_dict)]
        #
        return self.flatmap(resource, flatmap_function, n, break_function, key_type, value_type)

    def filter(self, resource, filter_function, n=ALL_MESSAGES, break_function=lambda _, _1: False, key_type="str", value_type="str"):
        def flatmap_function(message_dict):
            return [message_dict] if filter_function(message_dict) else []
        #
        return self.flatmap(resource, flatmap_function, n, break_function, key_type, value_type)

    def foreach(self, resource, foreach_function, n=ALL_MESSAGES, break_function=lambda _, _1: False, key_type="str", value_type="str"):
        def foldl_function(_, message_dict):
            foreach_function(message_dict)
        #
        self.foldl(resource, foldl_function, None, n, break_function, key_type, value_type)
