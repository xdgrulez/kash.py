from kashpy.functional import Functional

# Constants

ALL_MESSAGES = -1

#

class Shell(Functional):
    def cat(self, resource, n=ALL_MESSAGES, key_type="str", value_type="str"):
        def map_function(message_dict):
            return message_dict
        #
        return self.map(resource, map_function, n, key_type=key_type, value_type=value_type)

    def head(self, resource, n=10, key_type="str", value_type="str"):
        return self.cat(resource, n, key_type=key_type, value_type=value_type)
