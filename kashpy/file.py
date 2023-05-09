from kashpy.storage import Storage

#

class File(Storage):
    def __init__(self, dir_str, config_str, mandatory_section_str_list, optional_section_str_list):
        super().__init__(dir_str, config_str, mandatory_section_str_list, optional_section_str_list)
        #
        self.dir_str = dir_str
        self.config_str = config_str
        #
        self.kash_config_dict = self.config_dict["kash"]
        #
        if "progress.num.messages" not in self.kash_config_dict:
            self.progress_num_messages(1000)
        else:
            self.progress_num_messages(int(self.kash_config_dict["progress.num.messages"]))

    #

    def get_set_config(self, config_key_str, new_value=None):
        if new_value is not None:
            self.kash_config_dict[config_key_str] = new_value
        #
        return new_value

    def progress_num_messages(self, new_value=None): # int
        return self.get_set_config("progress.num.messages", new_value)

    # Open

    def open(self, file, mode):
        handle = self.open(file, mode)
        #
        print(file)
        #
        return handle
