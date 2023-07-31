import glob
import os
import re

from piny import YamlLoader

from kashpy.shell import Shell
from kashpy.helpers import is_interactive


class Storage(Shell):
    def __init__(self, dir_str, config_str, mandatory_section_str_list, optional_section_str_list):
        self.dir_str = dir_str
        self.config_str = config_str
        self.mandatory_section_str_list = mandatory_section_str_list
        self.optional_section_str_list = optional_section_str_list
        #
        self.config_dict = self.get_config_dict()
        #
        self.kash_config_dict = self.config_dict["kash"] if "kash" in self.config_dict else {}
        #
        if "progress.num.messages" not in self.kash_config_dict:
            self.progress_num_messages(1000)
        else:
            self.progress_num_messages(int(self.kash_config_dict["progress.num.messages"]))
        #
        if "read.batch.size" not in self.kash_config_dict:
            self.read_batch_size(1000)
        else:
            self.read_batch_size(int(self.kash_config_dict["read.batch.size"]))
        #
        if "write.batch.size" not in self.kash_config_dict:
            self.write_batch_size(1000)
        else:
            self.write_batch_size(int(self.kash_config_dict["write.batch.size"]))
        #
        if "verbose" not in self.kash_config_dict:
            verbose_int = 1 if is_interactive() else 0
            self.verbose(verbose_int)
        else:
            self.verbose(int(self.kash_config_dict["verbose"]))

    #

    def progress_num_messages(self, new_value=None): # int
        return self.get_set_config("progress.num.messages", new_value)

    def read_batch_size(self, new_value=None): # int
        return self.get_set_config("read.batch.size", new_value)
    
    def write_batch_size(self, new_value=None): # int
        return self.get_set_config("write.batch.size", new_value)

    def verbose(self, new_value=None): # int
        return self.get_set_config("verbose", new_value)

    #

    def get_set_config(self, config_key_str, new_value=None, dict=None):
        dict = self.kash_config_dict if dict is None else dict
        #
        if new_value is not None:
            dict[config_key_str] = new_value
        #
        return dict[config_key_str]

    #

    def get_config_dict(self):
        home_str = os.environ.get("KASHPY_HOME")
        if not home_str:
            home_str = "."
        #
        configs_path_str = f"{home_str}/configs/{self.dir_str}"
        if os.path.exists(f"{configs_path_str}/{self.config_str}.yaml"):
            config_dict = YamlLoader(f"{configs_path_str}/{self.config_str}.yaml").load()
        elif os.path.exists(f"{configs_path_str}/{self.config_str}.yml"):
            config_dict = YamlLoader(f"{configs_path_str}/{self.config_str}.yml").load()
        else:
            raise Exception(f"No configuration file \"{self.config_str}.yaml\" or \"{self.config_str}.yml\" found in \"{configs_path_str}\" directory (hint: use KASHPY_HOME environment variable to set the kash.py home directory).")
        #
        for mandatory_section_str in self.mandatory_section_str_list:
            if mandatory_section_str not in config_dict:
                raise Exception(f"Connection configuration file \"{self.config_str}.yaml\" does not include a \"{mandatory_section_str}\" section.")
        #
        for optional_section_str in self.optional_section_str_list:
            if optional_section_str not in config_dict:
                config_dict[optional_section_str] = {}
        #
        return config_dict

    def configs(self, pattern="*", verbose=False):
        pattern_str = pattern
        verbose_bool = verbose
        #
        home_str = os.environ.get("KASHPY_HOME")
        if not home_str:
            home_str = "."
        #
        configs_path_str = f"{home_str}/configs/{self.dir_str}"
        yaml_config_path_str_list = glob.glob(f"{configs_path_str}/{pattern_str}.yaml")
        yml_config_path_str_list = glob.glob(f"{configs_path_str}/{pattern_str}.yml")
        #
        yaml_config_str_list = [re.search(f"{configs_path_str}/(.*)\.yaml", yaml_config_path_str).group(1) for yaml_config_path_str in yaml_config_path_str_list if re.search(".*/(.*)\.yaml", yaml_config_path_str) is not None]
        yml_config_str_list = [re.search(".*/(.*)\.yml", yml_config_path_str).group(1) for yml_config_path_str in yml_config_path_str_list if re.search(".*/(.*)\.yml", yml_config_path_str) is not None]
        #
        config_str_list = yaml_config_str_list + yml_config_str_list
        #
        if verbose_bool:
            config_str_config_dict_dict = {config_str: self.get_config_dict(config_str) for config_str in config_str_list}
            return config_str_config_dict_dict
        else:
            config_str_list.sort()
            return config_str_list
