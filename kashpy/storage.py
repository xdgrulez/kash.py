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
        self.verbose_int = 1 if is_interactive() else 0
        
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

    #

    def verbose(self, new_value=None): # int
        if new_value is not None:
            self.verbose_int = new_value
        return self.verbose_int

