from abc import abstractmethod
import glob
import os
import re

from piny import YamlLoader


class Storage:
    def __init__(self, dir_str, mandatory_section_str_list, optional_section_str_list):
        self.dir_str = dir_str
        self.mandatory_section_str_list = mandatory_section_str_list
        self.optional_section_str_list = optional_section_str_list
        
    def get_config_dict(self, config_str):
        home_str = os.environ.get("KASHPY_HOME")
        if not home_str:
            home_str = "."
        #
        config_path_str = f"{home_str}/config/{self.dir_str}/{storage_str}"
        if os.path.exists(f"{storages_path_str}.yaml"):
            config_dict = YamlLoader(f"{storages_path_str}.yaml").load()
        elif os.path.exists(f"{storages_path_str}.yml"):
            config_dict = YamlLoader(f"{storages_path_str}.yml").load()
        else:
            raise Exception(f"No storage configuration file \"{storage_str}.yaml\" or \"{storage_str}.yml\" found in \"{storages_path_str}\" directory (hint: use KASHPY_HOME environment variable to set the kash.py home directory).")
        #
        for mandatory_section_str in self.mandatory_section_str_list:
            if mandatory_section_str not in config_dict:
                raise Exception(f"Connection configuration file \"{storage_str}.yaml\" does not include a \"{mandatory_section_str}\" section.")
        #
        for optional_section_str in self.optional_section_str_list:
            if optional_section_str not in config_dict:
                config_dict[optional_section_str] = {}
        #
        return config_dict

    def storages(self, pattern="*", config=False):
        pattern_str = pattern
        #
        home_str = os.environ.get("KASHPY_HOME")
        if not home_str:
            home_str = "."
        #
        storages_path_str = f"{home_str}/{self.dir_str}"
        yaml_storage_path_str_list = glob.glob(f"{storages_path_str}/{pattern_str}.yaml")
        yml_storage_path_str_list = glob.glob(f"{storages_path_str}/{pattern_str}.yml")
        #
        yaml_storage_str_list = [re.search(".*/(.*)\.yaml", yaml_storage_path_str).group(1) for yaml_storage_path_str in yaml_storage_path_str_list if re.search(".*/(.*)\.yaml", yaml_storage_path_str) is not None]
        yml_storage_str_list = [re.search(".*/(.*)\.yml", yml_storage_path_str).group(1) for yml_storage_path_str in yml_storage_path_str_list if re.search(".*/(.*)\.yml", yml_storage_path_str) is not None]
        #
        storage_str_list = yaml_storage_str_list + yml_storage_str_list
        #
        if config:
            storage_str_config_dict_dict = {storage_str: self.get_config_dict(storage_str) for storage_str in storage_str_list}
            return storage_str_config_dict_dict
        else:
            storage_str_list.sort()
            return storage_str_list

    # Read

    @abstractmethod
    def read():
        pass

    # Write

    @abstractmethod
    def write():
        pass
