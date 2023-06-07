import json
import requests
import sys
import time

def get_millis():
    return int(time.time()*1000)

def is_interactive():
    return hasattr(sys, 'ps1')


def is_file(str):
    return "/" in str


def pretty(dict):
    return json.dumps(dict, indent=2)


def ppretty(dict):
    print(pretty(dict))

#

def get(url_str, headers_dict, auth_str_tuple=None):
    response = requests.get(url_str, headers=headers_dict, auth=auth_str_tuple)
    if response.text == "":
        response_dict = {}
    else:
        response_dict = response.json()
    #
    if response.ok:
        return response_dict
    else:
        raise Exception(response_dict)

def delete(url_str, headers_dict, auth_str_tuple=None):
    response = requests.delete(url_str, headers=headers_dict, auth=auth_str_tuple)
    if response.text == "":
        response_dict = {}
    else:
        response_dict = response.json()
    #
    if response.ok:
        return response_dict
    else:
        raise Exception(response_dict)

def post(url_str, headers_dict, payload_dict, auth_str_tuple=None):
    response = requests.post(url_str, headers=headers_dict, json=payload_dict, auth=auth_str_tuple)
    if response.text == "":
        response_dict = {}
    else:
        response_dict = response.json()
    #
    if response.ok:
        return response_dict
    else:
        raise Exception(response_dict)


def get_auth_str_tuple(basic_auth_user_info):
    if basic_auth_user_info is None:
        auth_str_tuple = None
    else:
        auth_str_tuple = tuple(basic_auth_user_info.split(":"))
    #
    return auth_str_tuple

# filesystem -> *reader

def bytes_to_str(bytes):
    if bytes is None:
        str = None
    else:
        str = bytes.decode("utf-8")
    #
    return str

def bytes_to_dict(bytes):
    if bytes is None:
        dict = None
    else:
        dict = json.loads(bytes.decode("utf-8"))
    #
    return dict

def bytes_to_payload(key_or_value_bytes, type_str):
    if type_str == "bytes":
        key_or_value = key_or_value_bytes
    elif type_str == "str":
        key_or_value = bytes_to_str(key_or_value_bytes)
    elif type_str == "json" or type_str == "dict":
        key_or_value = bytes_to_dict(key_or_value_bytes)
    #
    return key_or_value

#

def split_key_value(message_bytes, key_value_separator_bytes):
    key_bytes = None
    value_bytes = b""
    #
    if message_bytes:
        if key_value_separator_bytes is not None:
            split_bytes_list = message_bytes.split(key_value_separator_bytes)
            if len(split_bytes_list) == 2:
                key_bytes = split_bytes_list[0]
                value_bytes = split_bytes_list[1]
            else:
                value_bytes = message_bytes
        else:
            value_bytes = message_bytes
    #
    return key_bytes, value_bytes 

# filesystem -> *writer

def str_to_bytes(str):
    if str is None:
        bytes = None
    else:
        bytes = str.encode("utf-8")
    #
    return bytes


def dict_to_bytes(dict):
    if dict is None:
        bytes = None
    else:
        bytes = str(dict).encode("utf-8")
    #
    return bytes

def payload_to_bytes(key_or_value, type_str):
    if type_str == "bytes":
        bytes = key_or_value
    elif type_str == "str":
        bytes = str_to_bytes(key_or_value)
    elif type_str == "json" or type_str == "dict":
        bytes = dict_to_bytes(key_or_value)
    #
    return bytes
