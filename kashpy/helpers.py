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



