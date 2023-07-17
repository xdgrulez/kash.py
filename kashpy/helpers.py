import json
import logging
import requests
from requests.adapters import HTTPAdapter, Retry
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

def create_session(retries_int):
    #logging.basicConfig(level=logging.DEBUG)
    session = requests.Session()
    retry = Retry(total=retries_int, backoff_factor=1, status_forcelist=[500, 502, 503, 504, 404], allowed_methods=None)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    #
    return session


def get(url_str, headers_dict, auth_str_tuple=None, retries=10):
    session = create_session(retries)
    response = session.get(url_str, headers=headers_dict, auth=auth_str_tuple)
    if is_json(response.text):
        response_dict = response.json()
    else:
        response_dict = {"response": response.text}
    #
    if response.ok:
        return response_dict
    else:
        raise Exception(response_dict)


def delete(url_str, headers_dict, auth_str_tuple=None, retries=10):
    session = create_session(retries)
    response = session.delete(url_str, headers=headers_dict, auth=auth_str_tuple)
    if is_json(response.text):
        response_dict = response.json()
    else:
        response_dict = {"response": response.text}
    #
    if response.ok:
        return response_dict
    else:
        raise Exception(response_dict)


def post(url_str, headers_dict, payload_dict_or_generator, auth_str_tuple=None, retries=10):
    session = create_session(retries)
    if isinstance(payload_dict_or_generator, dict):
        response = session.post(url_str, headers=headers_dict, json=payload_dict_or_generator, auth=auth_str_tuple)
    else:
        response = session.post(url_str, headers=headers_dict, data=payload_dict_or_generator, auth=auth_str_tuple)
    #
    if is_json(response.text):
        response_dict = response.json()
    else:
        response_text_list = "[" + response.text[:-2].replace("\r\n", ",") + "]"
        response_dict = json.loads(response_text_list)
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


def is_json(str):
    try:
        json.loads(str)
    except ValueError as e:
        return False
    return True


def is_pattern(str):
    return "*" in str or "?" in str or ("[" in str and "]" in str) or ("[!" in str and "]" in str)
