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

def get(url_str, headers_dict):
    response = requests.get(url_str, headers=headers_dict)
    if response.text == "":
        response_dict = {}
    else:
        response_dict = response.json()
    #
    if response.ok:
        return response_dict
    else:
        raise Exception(response_dict)

def delete(url_str, headers_dict):
    response = requests.delete(url_str, headers=headers_dict)
    if response.text == "":
        response_dict = {}
    else:
        response_dict = response.json()
    #
    if response.ok:
        return response_dict
    else:
        raise Exception(response_dict)

def post(url_str, headers_dict, payload_dict):
    response = requests.post(url_str, headers=headers_dict, json=payload_dict)
    if response.text == "":
        response_dict = {}
    else:
        response_dict = response.json()
    #
    if response.ok:
        return response_dict
    else:
        raise Exception(response_dict)
