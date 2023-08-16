import base64
import binascii
import json
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


def get(url_str, headers_dict, payload_dict=None, auth_str_tuple=None, retries=0):
    session = create_session(retries)
    if payload_dict is None:
        response = session.get(url_str, headers=headers_dict, auth=auth_str_tuple)
    else:
        response = session.get(url_str, headers=headers_dict, json=payload_dict, auth=auth_str_tuple)
    #
    if is_json(response.text):
        response_dict = response.json()
    else:
        response_dict = {"response": response.text}
    #
    if "error_code" in response_dict and response_dict["error_code"] > 400:
        raise Exception(response_dict["message"])
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
    if "error_code" in response_dict and response_dict["error_code"] > 400:
        raise Exception(response_dict["message"])
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
        #
        if "error_code" in response_dict and response_dict["error_code"] > 400:
            raise Exception(response_dict["message"])
        #
        if response.ok:
            return response_dict
        else:
            raise Exception(response_dict)
    else:
        response_text_list = "[" + response.text[:-2].replace("\r\n", ",") + "]"
        response_dict_list = json.loads(response_text_list)
        #
        for response_dict in response_dict_list:
            if "error_code" in response_dict and response_dict["error_code"] > 400:
                raise Exception(response_dict["message"])
        #
        if response.ok:
            return response_dict_list
        else:
            raise Exception(response_dict_list)


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


def is_base64_encoded(str_or_bytes_or_dict):
    try:
        if isinstance(str_or_bytes_or_dict, bytes):
            decoded_bytes = base64.b64decode(str_or_bytes_or_dict)
        elif isinstance(str_or_bytes_or_dict, str):
            decoded_bytes = base64.b64decode(bytes(str_or_bytes_or_dict, encoding="utf-8"))
        elif isinstance(str_or_bytes_or_dict, dict):
            decoded_bytes = base64.b64decode(bytes(json.dumps(str_or_bytes_or_dict), encoding="utf-8"))
        else:
            return False
        encoded_bytes = base64.b64encode(decoded_bytes)
        return str_or_bytes_or_dict == encoded_bytes
    except (binascii.Error, UnicodeDecodeError):
        return False


def base64_encode(str_or_bytes_or_dict):
    if isinstance(str_or_bytes_or_dict, bytes):
        encoded_bytes = base64.b64encode(str_or_bytes_or_dict)
    elif isinstance(str_or_bytes_or_dict, str):
        encoded_bytes = base64.b64encode(bytes(str_or_bytes_or_dict, encoding="utf-8"))
    elif isinstance(str_or_bytes_or_dict, dict):
        encoded_bytes = base64.b64encode(bytes(json.dumps(str_or_bytes_or_dict), encoding="utf-8"))
    return encoded_bytes


def base64_decode(base64_str):
    return base64.b64decode(bytes(base64_str, encoding="utf-8"))


def bytes_or_str_to_bytes(bytes_or_str):
    if isinstance(bytes_or_str, bytes):
        return_bytes = bytes_or_str
    elif isinstance(bytes_or_str, str):
        return_bytes = bytes(bytes_or_str, encoding="utf-8")
    #
    return return_bytes


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


# def find_file_offset_of_message(path_file_str, message_int, message_separator="\n"):
#     file_offset_int = 0
#     #



#         for _ in range(line_int):
#             line_str = bufferedReader.readline()
#             if not line_str:
#                 break
#             file_offset_int += len(line_str)
#     #
#     return file_offset_int

