#!/bin/bash
coverage run -m unittest test_kash.TestAdminClient
coverage html
