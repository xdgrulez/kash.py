#!/bin/bash
if [ -z $1 ]
then
    coverage run -m unittest test_kash.TestAdminClient
else
    coverage run -m unittest test_kash.TestAdminClient.$1
fi
coverage html
