#!/bin/bash
export KASHPY_HOME=".."
if [ -z $1 ]
then
    coverage run -m unittest test_kafka_restproxy.Test
else
    coverage run -m unittest test_kafka_restproxy.Test.$1
fi
coverage html
