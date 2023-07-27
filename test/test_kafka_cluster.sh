#!/bin/bash
if [ -z $1 ]
then
    coverage run -m unittest test_kafka_cluster.Test
else
    coverage run -m unittest test_kafka_cluster.Test.$1
fi
coverage html
