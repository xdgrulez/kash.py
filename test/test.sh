#!/bin/bash
if [ -z $1 ]
then
    coverage run -m unittest test_kash.Test
else
    coverage run -m unittest test_kash.Test.$1
fi
coverage html
