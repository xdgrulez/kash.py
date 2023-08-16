#!/bin/bash
export KASHPY_HOME=".."
if [ -z $1 ]
then
    coverage run -m unittest test_filesystem_local.Test
else
    coverage run -m unittest test_filesystem_local.Test.$1
fi
coverage html