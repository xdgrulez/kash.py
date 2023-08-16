#!/bin/bash
export KASHPY_HOME=".."
if [ -z $1 ]
then
    coverage run -m unittest test_fs_local.Test
else
    coverage run -m unittest test_fs_local.Test.$1
fi
coverage html
