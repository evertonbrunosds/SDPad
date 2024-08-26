#!/bin/bash
self_current_path=$(pwd)
cd database
./run_test.sh "$1"
rm -rf "${self_current_path}/.mypy_cache"