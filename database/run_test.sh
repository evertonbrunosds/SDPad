#!/bin/bash
./build.sh TST/util/assembled.sql
run_test() {
    local test_file="$1"
    local test_dir
    test_dir=$(dirname "$test_file")
    (cd "$test_dir" && python3 -m pytest "$(basename "$test_file")")
}
if [ -n "$1" ]; then
    if [ -f "$1" ]; then
        run_test "$1"
    else
        echo "Error: File $1 does not exist."
        exit 1
    fi
else
    for file in TST/*.py; do
        if [[ "$(basename "$file")" != "development_test_tool.py" ]]; then
            run_test "$file"
        fi
    done
fi
if [ -f TST/util/assembled.sql ]; then
    rm TST/util/assembled.sql
fi
if [ -d TST/__pycache__/ ]; then
    rm -rf TST/__pycache__/
fi
if [ -d TST/util/__pycache__/ ]; then
    rm -rf TST/util/__pycache__/
fi
if [ -d TST/.pytest_cache/ ]; then
    rm -rf TST/.pytest_cache/
fi
