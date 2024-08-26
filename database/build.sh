#!/bin/bash
output_file="${1:-assembled.sql}"
if [ -f "$output_file" ]; then
    rm "$output_file"
fi
file_names=("imports.sql" "database.sql" "indexes.sql")
for file in "${file_names[@]}"; do
    input_file="SQL/$file"
    if [ -f "$input_file" ]; then
        echo "/* ---> $file <--- */" >> "$output_file"
        cat "$input_file" >> "$output_file"
        echo >> "$output_file"
    else
        echo "Warning: $input_file does not exist."
    fi
done
