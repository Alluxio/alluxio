#!/bin/bash
rm $2

for file in examples/*; do
    if [ -f "$file" ]; then
        sudo ./target/debug/examples/$(basename "$file" .rs) $1 >> $2
    fi
done
