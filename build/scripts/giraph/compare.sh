#!/bin/bash

dir1="$1"
dir2="$2"
diff "$dir1"/sorted.txt "$dir2"/sorted.txt > diff.txt

if [ -s diff.txt ]; then
        rm diff.txt
        echo "Not equivalent"
        exit 1
else
        rm diff.txt
        echo "Equivalent"
        exit 0
fi