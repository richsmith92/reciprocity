#!/bin/bash -ue

output="$1"
mapper="$2"
partition_key="$3"
sort_key="$4"
reducer="$5"
inputs=${@:6}

"map.sh" $output  $partition_key $mapper $inputs
"reduce.sh" $output $sort_key $reducer

parallel sort -c ::: $output/reduce/*
