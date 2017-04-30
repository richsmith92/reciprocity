#!/bin/bash

output=$1
export key=$2
reducer=$3

if [[ -z $NR ]]; then export NR=$(nproc); fi
if [[ -z $GZIP_ARGS ]]; then export GZIP_ARGS=-1; fi

export sort_key="${key/-/,}"
reduce_dir="$output/reduce"

rm -rf $reduce_dir/
mkdir -p $reduce_dir

ls $output/map/ | parallel "-j$NR" --eta --nice=20 zsort -msk"$sort_key" "$output"/map/{}/* \| $reducer \| gzip "$GZIP_ARGS" \> $reduce_dir/{}.gz
