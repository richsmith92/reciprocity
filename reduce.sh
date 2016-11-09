#!/bin/bash

output=$1
key=$2
reducer=$3

reduce_dir=$output/reduce
ksort="sort --key=${key/-/,} --stable"

if [ -z $NM ]; then NM=$(nproc); fi
if [ -z $NR ]; then NR=$(nproc); fi

(mkdir -p $reduce_dir; rm -rf $reduce_dir/* && echo "Cleaned $reduce_dir")

time ls $output/map/ | \
  parallel --nice=20 $ksort $output/map/{}/* \| $reducer \| sort -s \> $reduce_dir/{}
