#!/bin/bash

output=$1
key=$2
reducer=$3

reduce_dir=$output/reduce
ksort="sort --key=${key/-/,} --stable"

rm -rf $reduce_dir/
mkdir -p $reduce_dir

ls $output/map/ | parallel --nice=20 $ksort $output/map/{}/* \| $reducer \> $reduce_dir/{}
