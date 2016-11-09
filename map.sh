#!/bin/bash

output="$1"
key="$2"
mapper=$3
inputs=${@:4}

map_dir=$output/map
ksort="sort --key=${key/-/,} --stable"

if [ -z $NM ]; then NM=$(nproc); fi
if [ -z $NR ]; then NR=$(nproc); fi

(rm -rf $map_dir && echo "Cleaned $map_dir")
mkdir -p $map_dir
seq $NR | parallel mkdir -p $map_dir/{}

if [ -z $inputs ]; then
  par() { parallel --pipe --nice=20 -j$NM $@ ;}
else
  par() { find $inputs -maxdepth 1 -type f | parallel --nice=20 -j$NM \< {} $@ ;}
fi

par $mapper \| tsvtool --key="$key" split --partition --buckets=$NR --out="$map_dir/{s}/{%}"
# par $mapper \| $ksort \| tsvtool --key="$key" split --out="$map_dir/{%}/{s}"

find $map_dir -empty -delete
