#!/bin/bash

output="$1"
export key="$2"
mapper=$3
inputs=${@:4}

export map_dir=$output/map
ksort="sort --key=${key/-/,} --stable"

if [ -z $NM ]; then export NM=$(nproc); fi
if [ -z $NR ]; then export NR=$(nproc); fi

rm -rf $map_dir
mkdir -p $map_dir
seq $NR | parallel mkdir -p $map_dir/{}


function par() {
  parallel --nice=20 -j$NM $@
}
function partition() {
  rp split -z --key=$key --partition --buckets=$NR --out="$map_dir/{s}/$1j$(printf %05d $2).gz"
}
export -f partition

if [ -z "$inputs" ]; then
  # put mapper output into {reducer id}/{job#}
  par --pipe $mapper \| partition \'\' {#}
else
  # enumerate files; put mapper output into {reducer id}/{file#}.{job#}
  find $inputs -maxdepth 1 -type f | awk '{printf("%05d\t%s\n",++i,$0)}' | par --eta -C'\t' \< {2} $mapper \| partition {1}. {#}
fi
find $map_dir -empty -delete
