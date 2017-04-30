#!/bin/bash

output="$1"
export key="$2"
mapper=$3
inputs=${@:4}

export map_dir=$output/map
ksort="sort --key=${key/-/,} --stable"

if [ -z $NM ]; then export NM=$(nproc); fi

rm -rf $map_dir
mkdir -p $map_dir


function par() {
  parallel --eta --nice=10 -j$NM $@
}
function split() {
  rp split -z --mkdir --key=$key --out="$map_dir/{s}/$1j$(printf %05d $2).gz"
}
export -f split

if [ -z "$inputs" ]; then
  # put mapper output into {reducer id}/{job#}
  par --pipe $mapper \| split \'\' {#}
else
  # enumerate files; put mapper output into {reducer id}/{file#}.{job#}
  find $inputs -maxdepth 1 -type f | awk '{printf("%05d\t%s\n",++i,$0)}' | par -C'\t' \< {2} $mapper \| split {1}. {#}
fi
find $map_dir -empty -delete
