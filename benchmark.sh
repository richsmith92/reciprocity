#!/bin/bash

N=1000000
dir=/dev/shm/tsvtool
mkdir -p "$dir"

paste <(seq -w 0 $N) <(seq -w 0 $N) > $dir/x
# seq -w 0 $N > $dir/x
sed -n '1~3p' < $dir/x > $dir/x1
# sed '1~3d' < $dir/x > $dir/x2

export TIMEFORMAT="%U+%S"
export LC_LANG=C

echo -ne "join:         "; (time join -t$'\t' -j1 --nocheck-order $dir/{x,x1} > $dir/j1) 2> >(bc)
echo -ne "tsvtool join: "; (time tsvtool --key=1 join --val=2 $dir/{x,x1} > $dir/j2) 2> >(bc)
mismatches=$(comm -3 $dir/{j1,j2} | wc -l)
(( $mismatches == 0 )) || echo "Found $mismatches mismatches"

echo -ne "join -a1:        "; (time join -t$'\t' -j1 --nocheck-order -a1 -o1.1,1.2,2.2 $dir/{x,x1} > $dir/j1) 2> >(bc)
echo -ne "tsvtool join -1: "; (time tsvtool --key=1 join --val=2 -1 $dir/{x,x1} > $dir/j2) 2> >(bc)
mismatches=$(comm -3 $dir/{j1,j2} | wc -l)
(( $mismatches == 0 )) || echo "Found $mismatches mismatches in: comm -3 $dir/{j1,j2}"

# rm -r "$dir"
