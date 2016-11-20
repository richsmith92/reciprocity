#!/bin/bash

N=1000000
dir=/dev/shm/rp
mkdir -p "$dir"

paste <(seq -w 0 $N) <(seq -w 0 $N) <(seq -w 0 $N) > $dir/x
sed -n '1~3p' < $dir/x > $dir/x1

export TIMEFORMAT="%U+%S"
export LC_LANG=C

paste <(seq -w 0 $N) <(seq -w 0 $N) > $dir/x
sed -n '1~3p' < $dir/x > $dir/x1
echo -ne "join:    "; (time join -t$'\t' -j1 --nocheck-order $dir/{x,x1} > $dir/j1) 2> >(bc)
echo -ne "rp join: "; (time rp join --key=1 --val=2 $dir/{x,x1} > $dir/j2) 2> >(bc)
mismatches=$(comm -3 $dir/{j1,j2} | wc -l)
(( $mismatches == 0 )) || echo "Found $mismatches mismatches"

echo -ne "join:    "; (time join -t$'\t' -j2 -o 1.2 1.3 2.3 --nocheck-order $dir/{x,x1} > $dir/j1) 2> >(bc)
echo -ne "rp join: "; (time rp join --key=1 --val=3 $dir/{x,x1} > $dir/j2) 2> >(bc)
mismatches=$(comm -3 $dir/{j1,j2} | wc -l)
(( $mismatches == 0 )) || echo "Found $mismatches mismatches"

#
# echo -ne "join -a1:        "; (time join -t$'\t' -j1 --nocheck-order -a1 -o1.1,1.2,2.2 $dir/{x,x1} > $dir/j1) 2> >(bc)
# echo -ne "rp join -1: "; (time rp join --key=1 --val=2 -1 $dir/{x,x1} > $dir/j2) 2> >(bc)
# mismatches=$(comm -3 $dir/{j1,j2} | wc -l)
# (( $mismatches == 0 )) || echo "Found $mismatches mismatches in: comm -3 $dir/{j1,j2}"

# rm -r "$dir"
