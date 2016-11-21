#!/bin/bash

N=100000
seqN(){ seq -w 0 $N ;}
seqA(){ seqN | tr 0-9 a-z ;}

paste <(seqN) <(seqA) > test/x
sed -n '1~3p' < test/x > test/x1

dir=/dev/shm/rp
mkdir -p "$dir"
cp test/* $dir

export TIMEFORMAT="%U %S"
export LC_LANG=C

bench() {
  n=$1
  cmd="$2"
  seq $n | parallel "time _={} $cmd" 2>&1 | awk '{s+=$1+$2} END {printf("%.3f\t", s/n)}' n=$n
  echo "$cmd"
}
# echo "JOIN"
bench 100 "join -t$'\t' -j1 --nocheck-order  -o 1.1 1.2 2.2 $dir/{x,x1} > $dir/out_j1_{%}"
bench 100 "rp join --key=1 --val=2 $dir/{x,x1} > $dir/out_j2_{%}"

mismatches=$(comm -3 $dir/{out_j1_1,out_j2_1} | wc -l)
(( $mismatches == 0 )) || echo "Found $mismatches mismatches"

bench 1 "rp replace --dict test/x --sub=1 < $dir/x > $dir/replace"
mismatches=$(comm -3 $dir/replace <(paste <(seqA) <(seqA)) | wc -l)
(( $mismatches == 0 )) || echo "Found $mismatches mismatches"

# paste <(seq -w 0 $N) <(seq -w 0 $N) <(seq -w 0 $N) > $dir/x
# sed -n '1~3p' < $dir/x > $dir/x1
# echo -ne "join:    "; (time join -t$'\t' -j2 -o 1.2 1.3 2.3 --nocheck-order $dir/{x,x1} > $dir/j1) 2> >(bc)
# echo -ne "rp join: "; (time rp join --key=1 --val=3 $dir/{x,x1} > $dir/j2) 2> >(bc)
# mismatches=$(comm -3 $dir/{j1,j2} | wc -l)
# (( $mismatches == 0 )) || echo "Found $mismatches mismatches"

#
# echo -ne "join -a1:        "; (time join -t$'\t' -j1 --nocheck-order -a1 -o1.1,1.2,2.2 $dir/{x,x1} > $dir/j1) 2> >(bc)
# echo -ne "rp join -1: "; (time rp join --key=1 --val=2 -1 $dir/{x,x1} > $dir/j2) 2> >(bc)
# mismatches=$(comm -3 $dir/{j1,j2} | wc -l)
# (( $mismatches == 0 )) || echo "Found $mismatches mismatches in: comm -3 $dir/{j1,j2}"

# rm -r "$dir"
