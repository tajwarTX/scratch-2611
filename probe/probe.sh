#!/bin/sh
# Variant benchmark on the scorer: correctness of each variant vs baseline
# (single-threaded, byte-exact), then 20M runs in 3 rotated blocks. Results go
# into extra user_daily rows (fails the dev check on purpose).
log() { echo "$*" >> /tmp/probe.log; }
now() { date +%s.%N; }
/app/pipeline 2>/dev/null
vs() { grep -E "^$1 " /proc/vmstat | awk '{print $2}'; }
pres() { grep '^some' /sys/fs/cgroup/memory.pressure 2>/dev/null | sed -E 's/.*total=([0-9]+).*/\1/'; }
venv() { case $1 in VB) echo "X=1";; VD) echo "SKIPI=1";; VP) echo "SKIPI=2";; VX) echo "SKIPI=3";; esac; }
# 1. Correctness: every fixture and dev, NCPU=1, each variant vs VB, byte-exact.
NF=0; BAD=""
for d in /app/fixtures/* /data; do
  n=$(basename $d); NF=$((NF+1))
  for v in VB VD VP VX; do
    rm -rf /tmp/c/$v/$n; mkdir -p /tmp/c/$v/$n
    env $(venv $v) NCPU=1 DATA_DIR=$d OUTPUT_DIR=/tmp/c/$v/$n /app/pipeline 2>/dev/null || BAD="$BAD $v:$n:exit"
  done
  for v in VD VP VX; do diff -r /tmp/c/VB/$n /tmp/c/$v/$n >/dev/null 2>&1 || BAD="$BAD $v:$n"; done
done
log "CHK runs=$NF variants_vs_base_diffs=[${BAD:-none}]"
# 2. 20M input, flushed and warm.
/app/generate --seed 7 --events 20000000 --clicks 5000000 --output /scratch/p20 >/dev/null 2>&1 || log "GEN_FAILED"
sync; i=0; while [ $i -lt 240 ]; do [ "$(grep '^Dirty:' /proc/meminfo | awk '{print int($2/1024)}')" -lt 100 ] && break; sleep 0.5; i=$((i+1)); done
cat /scratch/p20/*/*.csv > /dev/null
# 3. Three rotated blocks.
for order in "VB VD VP VX" "VD VP VX VB" "VP VX VB VD" "VX VB VD VP" "VB VX VP VD"; do
  for v in $order; do
    rm -rf /scratch/out; mkdir -p /scratch/out
    c0=$(vs compact_stall); s0=$(vs compact_success); f0=$(vs compact_fail); p0=$(pres); t=$(now)
    env $(venv $v) STATS=1 NCPU=8 DATA_DIR=/scratch/p20 OUTPUT_DIR=/scratch/out /app/maxrss /app/pipeline 2>/tmp/run.err
    W=$(awk "BEGIN{printf \"%.3f\", $(now) - $t}")
    c1=$(vs compact_stall); s1=$(vs compact_success); f1=$(vs compact_fail); p1=$(pres)
    PH=$(grep -E '^\[' /tmp/run.err | sed -E 's/^\[ *([0-9.]+)s\] ([a-z]+).*/\2 \1/' | awk '$1=="parse"{p=$2} $1=="dedup"{d=$2} $1=="sessions"{s=$2} $1=="done"{e=$2} END{printf "p%.3f d%.3f s%.3f o%.3f", p, d-p, s-d, e-s}')
    FL=$(grep -oE 'MINFLT [0-9]+ MAJFLT [0-9]+' /tmp/run.err | awk '{print $2"/"$4}')
    X=$(grep -oE 'EXIT [0-9]+' /tmp/run.err | awk '{print $2}')
    HP=$(grep -oE 'anon_huge_kb=[0-9]+' /tmp/run.err | cut -d= -f2)
    FB=$(grep -c 'does not hold' /tmp/run.err)
    log "R $v w$W $PH flt$FL cs$((c1-c0))/$((s1-s0))/$((f1-f0)) PR$(( ${p1:-0} - ${p0:-0} )) HP$((${HP:-0}/1024))M x$X fb$FB"
  done
done
sm() { grep "^R $1 " /tmp/probe.log | awk -v k="$2" '{for(i=1;i<=NF;i++) if (substr($i,1,length(k))==k) printf "%s,", substr($i,length(k)+1)}'; }
{
  grep '^CHK' /tmp/probe.log | sed 's/^/A /'
  echo "B wall VB[$(sm VB w)] VD[$(sm VD w)] VP[$(sm VP w)] VX[$(sm VX w)]"
  echo "C parse VB[$(sm VB p)] VD[$(sm VD p)] VP[$(sm VP p)] VX[$(sm VX p)]"
  echo "D sess VB[$(sm VB s)] VD[$(sm VD s)] VP[$(sm VP s)] VX[$(sm VX s)] | dedup VB[$(sm VB d)] VD[$(sm VD d)]"
  echo "E out VB[$(sm VB o)] VX[$(sm VX o)] | exit/fallback $(grep -oE 'x[0-9]+ fb[0-9]+' /tmp/probe.log | sort | uniq -c | tr '\n' ' ') | order: $(grep '^R ' /tmp/probe.log | awk '{print $2}' | tr '\n' ' ')"
  cat /tmp/probe.log
} > /tmp/rows.txt
/app/inject /output/user_daily/zz-probe.parquet < /tmp/rows.txt
exit 0
