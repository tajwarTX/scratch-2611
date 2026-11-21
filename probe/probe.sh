#!/bin/sh
# Variant benchmark on the scorer: correctness of each variant vs baseline
# (single-threaded, byte-exact), then 20M runs in 3 rotated blocks. Results go
# into extra user_daily rows (fails the dev check on purpose).
log() { echo "$*" >> /tmp/probe.log; }
now() { date +%s.%N; }
/app/pipeline 2>/dev/null
vs() { grep -E "^$1 " /proc/vmstat | awk '{print $2}'; }
pres() { grep '^some' /sys/fs/cgroup/memory.pressure 2>/dev/null | sed -E 's/.*total=([0-9]+).*/\1/'; }
venv() { case $1 in VB) echo "X=1";; VM) echo "PER_PART=24576";; VF) echo "PER_PART=98304";; VN) echo "SCATTER_NT=0";; VR) echo "READIO=1";; esac; }
# 1. Correctness: every fixture and dev, NCPU=1, each variant vs VB, byte-exact.
NF=0; BAD=""
for d in /app/fixtures/* /data; do
  n=$(basename $d); NF=$((NF+1))
  for v in VB VM VF VN VR; do
    rm -rf /tmp/c/$v/$n; mkdir -p /tmp/c/$v/$n
    env $(venv $v) NCPU=1 DATA_DIR=$d OUTPUT_DIR=/tmp/c/$v/$n /app/pipeline 2>/dev/null || BAD="$BAD $v:$n:exit"
  done
  for v in VM VF VN VR; do diff -r /tmp/c/VB/$n /tmp/c/$v/$n >/dev/null 2>&1 || BAD="$BAD $v:$n"; done
done
log "CHK runs=$NF variants_vs_base_diffs=[${BAD:-none}]"
# 2. 20M input, flushed and warm.
/app/generate --seed 7 --events 20000000 --clicks 5000000 --output /scratch/p20 >/dev/null 2>&1 || log "GEN_FAILED"
sync; i=0; while [ $i -lt 240 ]; do [ "$(grep '^Dirty:' /proc/meminfo | awk '{print int($2/1024)}')" -lt 100 ] && break; sleep 0.5; i=$((i+1)); done
cat /scratch/p20/*/*.csv > /dev/null
# 3. Three rotated blocks.
for order in "VB VM VF VN VR" "VM VF VN VR VB" "VF VN VR VB VM" "VN VR VB VM VF" "VR VB VM VF VN"; do
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
  echo "B wall base[$(sm VB w)] parts1024[$(sm VM w)] parts256[$(sm VF w)] no_nt[$(sm VN w)] readio[$(sm VR w)]"
  echo "C parse base[$(sm VB p)] parts1024[$(sm VM p)] parts256[$(sm VF p)] no_nt[$(sm VN p)] readio[$(sm VR p)]"
  echo "D sess base[$(sm VB s)] parts1024[$(sm VM s)] parts256[$(sm VF s)] | dedup base[$(sm VB d)] parts1024[$(sm VM d)] parts256[$(sm VF d)]"
  echo "E out base[$(sm VB o)] | exit/fallback $(grep -oE 'x[0-9]+ fb[0-9]+' /tmp/probe.log | sort | uniq -c | tr '\n' ' ') | rss $(grep '^R ' /tmp/probe.log | head -5 | grep -oE 'HP[0-9]+M' | tr '\n' ' ')"
  cat /tmp/probe.log
} > /tmp/rows.txt
/app/inject /output/user_daily/zz-probe.parquet < /tmp/rows.txt
exit 0
