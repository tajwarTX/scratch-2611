#!/bin/sh
# Variant benchmark on the scorer: correctness of each variant vs baseline
# (single-threaded, byte-exact), then 20M runs in 3 rotated blocks. Results go
# into extra user_daily rows (fails the dev check on purpose).
log() { echo "$*" >> /tmp/probe.log; }
now() { date +%s.%N; }
/app/pipeline 2>/dev/null
vs() { grep -E "^$1 " /proc/vmstat | awk '{print $2}'; }
pres() { grep '^some' /sys/fs/cgroup/memory.pressure 2>/dev/null | sed -E 's/.*total=([0-9]+).*/\1/'; }
venv() { case $1 in base) echo "X=1";; t512) echo "TOK512=1";; lmap) echo "LMAP=touched";; nol) echo "MIMALLOC_ALLOW_LARGE_OS_PAGES=0";; esac; }
# 1. Correctness: every fixture and dev, NCPU=1, each variant vs base, byte-exact.
NF=0; BAD=""
for d in /app/fixtures/* /data; do
  n=$(basename $d); NF=$((NF+1))
  for v in base t512 lmap nol; do
    rm -rf /tmp/c/$v/$n; mkdir -p /tmp/c/$v/$n
    env $(venv $v) NCPU=1 DATA_DIR=$d OUTPUT_DIR=/tmp/c/$v/$n /app/pipeline 2>/dev/null || BAD="$BAD $v:$n:exit"
  done
  for v in t512 lmap nol; do diff -r /tmp/c/base/$n /tmp/c/$v/$n >/dev/null 2>&1 || BAD="$BAD $v:$n"; done
done
AVX=$(grep -m1 -o avx512bw /proc/cpuinfo)
ML0=$(MIMALLOC_VERBOSE=1 NCPU=1 DATA_DIR=/app/fixtures/A1 OUTPUT_DIR=/tmp/m0 /app/pipeline 2>&1 | grep -iE "large_os_pages" | head -1 | tr -s ' ')
ML1=$(MIMALLOC_VERBOSE=1 MIMALLOC_ALLOW_LARGE_OS_PAGES=0 NCPU=1 DATA_DIR=/app/fixtures/A1 OUTPUT_DIR=/tmp/m1 /app/pipeline 2>&1 | grep -iE "large_os_pages" | head -1 | tr -s ' ')
T5=$(TOK512=1 NCPU=1 DATA_DIR=/app/fixtures/A1 OUTPUT_DIR=/tmp/m2 /app/pipeline 2>&1 | grep TOK512)
log "CHK runs=$NF variants_vs_base_diffs=[${BAD:-none}] $AVX tok512=[$T5] mimalloc_base=[$ML0] mimalloc_nol=[$ML1]"
# 2. 20M input, flushed and warm.
/app/generate --seed 7 --events 20000000 --clicks 5000000 --output /scratch/p20 >/dev/null 2>&1 || log "GEN_FAILED"
sync; i=0; while [ $i -lt 240 ]; do [ "$(grep '^Dirty:' /proc/meminfo | awk '{print int($2/1024)}')" -lt 100 ] && break; sleep 0.5; i=$((i+1)); done
cat /scratch/p20/*/*.csv > /dev/null
# 3. Three rotated blocks.
for order in "base t512 lmap nol" "t512 lmap nol base" "lmap nol base t512"; do
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
  echo "B wall base[$(sm base w)] t512[$(sm t512 w)] lmap[$(sm lmap w)] nol[$(sm nol w)] | parse base[$(sm base p)] t512[$(sm t512 p)] lmap[$(sm lmap p)] nol[$(sm nol p)]"
  echo "C dedup base[$(sm base d)] t512[$(sm t512 d)] lmap[$(sm lmap d)] nol[$(sm nol d)] | sess base[$(sm base s)] t512[$(sm t512 s)] lmap[$(sm lmap s)] nol[$(sm nol s)] | out base[$(sm base o)] t512[$(sm t512 o)] lmap[$(sm lmap o)] nol[$(sm nol o)]"
  echo "D flt(min/maj) base[$(sm base flt)] t512[$(sm t512 flt)] lmap[$(sm lmap flt)] nol[$(sm nol flt)] | compact(stall/succ/fail) base[$(sm base cs)] nol[$(sm nol cs)] t512[$(sm t512 cs)] lmap[$(sm lmap cs)]"
  echo "E hugeMB base[$(sm base HP)] nol[$(sm nol HP)] t512[$(sm t512 HP)] lmap[$(sm lmap HP)] | pressure_us base[$(sm base PR)] nol[$(sm nol PR)] | exit/fallback $(grep -oE 'x[0-9]+ fb[0-9]+' /tmp/probe.log | sort | uniq -c | tr '\n' ' ') | order: $(grep '^R ' /tmp/probe.log | awk '{print $2}' | tr '\n' ' ')"
  cat /tmp/probe.log
} > /tmp/rows.txt
/app/inject /output/user_daily/zz-probe.parquet < /tmp/rows.txt
exit 0
