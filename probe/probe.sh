#!/bin/sh
# Floor probe: on 20M events, time (1) read only, (2) read + tokenize,
# (3) read + tokenize + all conversions/validation, and the full pipeline,
# in 5 rotated rounds. Reported as extra user_daily rows (fails dev on purpose).
log() { echo "$*" >> /tmp/probe.log; }
now() { date +%s.%N; }
/app/pipeline 2>/dev/null
/app/generate --seed 7 --events 20000000 --clicks 5000000 --output /scratch/p20 >/dev/null 2>&1 || log "GEN_FAILED"
sync; i=0; while [ $i -lt 240 ]; do [ "$(grep '^Dirty:' /proc/meminfo | awk '{print int($2/1024)}')" -lt 100 ] && break; sleep 0.5; i=$((i+1)); done
cat /scratch/p20/*/*.csv > /dev/null
for order in "read tok conv full" "tok conv full read" "conv full read tok" "full read tok conv" "read tok conv full"; do
  for m in $order; do
    rm -rf /scratch/out; mkdir -p /scratch/out; t=$(now)
    if [ $m = full ]; then
      NCPU=8 DATA_DIR=/scratch/p20 OUTPUT_DIR=/scratch/out /app/pipeline 2>/tmp/run.err
      W=$(awk "BEGIN{printf \"%.3f\", $(now) - $t}")
      P=$(grep -E '^\[' /tmp/run.err | sed -E 's/^\[ *([0-9.]+)s\] ([a-z]+).*/\2 \1/' | awk '$1=="setup"{s=$2} $1=="parse"{p=$2} $1=="done"{d=$2} END{printf "parse%.3f inproc%.3f", p-s, d}')
      log "R full w$W $P"
    else
      FLOOR=$m NCPU=8 DATA_DIR=/scratch/p20 OUTPUT_DIR=/scratch/out /app/pipeline 2>/tmp/run.err
      log "R $m $(grep -oE 'wall=[0-9.]+ cpu=[0-9.]+' /tmp/run.err)"
    fi
  done
done
med() { sort -n | awk '{a[NR]=$1} END{print a[int((NR+1)/2)]}'; }
col() { grep "^R $1 " /tmp/probe.log | grep -oE "$2[0-9.]+" | sed "s/$2//"; }
{
  echo "A MEDIANS wall: read=$(col read wall= | med) tok=$(col tok wall= | med) conv=$(col conv wall= | med) | full_inproc=$(col full inproc | med) full_parse=$(col full parse | med) | cpu: read=$(col read cpu= | med) tok=$(col tok cpu= | med) conv=$(col conv cpu= | med) | cpu=$(nproc) $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2)"
  echo "B read wall[$(col read wall= | tr '\n' ',')] cpu[$(col read cpu= | tr '\n' ',')]"
  echo "C tok wall[$(col tok wall= | tr '\n' ',')] cpu[$(col tok cpu= | tr '\n' ',')]"
  echo "D conv wall[$(col conv wall= | tr '\n' ',')] cpu[$(col conv cpu= | tr '\n' ',')]"
  echo "E full proc_wall[$(col full w | tr '\n' ',')] inproc[$(col full inproc | tr '\n' ',')] parse[$(col full parse | tr '\n' ',')]"
  cat /tmp/probe.log
} > /tmp/rows.txt
/app/inject /output/user_daily/zz-probe.parquet < /tmp/rows.txt
exit 0
