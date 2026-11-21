#!/bin/sh
# Write-cost decomposition: parse phase only on 20M events, with each kind of
# parse-time write switched off (SKIPW bits: 1 events, 2 dedup, 4 clicks,
# 8 link inserts), 7 rotated rounds. Extra user_daily rows (fails dev on purpose).
log() { echo "$*" >> /tmp/probe.log; }
/app/pipeline 2>/dev/null
/app/generate --seed 7 --events 20000000 --clicks 5000000 --output /scratch/p20 >/dev/null 2>&1 || log "GEN_FAILED"
sync; i=0; while [ $i -lt 240 ]; do [ "$(grep '^Dirty:' /proc/meminfo | awk '{print int($2/1024)}')" -lt 100 ] && break; sleep 0.5; i=$((i+1)); done
cat /scratch/p20/*/*.csv > /dev/null
for ord in "0 1 2 4 8 15" "1 2 4 8 15 0" "2 4 8 15 0 1" "4 8 15 0 1 2" "8 15 0 1 2 4" "15 0 1 2 4 8" "0 15 8 4 2 1"; do
  for m in $ord; do
    SKIPW=$m PARSEONLY=1 NCPU=8 DATA_DIR=/scratch/p20 OUTPUT_DIR=/scratch/out /app/maxrss /app/pipeline 2>/tmp/run.err
    P=$(grep -E '^\[' /tmp/run.err | sed -E 's/^\[ *([0-9.]+)s\] ([a-z]+).*/\2 \1/' | awk '$1=="setup"{s=$2} $1=="parse"{p=$2} END{printf "%.3f", p-s}')
    log "R s$m p$P flt$(grep -oE 'MINFLT [0-9]+' /tmp/run.err | awk '{print $2}') rss$(grep -oE 'MAXRSS [0-9]+' /tmp/run.err | awk '{print $2}')"
  done
done
med() { sort -n | awk '{a[NR]=$1} END{print a[int((NR+1)/2)]}'; }
col() { grep "^R s$1 " /tmp/probe.log | grep -oE " $2[0-9.]+" | sed "s/ $2//"; }
{
  echo "A parse medians: all=$(col 0 p | med) -events=$(col 1 p | med) -dedup=$(col 2 p | med) -clicks=$(col 4 p | med) -links=$(col 8 p | med) -allwrites=$(col 15 p | med)"
  echo "B minflt medians: all=$(col 0 flt | med) -events=$(col 1 flt | med) -dedup=$(col 2 flt | med) -clicks=$(col 4 flt | med) -links=$(col 8 flt | med) -allwrites=$(col 15 flt | med) | rssMB all=$(col 0 rss | med) none=$(col 15 rss | med)"
  echo "C all[$(col 0 p | tr '\n' ',')] none[$(col 15 p | tr '\n' ',')]"
  echo "D -events[$(col 1 p | tr '\n' ',')] -dedup[$(col 2 p | tr '\n' ',')]"
  echo "E -clicks[$(col 4 p | tr '\n' ',')] -links[$(col 8 p | tr '\n' ',')]"
  cat /tmp/probe.log
} > /tmp/rows.txt
/app/inject /output/user_daily/zz-probe.parquet < /tmp/rows.txt
exit 0
