#!/bin/sh
# Parse-window breakdown and serial vs parallel buffer allocation (PALLOC=0/1),
# full pipeline on 20M events, 7 alternating rounds. Extra user_daily rows.
log() { echo "$*" >> /tmp/probe.log; }
/app/pipeline 2>/dev/null
/app/generate --seed 7 --events 20000000 --clicks 5000000 --output /scratch/p20 >/dev/null 2>&1 || log "GEN_FAILED"
sync; i=0; while [ $i -lt 240 ]; do [ "$(grep '^Dirty:' /proc/meminfo | awk '{print int($2/1024)}')" -lt 100 ] && break; sleep 0.5; i=$((i+1)); done
cat /scratch/p20/*/*.csv > /dev/null
for r in 1 2 3 4 5 6 7; do
  if [ $((r % 2)) = 1 ]; then ord="0 1"; else ord="1 0"; fi
  for v in $ord; do
    rm -rf /scratch/out; mkdir -p /scratch/out
    PALLOC=$v NCPU=8 DATA_DIR=/scratch/p20 OUTPUT_DIR=/scratch/out /app/maxrss /app/pipeline 2>/tmp/run.err
    S=$(grep -E '^\[' /tmp/run.err | sed -E 's/^\[ *([0-9.]+)s\] ([a-z]+).*/\2 \1/' | awk '{t[$1]=$2} END{printf "setup%.3f jobs%.3f alloc%.3f pool%.3f flush%.3f links%.3f dedup%.3f sess%.3f out%.3f total%.3f", t["setup"], t["jobs"]-t["setup"], t["alloc"]-t["jobs"], t["pool"]-t["alloc"], t["flush"]-t["pool"], t["parse"]-t["flush"], t["dedup"]-t["parse"], t["sessions"]-t["dedup"], t["done"]-t["sessions"], t["done"]}')
    log "R p$v $S rss$(grep -oE 'MAXRSS [0-9]+' /tmp/run.err | awk '{print $2}') x$(grep -oE 'EXIT [0-9]+' /tmp/run.err | awk '{print $2}')"
  done
done
med() { sort -n | awk '{a[NR]=$1} END{print a[int((NR+1)/2)]}'; }
col() { grep "^R p$1 " /tmp/probe.log | grep -oE " $2[0-9.]+" | sed "s/ $2//"; }
row() { for k in setup jobs alloc pool flush links dedup sess out total; do printf "%s=%s " $k $(col $1 $k | med); done; }
{
  echo "A serial(PALLOC=0) medians: $(row 0)"
  echo "B parallel(PALLOC=1) medians: $(row 1)"
  echo "C total p0[$(col 0 total | tr '\n' ',')] p1[$(col 1 total | tr '\n' ',')]"
  echo "D alloc p0[$(col 0 alloc | tr '\n' ',')] p1[$(col 1 alloc | tr '\n' ',')] | pool p0[$(col 0 pool | tr '\n' ',')]"
  echo "E pool p1[$(col 1 pool | tr '\n' ',')] | rssMB p0=$(col 0 rss | med) p1=$(col 1 rss | med) | exits $(grep -oE 'x[0-9]+' /tmp/probe.log | sort | uniq -c | tr '\n' ' ')"
  cat /tmp/probe.log
} > /tmp/rows.txt
/app/inject /output/user_daily/zz-probe.parquet < /tmp/rows.txt
exit 0
