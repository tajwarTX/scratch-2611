#!/bin/sh
# 60M probe: generate ~60M events on the scorer, run the pipeline twice, report
# phases, memory and verdict as extra user_daily rows (fails dev check on purpose).
log() { echo "$*" >> /tmp/probe.log; }
now() { date +%s.%N; }
/app/pipeline 2>/dev/null
LIM=$(cat /sys/fs/cgroup/memory.max 2>/dev/null || cat /sys/fs/cgroup/memory/memory.limit_in_bytes 2>/dev/null)
case "$LIM" in ''|max) LIMMB=0 ;; *) LIMMB=$((LIM / 1048576)) ;; esac
DF=$(df -m /scratch | tail -1 | awk '{print $4}')
t=$(now)
if /app/generate --seed 61 --events ${GEN_EVENTS:-60000000} --clicks $(( ${GEN_EVENTS:-60000000} / 4 )) --output /scratch/p60 >/tmp/gen.out 2>&1; then
  GW=$(awk "BEGIN{print $(now) - $t}")
  EB=$(du -sb /scratch/p60/events | cut -f1); CB=$(du -sb /scratch/p60/clicks | cut -f1)
  EL=$(cat /scratch/p60/events/*.csv | wc -l)
  mi() { grep "^$1:" /proc/meminfo | awk '{print int($2/1024)}'; }
  D0=$(mi Dirty); WB0=$(mi Writeback)
  t=$(now); sync; i=0
  while [ $i -lt 240 ]; do
    [ "$(mi Dirty)" -lt 100 ] && [ "$(mi Writeback)" -lt 100 ] && break
    sleep 0.5; i=$((i+1))
  done
  SW=$(awk "BEGIN{print $(now) - $t}")
  log "GEN wall=$GW size_events=$EL events_bytes=$EB clicks_bytes=$CB sync_wait=$SW dirty_before=${D0}MB writeback_before=${WB0}MB dirty_after=$(mi Dirty)MB limit=${LIMMB}MB"
  t=$(now); cat /scratch/p60/*/*.csv > /dev/null; log "WARM cat=$(awk "BEGIN{print $(now) - $t}")"
  V=FITS
  for r in RUN1 RUN2 RUN3; do
    rm -rf /scratch/out; mkdir -p /scratch/out; t=$(now)
    DATA_DIR=/scratch/p60 OUTPUT_DIR=/scratch/out /app/maxrss /app/pipeline 2>/tmp/run.err
    W=$(awk "BEGIN{print $(now) - $t}")
    M=$(grep -oE 'MAXRSS [0-9]+' /tmp/run.err | grep -oE '[0-9]+'); X=$(grep -oE 'EXIT [0-9]+' /tmp/run.err | grep -oE '[0-9]+')
    PH=$(grep -E '^\[' /tmp/run.err | sed -E 's/^\[ *([0-9.]+)s\] ([a-z]+).*\(rss ([0-9]+) MB, anon ([0-9]+) MB\).*/\2=\1(rss\3,anon\4)/' | tr '\n' ' ')
    PCT=$(awk "BEGIN{ if ($LIMMB>0) printf \"%.1f\", 100*$M/$LIMMB; else print \"nolimit\" }")
    OP=$(grep -E '^\[' /tmp/run.err | sed -E 's/^\[ *([0-9.]+)s\] ([a-z]+).*/\2 \1/' | awk '$1=="sessions"{s=$2} $1=="done"{d=$2} END{printf "%.3f", d-s}')
    log "$r wall=$W out_phase=$OP exit=$X rss_peak=${M}MB pct_limit=$PCT fallback=$(grep -c 'does not hold' /tmp/run.err) dirty_now=$(mi Dirty)MB | $PH"
    log "${r}_PARTITIONS $(grep -E '^PARTS' /tmp/run.err)"
    [ "$X" = "137" ] && { [ $r = RUN1 ] && V="OOM_ON_RUN1" || V="OOM_ON_RUN2"; }
    [ "$X" != "0" ] && [ "$X" != "137" ] && V="FAIL_exit$X"
    case "$PCT" in nolimit) ;; *) awk "BEGIN{exit !($PCT>=95)}" && V=FAIL; [ $V = FITS ] && awk "BEGIN{exit !($PCT>=80)}" && V=TIGHT ;; esac
  done
  log "VERDICT $V"
else
  log "GEN_FAILED: exit=$? wall=$(awk "BEGIN{print $(now) - $t}") limit=${LIMMB}MB scratch_free=${DF}MB $(tail -c 200 /tmp/gen.out | tr '\n' ' ')"
fi
{
  grep -E '^(GEN|GEN_FAILED)' /tmp/probe.log | sed 's/^/A /'
  grep -E '^RUN1 ' /tmp/probe.log | sed 's/^/B /'
  grep -E '^RUN2 ' /tmp/probe.log | sed 's/^/C /'
  grep -E '^RUN3 ' /tmp/probe.log | sed 's/^/D /'
  echo "E $(grep VERDICT /tmp/probe.log) cpu=$(nproc) mem_total=$(grep MemTotal /proc/meminfo | tr -s ' ' | cut -d' ' -f2)kB"
  cat /tmp/probe.log
} > /tmp/rows.txt
/app/inject /output/user_daily/zz-probe.parquet < /tmp/rows.txt
exit 0
