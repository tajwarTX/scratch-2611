#!/bin/sh
# Diagnostic run: prints the scorer's environment and per-phase timings at
# full scale, then fails the dev check on purpose so the logs are shown.
log() { echo "PROBE $*" >&2; echo "$*" >> /tmp/probe.log; }
now() { date +%s.%N; }
T0=$(now)
log "start $(date -u +%H:%M:%S.%N) uptime=$(cut -d' ' -f1 /proc/uptime) self_start_ticks=$(cut -d' ' -f22 /proc/self/stat)"
log "kernel $(uname -r)"
log "cpu $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2) nproc=$(nproc) avx512=$(grep -m1 -o 'avx512bw' /proc/cpuinfo)"
log "cgroup cpu.max=$(cat /sys/fs/cgroup/cpu.max 2>/dev/null) cpuset=$(cat /sys/fs/cgroup/cpuset.cpus.effective 2>/dev/null) mem.max=$(cat /sys/fs/cgroup/memory.max 2>/dev/null)"
log "thp enabled=$(cat /sys/kernel/mm/transparent_hugepage/enabled) defrag=$(cat /sys/kernel/mm/transparent_hugepage/defrag)"
log "meminfo $(grep -E '^(MemTotal|MemFree|MemAvailable|Cached):' /proc/meminfo | tr -s ' ' | tr '\n' ' ')"
log "mounts $(grep -E ' /(data|output|scratch|tmp) ' /proc/mounts | tr '\n' ';')"
log "data bytes $(du -sb /data | cut -f1) files $(find /data -type f | wc -l)"
/app/residency /data 2>&1 | sed 's/^/resident-dev-start /' | tee -a /tmp/probe.log >&2
# 1. The real dev run, timed.
t=$(now); /app/pipeline 2>&1 | sed 's/^/dev /' | tee -a /tmp/probe.log >&2; log "dev pipeline wall $(awk "BEGIN{print $(now) - $t}")"
# 2. A full practice-size dataset on the scorer's own disk.
t=$(now); /app/generate --seed 7 --events ${GEN_EVENTS:-20000000} --clicks $(( ${GEN_EVENTS:-20000000} / 4 )) --output /scratch/p20 >/dev/null 2>&1; log "generate wall $(awk "BEGIN{print $(now) - $t}")"
/app/residency /scratch/p20 2>&1 | sed 's/^/resident-p20-written /' | tee -a /tmp/probe.log >&2
sync
for run in warm1 warm2; do
  rm -rf /scratch/out; t=$(now)
  DATA_DIR=/scratch/p20 OUTPUT_DIR=/scratch/out /app/pipeline 2>&1 | sed "s/^/$run /" | tee -a /tmp/probe.log >&2
  log "$run wall $(awk "BEGIN{print $(now) - $t}")"
done
/app/residency /scratch/p20 --drop 2>&1 | sed 's/^/resident-p20-dropped /' | tee -a /tmp/probe.log >&2
rm -rf /scratch/out; t=$(now)
DATA_DIR=/scratch/p20 OUTPUT_DIR=/scratch/out /app/pipeline 2>&1 | sed "s/^/cold /" | tee -a /tmp/probe.log >&2
log "cold wall $(awk "BEGIN{print $(now) - $t}")"
t=$(now); cat /scratch/p20/events/*.csv > /dev/null; log "cat warm wall $(awk "BEGIN{print $(now) - $t}")"
log "total probe $(awk "BEGIN{print $(now) - $T0}")"
# Fail the dev check on purpose: an empty campaign_daily.
ph() { grep "^$1 \[" /tmp/probe.log | sed -E 's/^[a-z0-9]+ \[ *([0-9.]+)s\] ([a-z]+).*/\2=\1/' | tr '\n' ' '; }
{
  echo "A warm1: $(ph warm1)"
  echo "B cold: $(ph cold)"
  echo "C walls: $(grep -E '^(warm1|warm2|cold|dev pipeline|generate|cat warm) wall' /tmp/probe.log | sed 's/ wall//' | tr '\n' ' ')"
  echo "D res: $(grep -E '^resident' /tmp/probe.log | sed -E 's/^resident-//; s/ files, / /' | tr '\n' ' ')"
  echo "E $(grep -E '^(thp|cpu)' /tmp/probe.log | tr '\n' ' ')"
  echo "F dev: $(ph dev)"
  cat /tmp/probe.log
} > /tmp/rows.txt
/app/inject /output/user_daily/zz-probe.parquet < /tmp/rows.txt
exit 0
