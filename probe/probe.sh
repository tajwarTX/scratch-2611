#!/bin/sh
# Diagnostic run: per-phase timings at full scale with 4, 6 and 8 threads,
# reported as extra user_daily rows (fails the dev check on purpose).
log() { echo "$*" >> /tmp/probe.log; }
now() { date +%s.%N; }
T0=$(now)
t=$(now); /app/pipeline 2>/tmp/dev.err; log "dev wall $(awk "BEGIN{print $(now) - $t}")"
t=$(now); /bin/true; log "true wall $(awk "BEGIN{print $(now) - $t}")"
t=$(now); /app/generate --seed 7 --events 20000000 --clicks 5000000 --output /scratch/p20 >/dev/null 2>&1; log "gen wall $(awk "BEGIN{print $(now) - $t}")"
cat /scratch/p20/*/*.csv > /dev/null
for n in 8 4 6 8 4 6; do
  rm -rf /scratch/out; mkdir -p /scratch/out; t=$(now)
  NCPU=$n DATA_DIR=/scratch/p20 OUTPUT_DIR=/scratch/out /app/pipeline 2>/tmp/run.err
  w=$(awk "BEGIN{print $(now) - $t}")
  log "n$n wall $w | $(grep -oE '^\[ *[0-9.]+s\] [a-z]+' /tmp/run.err | sed -E 's/\[ *([0-9.]+)s\] ([a-z]+)/\2=\1/' | tr '\n' ' ') | $(grep -oE 'rss [0-9]+ MB' /tmp/run.err | tail -1)"
done
log "total $(awk "BEGIN{print $(now) - $T0}")"
{
  grep '^n8 ' /tmp/probe.log | head -1 | sed 's/^/A /'
  grep '^n4 ' /tmp/probe.log | head -1 | sed 's/^/B /'
  echo "C $(grep -E '^n[468] ' /tmp/probe.log | awk '{print $1"="$3}' | tr '\n' ' ') $(grep -E '^(dev|true|gen) wall' /tmp/probe.log | tr '\n' ' ')"
  grep '^n6 ' /tmp/probe.log | head -1 | sed 's/^/D /'
  echo "E cpu $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2) nproc=$(nproc) $(grep -m1 -o 'avx512[a-z]*' /proc/cpuinfo | head -1) siblings=$(grep -m1 siblings /proc/cpuinfo | cut -d: -f2) cores=$(grep -m1 'cpu cores' /proc/cpuinfo | cut -d: -f2) kernel=$(uname -r)"
  cat /tmp/probe.log
} > /tmp/rows.txt
/app/inject /output/user_daily/zz-probe.parquet < /tmp/rows.txt
exit 0
