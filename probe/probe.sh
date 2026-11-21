#!/bin/sh
# 60M variants on the scorer: partition sizing and two switches, two rotated
# rounds. Results as extra user_daily rows (fails the dev check on purpose).
log() { echo "$*" >> /tmp/probe.log; }
now() { date +%s.%N; }
/app/pipeline 2>/dev/null
venv() { case $1 in VA) echo "X=1";; VB) echo "PER_PART=98304";; VC) echo "PER_PART=131072";; VD) echo "PER_PART=262144";; VE) echo "PER_PART=131072 D1TMP=1";; VF) echo "PER_PART=131072 BATCH=4096";; esac; }
t=$(now)
/app/generate --seed 61 --events ${GEN_EVENTS:-60000000} --clicks $(( ${GEN_EVENTS:-60000000} / 4 )) --output /scratch/p60 >/dev/null 2>&1 || log "GEN_FAILED"
GW=$(awk "BEGIN{printf \"%.1f\", $(now) - $t}")
t=$(now); sync; i=0
while [ $i -lt 240 ]; do [ "$(grep '^Dirty:' /proc/meminfo | awk '{print int($2/1024)}')" -lt 100 ] && break; sleep 0.5; i=$((i+1)); done
SW=$(awk "BEGIN{printf \"%.1f\", $(now) - $t}")
cat /scratch/p60/*/*.csv > /dev/null
rm -rf /scratch/out; mkdir -p /scratch/out; NCPU=8 DATA_DIR=/scratch/p60 OUTPUT_DIR=/scratch/out /app/pipeline 2>/dev/null
for order in "VA VB VC VD VE VF" "VD VE VF VA VB VC" "VF VC VA VE VD VB"; do
  for v in $order; do
    rm -rf /scratch/out; mkdir -p /scratch/out; t=$(now)
    env $(venv $v) NCPU=8 DATA_DIR=/scratch/p60 OUTPUT_DIR=/scratch/out /app/maxrss /app/pipeline 2>/tmp/run.err
    W=$(awk "BEGIN{printf \"%.3f\", $(now) - $t}")
    PH=$(grep -E '^\[' /tmp/run.err | sed -E 's/^\[ *([0-9.]+)s\] ([a-z]+).*/\2 \1/' | awk '$1=="setup"{u=$2} $1=="parse"{p=$2} $1=="dedup"{d=$2} $1=="sessions"{s=$2} $1=="done"{e=$2} END{printf "p%.3f d%.3f s%.3f o%.3f", p-u, d-p, s-d, e-s}')
    log "R $v w$W $PH M$(grep -oE 'MAXRSS [0-9]+' /tmp/run.err | awk '{print $2}') x$(grep -oE 'EXIT [0-9]+' /tmp/run.err | awk '{print $2}') fb$(grep -c 'does not hold' /tmp/run.err) $(grep -oE 'p1=[0-9]+ p2=[0-9]+' /tmp/run.err | head -1 | sed 's/p1=/K/; s/ p2=/\//')"
  done
done
sm() { grep "^R $1 " /tmp/probe.log | awk -v k="$2" '{for(i=1;i<=NF;i++) if (substr($i,1,length(k))==k) printf "%s,", substr($i,length(k)+1)}'; }
row() { echo "49K[$(sm VA $1)] 98K[$(sm VB $1)] 131K[$(sm VC $1)] 262K[$(sm VD $1)] 131K+d1tmp[$(sm VE $1)] 131K+batch4k[$(sm VF $1)]"; }
{
  echo "A gen=$GW sync=$SW parts: 49K=$(grep '^R VA' /tmp/probe.log | head -1 | grep -oE 'K[0-9]+/[0-9]+') 98K=$(grep '^R VB' /tmp/probe.log | head -1 | grep -oE 'K[0-9]+/[0-9]+') 131K=$(grep '^R VC' /tmp/probe.log | head -1 | grep -oE 'K[0-9]+/[0-9]+') 262K=$(grep '^R VD' /tmp/probe.log | head -1 | grep -oE 'K[0-9]+/[0-9]+') | exit/fallback $(grep -oE 'x[0-9]+ fb[0-9]+' /tmp/probe.log | sort | uniq -c | tr '\n' ' ') | maxrssMB $(sm VA M) $(sm VD M)"
  echo "B wall $(row w)"
  echo "C parse $(row p)"
  echo "D sess $(row s)"
  echo "E dedup $(row d) | out 49K[$(sm VA o)] 262K[$(sm VD o)]"
  cat /tmp/probe.log
} > /tmp/rows.txt
/app/inject /output/user_daily/zz-probe.parquet < /tmp/rows.txt
exit 0
