# usage: probe.sh LABEL SERVER_URL OUT.csv  -- runs until killed.
# Every 10 s, one real clone from this client box, rotating repo and mode.
# Records wall time and exit code. Cache off, fresh dir each time.
set -u
label=$1; url=$2; out=$3
export RIPCLONE_SERVER=$url RIPCLONE_SERVER_TOKEN="$(cat /root/bench_token)" RIPCLONE_NO_CACHE=1 RIPCLONE_NO_METRICS=1 RIPCLONE_VERIFY_UPSTREAM=never
cases=("expressjs/express --depth 1" "facebook/react --depth 1" "pandas-dev/pandas --mode files" "expressjs/express --depth 0" "facebook/react --mode files" "pandas-dev/pandas --depth 1")
[ -s $out ] || echo "ts,label,repo,mode,exit,wall_s" > $out
i=0
while true; do
  c=${cases[$((i % ${#cases[@]}))]}; i=$((i+1))
  repo=${c%% *}; mode=${c#* }
  d=$(mktemp -d /data/probe.XXXX)
  s=$(date +%s.%N)
  /opt/rc/bin/ripclone clone $repo $d/w $mode > $d/log 2>&1; rc=$?
  e=$(date +%s.%N)
  echo "$s,$label,$repo,$mode,$rc,$(python3 -c "print(round($e-$s,3))")" >> $out
  [ $rc -ne 0 ] && { echo "--- $(date -u +%T) $repo $mode rc=$rc" >> ${out%.csv}.err; tail -5 $d/log >> ${out%.csv}.err; }
  rm -rf $d
  sleep 10
done
