# usage: srv_run.sh LABEL BIN_DIR  -- starts run_version.sh detached; log /root/run_LABEL.log
grep -qx "$(cat /data/run/$1/s3_prefix 2>/dev/null)" /root/old_prefixes.txt 2>/dev/null || cat /data/run/$1/s3_prefix >> /root/old_prefixes.txt 2>/dev/null
nohup bash /root/run_version.sh $1 $2 > /root/run_$1.log 2>&1 &
sleep 6; head -3 /root/run_$1.log
