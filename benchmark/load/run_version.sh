# usage: run_version.sh LABEL BIN_DIR  -- one full workload against one server version.
# Fresh server state and S3 prefix; origin `bench` reset by the driver.
set -u
label=$1; bindir=$2; port=8001
R=/data/run/$label; rm -rf $R; mkdir -p $R/home $R/cache $R/repos $R/out
prefix="loadtest-$(date +%Y%m%d%H%M%S)-$label/"; echo "$prefix" > $R/s3_prefix
pkill -f "ripclone-server" || true; sleep 1
cd $R
nohup env -i PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin HOME=$R/home \
  RIPCLONE_SERVER_TOKEN="$(cat /root/bench_token)" RUST_LOG=info \
  RIPCLONE_ORIGIN_BASE=file:///opt/rc/origin RIPCLONE_POLL_INTERVAL_SECS=0 \
  RIPCLONE_RATE_LIMIT_PER_SEC=1000000 \
  RIPCLONE_S3_PREFIX="$prefix" ${EXTRA_ENV:-} ENVFILE=/root/s3.env \
  bash -c 'set -a; . "$ENVFILE"; set +a; unset ENVFILE; exec "$@"' _ \
  $bindir/ripclone-server --data-dir $R --host 0.0.0.0 --port $port >> $R/server.log 2>&1 &
sleep 3; curl -fsS http://127.0.0.1:$port/healthz || { echo SERVER_DOWN; exit 1; }
python3 /root/sampler.py $R/out/sampler.csv & sp=$!
python3 /root/driver.py $label $port /root/commits.json $R/out ${STEADY_N:-12} ${GAP_S:-6} ${STORM_N:-4} 2>&1 | tee $R/out/driver.log
kill $sp
du -sb $R/cache $R/repos > $R/out/disk.txt
( set -a; . /root/s3.env; set +a; aws s3 ls --recursive --summarize "s3://$RIPCLONE_S3_BUCKET/$prefix" | tail -2 ) > $R/out/bucket.txt
pkill -f "ripclone-server" || true
echo "RUN_DONE $label"
