# usage: probe_ctl.sh stop | start LABEL URL
for p in $(pgrep -f 'bash /root/probe.sh'); do kill $p; done
[ "$1" = start ] || exit 0
nohup bash /root/probe.sh $2 $3 /root/probe.csv > /dev/null 2>&1 &
echo started
