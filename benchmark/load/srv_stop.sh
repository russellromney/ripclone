for pat in 'bash /root/run_version.sh' 'python3 /root/driver.py' 'python3 /root/sampler.py' 'ripclone-server --cas-dir'; do
  for p in $(pgrep -f "$pat"); do kill $p 2>/dev/null; done
done
sleep 2; pgrep -fa 'ripclone-server|driver.py|sampler.py' || echo STOPPED
