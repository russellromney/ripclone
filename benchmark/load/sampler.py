#!/usr/bin/env python3
# usage: sampler.py OUT.csv  -- every 2 s: CPU split, memory, disk and net bytes,
# RSS of every ripclone-server process tree, git process count.
import os, sys, time
out = open(sys.argv[1], 'a')
def cpu():
    v = list(map(int, open('/proc/stat').readline().split()[1:8]))
    return v  # user nice system idle iowait irq softirq
def disk():
    r = w = 0
    for l in open('/proc/diskstats'):
        f = l.split()
        if f[2] in ('sda', 'vda', 'nvme0n1'): r += int(f[5]) * 512; w += int(f[9]) * 512
    return r, w
def net():
    rx = tx = 0
    for l in open('/proc/net/dev').readlines()[2:]:
        n, rest = l.split(':', 1)
        if n.strip() == 'lo': continue
        f = rest.split(); rx += int(f[0]); tx += int(f[8])
    return rx, tx
def procs():
    ppid, comm, rss = {}, {}, {}
    for p in os.listdir('/proc'):
        if not p.isdigit(): continue
        try:
            s = open(f'/proc/{p}/stat').read()
            c = s[s.index('(') + 1:s.rindex(')')]; f = s[s.rindex(')') + 2:].split()
            ppid[int(p)] = int(f[1]); comm[int(p)] = c; rss[int(p)] = int(f[21]) * 4096
        except Exception: pass
    roots = [p for p, c in comm.items() if c.startswith('ripclone-server')]
    kids = {}
    for p, pp in ppid.items(): kids.setdefault(pp, []).append(p)
    tree_rss, gits, stack = 0, 0, list(roots)
    while stack:
        p = stack.pop(); tree_rss += rss.get(p, 0)
        if comm.get(p, '').startswith('git'): gits += 1
        stack += kids.get(p, [])
    srv = sum(rss[p] for p in roots)
    return srv, tree_rss, gits
def mem():
    m = {l.split(':')[0]: int(l.split()[1]) * 1024 for l in open('/proc/meminfo')}
    return m['MemTotal'] - m['MemAvailable']
if os.path.getsize(sys.argv[1]) == 0:
    out.write('ts,user,sys,idle,iowait,mem_used,srv_rss,tree_rss,git_procs,disk_r,disk_w,net_rx,net_tx\n')
pc, pd, pn = cpu(), disk(), net()
while True:
    time.sleep(2)
    c, d, n = cpu(), disk(), net()
    dc = [a - b for a, b in zip(c, pc)]; tot = sum(dc) or 1
    srv, tree, gits = procs()
    out.write(f'{time.time():.1f},{(dc[0]+dc[1])/tot:.3f},{(dc[2]+dc[5]+dc[6])/tot:.3f},{dc[3]/tot:.3f},{dc[4]/tot:.3f},{mem()},{srv},{tree},{gits},{d[0]-pd[0]},{d[1]-pd[1]},{n[0]-pn[0]},{n[1]-pn[1]}\n')
    out.flush(); pc, pd, pn = c, d, n
