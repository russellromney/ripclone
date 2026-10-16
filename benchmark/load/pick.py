#!/usr/bin/env python3
# usage: pick.py N > commits.json
# For each origin in /opt/rc/origin/<owner>/<repo>.git: walk the default
# branch's first-parent chain; base = tip~(N*step); pushes = every step-th
# first-parent commit after base, oldest first (the last one is the tip).
import json, subprocess, sys
N = int(sys.argv[1])
out = {}
for line in open('/root/repos.txt'):
    line = line.split('#')[0].strip()
    if not line: continue
    repo, step = line.split(); step = int(step)
    d = f'/opt/rc/origin/{repo}.git'
    tip = subprocess.check_output(['git', '-C', d, 'rev-parse', 'refs/heads/' + subprocess.check_output(['git', '-C', d, 'symbolic-ref', '--short', 'HEAD'], text=True).strip()], text=True).strip()
    chain = subprocess.check_output(['git', '-C', d, 'rev-list', '--first-parent', '-n', str(N * step + 1), tip], text=True).split()
    chain.reverse()
    base = chain[0]
    pushes = chain[step::step][:N]
    merges = sum(1 for c in pushes if len(subprocess.check_output(['git', '-C', d, 'rev-list', '--parents', '-n1', c], text=True).split()) > 2)
    out[repo] = {'base': base, 'pushes': pushes, 'step': step, 'merge_pushes': merges}
    print(f'{repo}: base {base[:10]} pushes {len(pushes)} merges {merges}', file=sys.stderr)
json.dump(out, sys.stdout, indent=1)
