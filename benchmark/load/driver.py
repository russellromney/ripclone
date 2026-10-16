#!/usr/bin/env python3
"""Build-load workload against one ripclone server.

usage: driver.py LABEL PORT commits.json OUTDIR [steady_per_repo] [steady_gap_s] [storm_per_repo]

Phases:
  cold    every repo's base commit admitted at once via POST /add (origin
          branch `bench` points at the base). Wait for all outputs.
  steady  pushes round-robin across repos, one admission every gap seconds:
          move origin `bench` to the commit, POST /builds {selector: sha}.
  storm   the next pushes of every repo admitted at once, oldest first per repo.
Each admitted commit is polled (GET commits/{sha}?kind=) every 2 s until all
three outputs are ready or failed. Every poll's latency is recorded.
Writes OUTDIR/events.jsonl (one line per build) and OUTDIR/polls.csv.
"""
import hashlib, json, os, subprocess, sys, threading, time, urllib.request, urllib.error
label, port, commits_path, outdir = sys.argv[1:5]
steady_n = int(sys.argv[5]) if len(sys.argv) > 5 else 12
gap = float(sys.argv[6]) if len(sys.argv) > 6 else 6.0
storm_n = int(sys.argv[7]) if len(sys.argv) > 7 else 4
os.makedirs(outdir, exist_ok=True)
C = json.load(open(commits_path))
TOK = open('/root/bench_token').read().strip()
AUTH = 'Ripclone ' + hashlib.sha256(TOK.encode()).hexdigest()
BASE = f'http://127.0.0.1:{port}/v1/repos/github'
KINDS = ('head', 'files', 'full')
lock = threading.Lock()
pollf = open(f'{outdir}/polls.csv', 'a')
evf = open(f'{outdir}/events.jsonl', 'a')
def log(*a):
    print(time.strftime('%H:%M:%S'), *a, flush=True)
def req(method, url, body=None):
    h = {'Authorization': AUTH}
    data = None
    if method == 'POST':
        data = json.dumps(body).encode() if body is not None else b''
        if body is not None: h['Content-Type'] = 'application/json'
    t = time.time()
    try:
        with urllib.request.urlopen(urllib.request.Request(url, data=data, method=method, headers=h), timeout=120) as r:
            st, txt = r.status, r.read().decode()
    except urllib.error.HTTPError as e: st, txt = e.code, e.read().decode()
    except Exception as e: st, txt = 0, repr(e)
    return st, txt, time.time() - t
def set_bench(repo, sha):
    subprocess.run(['git', '-C', f'/opt/rc/origin/{repo}.git', 'update-ref', 'refs/heads/bench', sha], check=True)
    subprocess.run(['git', '-C', f'/opt/rc/origin/{repo}.git', 'symbolic-ref', 'HEAD', 'refs/heads/bench'], check=True)
pending = {}  # (repo, sha) -> record
def admit(phase, repo, sha, via_add=False):
    set_bench(repo, sha)
    rec = {'label': label, 'phase': phase, 'repo': repo, 'commit': sha, 'admitted': time.time()}
    for attempt in range(10):  # 429/5xx/connection errors retry; anything else is final
        if via_add: st, txt, dt = req('POST', f'{BASE}/{repo}/add')
        else: st, txt, dt = req('POST', f'{BASE}/{repo}/builds', {'selector': sha})
        if st in (200, 202) or (st and st < 500 and st != 429): break
        log('ADMIT RETRY', repo, sha[:10], st, txt[:120]); time.sleep(2)
    rec.update(admit_http=st, admit_s=round(dt, 3))
    if st not in (200, 202): rec['admit_body'] = txt[:300]; log('ADMIT FAIL', repo, sha[:10], st, txt[:200])
    if st not in (200, 202): rec['done'] = True; evf.write(json.dumps(rec) + '\n'); evf.flush(); return
    with lock: pending[(repo, sha)] = rec
def poller():
    while True:
        with lock: items = list(pending.items())
        for key, rec in items:
            if rec.get('done'): continue
            repo, sha = key
            for k in KINDS:
                if k in rec: continue
                st, txt, dt = req('GET', f'{BASE}/{repo}/commits/{sha}?kind={k}')
                pollf.write(f'{time.time():.3f},{label},{st},{dt*1000:.1f}\n')
                try: state = json.loads(txt).get('state')
                except Exception: state = None
                if state in ('ready', 'failed'):
                    rec[k] = round(time.time() - rec['admitted'], 1)
                    if state == 'failed': rec[k + '_failed'] = txt[:300]; log('FAILED', repo, sha[:10], k, txt[:200])
            if all(k in rec for k in KINDS):
                rec['done'] = True
                with lock: evf.write(json.dumps(rec) + '\n'); evf.flush()
        pollf.flush()
        time.sleep(2)
def wait_all(timeout):
    t = time.time()
    while time.time() - t < timeout:
        with lock: left = [k for k, r in pending.items() if not r.get('done')]
        if not left: return True
        time.sleep(5)
    log('TIMEOUT waiting for', len(left), [f'{r}@{s[:8]}' for r, s in left][:10])
    return False
threading.Thread(target=poller, daemon=True).start()
repos = list(C)
log(label, 'COLD start', len(repos), 'repos')
for r in repos: admit('cold', r, C[r]['base'], via_add=True)
wait_all(3 * 3600); log(label, 'COLD done')
log(label, 'STEADY start', steady_n, 'per repo, gap', gap)
for i in range(steady_n):
    for r in repos:
        if i < len(C[r]['pushes']):
            admit('steady', r, C[r]['pushes'][i]); time.sleep(gap)
wait_all(3 * 3600); log(label, 'STEADY done')
log(label, 'STORM start', storm_n, 'per repo')
for r in repos:
    for c in C[r]['pushes'][steady_n:steady_n + storm_n]: admit('storm', r, c)
wait_all(3 * 3600); log(label, 'STORM done')
log(label, 'ALL DONE')
