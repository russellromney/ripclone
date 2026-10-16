#!/usr/bin/env python3
"""Summarize load-test runs.

usage: analyze.py RUN_DIR... [--probe probe.csv]

Each RUN_DIR is /data/run/<label> copied back (out/events.jsonl, out/polls.csv,
out/sampler.csv, out/disk.txt, out/bucket.txt). Prints one markdown section per
run and a side-by-side table.
"""
import csv, json, os, statistics, sys

def pct(xs, p):
    if not xs: return None
    xs = sorted(xs); k = (len(xs) - 1) * p / 100
    lo = int(k); hi = min(lo + 1, len(xs) - 1)
    return xs[lo] + (xs[hi] - xs[lo]) * (k - lo)

def fmt(x, d=1):
    return '-' if x is None else f'{x:.{d}f}'

EXCLUDE = set()

def load_run(d):
    out = os.path.join(d, 'out')
    ev = [json.loads(l) for l in open(os.path.join(out, 'events.jsonl')) if l.strip()]
    ev = [e for e in ev if e['repo'] not in EXCLUDE]
    polls = []
    p = os.path.join(out, 'polls.csv')
    if os.path.exists(p):
        for row in csv.reader(open(p)):
            polls.append((float(row[0]), int(row[2]), float(row[3])))
    samp = list(csv.DictReader(open(os.path.join(out, 'sampler.csv')))) if os.path.exists(os.path.join(out, 'sampler.csv')) else []
    extra = {}
    for f in ('disk.txt', 'bucket.txt'):
        fp = os.path.join(out, f)
        if os.path.exists(fp): extra[f] = open(fp).read().strip()
    label = ev[0]['label'] if ev else os.path.basename(d.rstrip('/'))
    return label, ev, polls, samp, extra

def phase_windows(ev):
    w = {}
    for e in ev:
        end = e['admitted'] + max(e.get(k, 0) for k in ('head', 'files', 'full'))
        a, b = w.get(e['phase'], (e['admitted'], end))
        w[e['phase']] = (min(a, e['admitted']), max(b, end))
    return w

def summarize(label, ev, polls, samp, extra, probe):
    s = {'label': label}
    print(f'\n## {label}\n')
    w = phase_windows(ev)
    print('| phase | n | head p50/p90/max | files p50/p90/max | full p50/p90/max | failed | wall s | fulls/h |')
    print('|---|---|---|---|---|---|---|---|')
    for ph in ('cold', 'steady', 'storm'):
        es = [e for e in ev if e['phase'] == ph]
        if not es: continue
        cells = []
        for k in ('head', 'files', 'full'):
            xs = [e[k] for e in es if k in e and k + '_failed' not in e]
            cells.append(f'{fmt(pct(xs,50))}/{fmt(pct(xs,90))}/{fmt(max(xs) if xs else None)}')
            s[f'{ph}_{k}_p50'] = pct(xs, 50); s[f'{ph}_{k}_p90'] = pct(xs, 90); s[f'{ph}_{k}_max'] = max(xs) if xs else None
        failed = sum(1 for e in es for k in ('head', 'files', 'full') if k + '_failed' in e)
        failed += sum(1 for e in es if e.get('admit_http') not in (200, 202))
        a, b = w[ph]; wall = b - a
        fph = len(es) / wall * 3600 if wall > 0 else None
        s[f'{ph}_wall'] = wall; s[f'{ph}_failed'] = failed; s[f'{ph}_fph'] = fph
        print(f'| {ph} | {len(es)} | {cells[0]} | {cells[1]} | {cells[2]} | {failed} | {wall:.0f} | {fmt(fph)} |')
    # per-repo cold, the long poles
    print('\ncold per repo (head/files/full s):', ', '.join(
        f"{e['repo'].split('/')[1]} {e.get('head','?')}/{e.get('files','?')}/{e.get('full','?')}"
        for e in sorted((e for e in ev if e['phase'] == 'cold'), key=lambda e: -e.get('full', 0))))
    # API latency per phase
    print('\n| phase | polls | api ms p50 | p99 | max | non-200 |')
    print('|---|---|---|---|---|---|')
    for ph, (a, b) in w.items():
        xs = [ms for t, st, ms in polls if a <= t <= b]
        bad = sum(1 for t, st, ms in polls if a <= t <= b and st != 200)
        s[f'{ph}_api_p99'] = pct(xs, 99); s[f'{ph}_api_max'] = max(xs) if xs else None
        print(f'| {ph} | {len(xs)} | {fmt(pct(xs,50))} | {fmt(pct(xs,99))} | {fmt(max(xs) if xs else None)} | {bad} |')
    adm = [e['admit_s'] * 1000 for e in ev if 'admit_s' in e and e['phase'] != 'cold']
    s['admit_p99'] = pct(adm, 99)
    print(f'\nPOST /builds ms p50/p99/max: {fmt(pct(adm,50))}/{fmt(pct(adm,99))}/{fmt(max(adm) if adm else None)}')
    # resources
    if samp:
        cpu = [1 - float(r['idle']) - float(r['iowait']) for r in samp]
        io = [float(r['iowait']) for r in samp]
        mem = [int(r['mem_used']) for r in samp]
        tree = [int(r['tree_rss']) for r in samp]
        srv = [int(r['srv_rss']) for r in samp]
        gits = [int(r['git_procs']) for r in samp]
        dw = sum(int(r['disk_w']) for r in samp); rx = sum(int(r['net_rx']) for r in samp); tx = sum(int(r['net_tx']) for r in samp)
        s.update(cpu_mean=statistics.mean(cpu), mem_max=max(mem), srv_rss_max=max(srv), tree_rss_max=max(tree), disk_w=dw, net_tx=tx)
        print(f'\nCPU busy mean {statistics.mean(cpu)*100:.0f}% (p90 {pct(cpu,90)*100:.0f}%), iowait mean {statistics.mean(io)*100:.1f}%; '
              f'mem used max {max(mem)/2**30:.1f} GiB; server RSS max {max(srv)/2**30:.2f} GiB; '
              f'tree RSS max {max(tree)/2**30:.1f} GiB; git procs max {max(gits)}; '
              f'disk written {dw/2**30:.1f} GiB; net rx {rx/2**30:.1f} GiB tx {tx/2**30:.1f} GiB')
        # CPU busy per phase: how well the box is used
        for ph, (a, b) in w.items():
            xs = [1 - float(r['idle']) - float(r['iowait']) for r in samp if a <= float(r['ts']) <= b]
            if xs: s[f'{ph}_cpu'] = statistics.mean(xs); print(f'  {ph}: CPU busy mean {statistics.mean(xs)*100:.0f}%')
    for k, v in extra.items(): print(f'\n{k}:\n{v}')
    if probe:
        # Only clones taken after cold finished: before that the repos are not built.
        lo = min(a for ph, (a, b) in w.items() if ph != 'cold') if len(w) > 1 else 0
        rows = [r for r in probe if r['label'] == label and float(r['ts']) >= lo]
        for mode in sorted({r['mode'] for r in rows}):
            xs = [float(r['wall_s']) for r in rows if r['mode'] == mode and r['exit'] == '0']
            bad = sum(1 for r in rows if r['mode'] == mode and r['exit'] != '0')
            print(f'probe {mode}: n={len(xs)} p50 {fmt(pct(xs,50),2)} p90 {fmt(pct(xs,90),2)} max {fmt(max(xs) if xs else None,2)} failed {bad}')
        s['probe_failed'] = sum(1 for r in rows if r['exit'] != '0')
    return s

def main():
    args = sys.argv[1:]; probe = None
    while '--exclude' in args:
        i = args.index('--exclude'); EXCLUDE.add(args[i + 1]); del args[i:i + 2]
    if '--probe' in args:
        i = args.index('--probe'); probe = list(csv.DictReader(open(args[i + 1]))); del args[i:i + 2]
    sums = [summarize(*load_run(d), probe) for d in args]
    if len(sums) < 2: return
    keys = ['cold_full_max', 'cold_wall', 'steady_head_p50', 'steady_head_p90', 'steady_files_p90', 'steady_full_p50', 'steady_full_p90',
            'storm_head_p90', 'storm_full_p90', 'storm_full_max', 'storm_wall', 'steady_fph', 'storm_fph',
            'steady_api_p99', 'storm_api_p99', 'admit_p99', 'cold_cpu', 'steady_cpu', 'storm_cpu', 'tree_rss_max', 'disk_w',
            'cold_failed', 'steady_failed', 'storm_failed', 'probe_failed']
    print('\n## side by side\n')
    print('| metric | ' + ' | '.join(s['label'] for s in sums) + ' |')
    print('|---|' + '---|' * len(sums))
    for k in keys:
        def cell(v):
            if v is None: return '-'
            if k in ('tree_rss_max', 'disk_w'): return f'{v/2**30:.1f} GiB'
            if k.endswith('_cpu'): return f'{v*100:.0f}%'
            return fmt(v) if isinstance(v, float) else str(v)
        print(f'| {k} | ' + ' | '.join(cell(s.get(k)) for s in sums) + ' |')

main()
