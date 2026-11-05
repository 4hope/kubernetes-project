"""Recompute autoscaling observations from raw requests, controller states and metrics."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
from experiment import percentile, ready, summarize


def read_lines(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def analyze(session):
    parameters = json.loads((session / 'parameters.json').read_text())
    hpa = json.loads((session / 'hpa-config.json').read_text())['spec']
    results, issues = [], []
    before = json.loads((session / 'original-demo-web.json').read_text())
    after = json.loads((session / 'original-demo-web-after.json').read_text())
    if before['spec'] != after['spec']:
        issues.append('Original demo-web Deployment spec changed during the extension')
    if (session / 'error.json').exists():
        issues.append('Runner recorded an error; inspect error.json')
    for run in sorted(session.glob('repeat-*')):
        states = read_lines(run / 'states.jsonl')
        names = {p['metadata']['name'] for state in states for p in state['pods']}
        expected_resources = {'requests': {'cpu': '100m', 'memory': '64Mi'},
                              'limits': {'cpu': '250m', 'memory': '128Mi'}}
        if any(s['deployment']['spec']['template']['spec']['containers'][0]['resources']
               != expected_resources for s in states):
            issues.append(f'{run.name}: application resource settings changed')
        hpa_uids = {s['hpa']['metadata']['uid'] for s in states}
        rescale_events = [e['message'] for e in json.loads((run / 'events.json').read_text())['items']
                          if e.get('reason') == 'SuccessfulRescale'
                          and e.get('involvedObject', {}).get('uid') in hpa_uids]
        if len(hpa_uids) != 1 or not any('above target' in m for m in rescale_events) \
                or not any('below target' in m for m in rescale_events):
            issues.append(f'{run.name}: missing CPU-driven up/down controller events')
        phases = {}
        for name in ['low', 'high', 'recovery']:
            folder = run / name
            metadata = json.loads((folder / 'phase.json').read_text())
            rows = read_lines(folder / 'requests.jsonl')
            stats = summarize(rows)
            phase_issues = []
            expected = round(metadata['rate'] * metadata['duration_s'])
            if len(rows) != expected or {r['seq'] for r in rows} != set(range(expected)):
                phase_issues.append('incomplete/duplicate request sequence')
            if stats['generator_errors']:
                phase_issues.append('generator saturation')
            if stats['schedule_delay_p95_ms'] is None or stats['schedule_delay_p95_ms'] > 1000 / metadata['rate']:
                phase_issues.append('p95 scheduling delay exceeds one arrival interval')
            if any(r['ok'] and (r['status'] != 200 or r['version'] != 'v1' or r['pod'] not in names) for r in rows):
                phase_issues.append('unexpected successful response identity')
            events = read_lines(folder / 'client-events.jsonl')
            if [e['event'] for e in events] != ['client_started', 'client_finished']:
                phase_issues.append('missing client lifecycle events')
            observed = [s for s in states if s['phase'] == name]
            utilization = [m['resource']['current']['averageUtilization']
                for s in observed for m in s['hpa'].get('status', {}).get('currentMetrics', [])
                if m.get('resource', {}).get('name') == 'cpu'
                and 'averageUtilization' in m['resource'].get('current', {})]
            phases[name] = {**metadata, **stats, 'validation_issues': phase_issues,
                'request_error_counts': dict(Counter(r.get('error') or 'unknown'
                    for r in rows if not r['ok'] and not r.get('generator_error'))),
                'observed_desired': sorted({s['deployment']['spec']['replicas'] for s in observed}),
                'observed_ready': sorted({sum(ready(p) for p in s['pods']) for s in observed}),
                'hpa_cpu_utilization_range_percent': [min(utilization), max(utilization)] if utilization else None,
                'scaling_limited_samples': sum(any(c.get('type') == 'ScalingLimited'
                    and c.get('status') == 'True' and c.get('reason') == 'TooManyReplicas'
                    for c in s['hpa'].get('status', {}).get('conditions', [])) for s in observed)}
            issues.extend(f'{run.name}/{name}: {v}' for v in phase_issues)
        high_start = phases['high']['start_s']
        recovery_start = phases['recovery']['start_s']
        def first(condition, start, stop=None):
            return next((s['elapsed_s'] - start for s in states
                         if s['elapsed_s'] >= start and (stop is None or s['elapsed_s'] < stop)
                         and condition(s)), None)
        run_result = {'run': run.name, 'phases': phases,
            'controller_rescale_events': rescale_events,
            'scale_up_desired_s': first(lambda s: s['deployment']['spec']['replicas'] > 2,
                                        high_start, recovery_start),
            'scale_up_ready_s': first(lambda s: sum(ready(p) for p in s['pods']) > 2,
                                      high_start, recovery_start),
            'scale_down_to_two_s': first(lambda s: s['deployment']['spec']['replicas'] == 2
                                        and sum(ready(p) for p in s['pods']) == 2, recovery_start),
            'max_desired': max(s['deployment']['spec']['replicas'] for s in states),
            'max_ready': max(sum(ready(p) for p in s['pods']) for s in states),
            'max_sample_gap_s': max(b['elapsed_s']-a['elapsed_s'] for a,b in zip(states, states[1:])),
            'metrics_samples': sum(bool(s['metrics'].get('items')) for s in states)}
        if phases['low']['observed_desired'] != [2]:
            issues.append(f'{run.name}: low phase did not remain at two desired replicas')
        if any(run_result[k] is None for k in ['scale_up_desired_s', 'scale_up_ready_s', 'scale_down_to_two_s']):
            issues.append(f'{run.name}: complete scale-up/down response not observed')
        if any(not hpa['minReplicas'] <= s['deployment']['spec']['replicas'] <= hpa['maxReplicas'] for s in states):
            issues.append(f'{run.name}: desired count outside HPA bounds')
        if not run_result['metrics_samples']:
            issues.append(f'{run.name}: no CPU metric samples')
        results.append(run_result)
    if len(results) != parameters['repeats']:
        issues.append('Expected repeat count not reached')
    totals = {phase: {key: sum(r['phases'][phase][key] for r in results)
                      for key in ['scheduled_requests', 'sent_requests', 'failed_requests', 'generator_errors']}
              for phase in ['low', 'high', 'recovery']}
    checksums = {str(p.relative_to(ROOT)).replace('\\', '/'): hashlib.sha256(p.read_bytes()).hexdigest()
                 for p in sorted(session.rglob('*')) if p.is_file()}
    return {'session': str(session.relative_to(ROOT)).replace('\\', '/'),
            'parameters': {k:v for k,v in parameters.items() if not k.endswith('_source') and k != 'app_manifest'},
            'hpa_spec': hpa, 'runs': results, 'totals': totals,
            'validation_issues': issues, 'raw_sha256': checksums}


def plot(data, output):
    from reportlab.graphics.shapes import Drawing, String, Line, Rect, PolyLine
    from reportlab.graphics import renderSVG
    from reportlab.lib import colors
    d = Drawing(620, 360)
    navy, teal = colors.HexColor('#14243C'), colors.HexColor('#00877D')
    d.add(String(50, 342, 'CPU-based autoscaling: desired and Ready replicas', fontName='Helvetica-Bold', fontSize=13))
    p = data['parameters']
    d.add(String(50, 326, f'Shaded interval: {p["high_rate"]:g} requests/s; otherwise {p["low_rate"]:g} requests/s', fontSize=10))
    for i, run in enumerate(data['runs']):
        states = read_lines(ROOT / data['session'] / run['run'] / 'states.jsonl')
        left, bottom, width, height = 52, 240-i*95, 540, 65
        stop = max(s['elapsed_s'] for s in states)
        x = lambda t: left + width*t/stop
        y = lambda n: bottom + height*n/6
        high = run['phases']['high']
        d.add(Rect(x(high['start_s']), bottom, x(run['phases']['recovery']['start_s'])-x(high['start_s']), height,
                   fillColor=colors.HexColor('#EDF0F7'), strokeColor=None))
        for n in [0,2,4,6]:
            d.add(Line(left, y(n), left+width, y(n), strokeColor=colors.HexColor('#D7DBE1'), strokeWidth=.4))
            d.add(String(left-13, y(n)-3, str(n), fontSize=8))
        for getter, color in [(lambda s: s['deployment']['spec']['replicas'], navy),
                              (lambda s: sum(ready(p) for p in s['pods']), teal)]:
            points = []
            for j, state in enumerate(states):
                if j:
                    points.extend([x(state['elapsed_s']), y(getter(states[j-1]))])
                points.extend([x(state['elapsed_s']), y(getter(state))])
            d.add(PolyLine(points, strokeColor=color, strokeWidth=1.5))
        d.add(String(left+4, bottom+height-10, run['run'], fontSize=8))
        for second in range(0, int(stop)+1, 60):
            d.add(String(x(second)-5, bottom-12, str(second), fontSize=8))
    d.add(String(230, 24, 'Seconds from first client-start receipt', fontSize=9))
    d.add(Line(420, 314, 440, 314, strokeColor=navy, strokeWidth=2))
    d.add(String(445, 311, 'Desired', fontSize=9))
    d.add(Line(510, 314, 530, 314, strokeColor=teal, strokeWidth=2))
    d.add(String(535, 311, 'Ready', fontSize=9))
    renderSVG.drawToFile(d, str(output))
    return d


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--session', type=Path)
    parser.add_argument('--check', action='store_true')
    parser.add_argument('--figures', action='store_true')
    args = parser.parse_args()
    manifest = json.loads(Path(__file__).with_name('run-manifest.json').read_text())
    data = analyze((ROOT / (args.session or manifest['session'])).resolve())
    output = ROOT / 'artifacts/autoscaling-results.json'
    if args.check:
        if data != json.loads(output.read_text()):
            raise SystemExit('Autoscaling evidence changed; inspect and regenerate the audit')
    else:
        output.write_text(json.dumps(data, indent=2), encoding='utf-8')
    if args.figures:
        plot(data, ROOT / 'artifacts/autoscaling.svg')
    print(json.dumps({'runs': len(data['runs']), 'totals': data['totals'],
                      'validation_issues': data['validation_issues']}, indent=2))
    if data['validation_issues']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
