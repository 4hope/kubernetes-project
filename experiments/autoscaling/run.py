"""Observe CPU-based autoscaling during low/high/low HTTP load. Run from any directory."""
import argparse
import datetime
import json
from pathlib import Path
import subprocess
import sys
import threading
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
from experiment import ready

BASE = ['kubectl', '--context', 'kind-selfheal', '-n', 'selfheal']
NAME = 'demo-autoscale'


def k(*args, input=None):
    result = subprocess.run(BASE + list(args), input=input, capture_output=True,
                            text=True, timeout=45)
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    return result.stdout


def obj(*args):
    return json.loads(k(*args, '-o', 'json'))


def save(path, value):
    path.write_text(json.dumps(value, indent=2), encoding='utf-8')


def current_pods():
    return obj('get', 'pods', '-l', f'app={NAME}')['items']


def reset(hpa):
    # Recreate the HPA once its Deployment has two Ready replicas.
    k('delete', 'hpa', NAME, '--ignore-not-found=true')
    k('scale', f'deployment/{NAME}', '--replicas=2')
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        pods = current_pods()
        if len(pods) == 2 and all(ready(p) for p in pods):
            k('apply', '-f', '-', input=json.dumps(hpa))
            return
        time.sleep(2)
    raise RuntimeError('Two initial Ready Pods not reached')


def phase(out, name, rate, duration, origin, states):
    folder = out / name
    folder.mkdir()
    handshake = threading.Event()
    started = [None]
    errors = []
    with (folder / 'requests.jsonl').open('w') as requests, \
         (folder / 'client-events.jsonl').open('w') as events, \
         (folder / 'client-stderr.txt').open('w') as stderr:
        process = subprocess.Popen(BASE + ['exec', 'loadgen-pressure', '--', 'python',
            '/scripts/load.py', '--url', f'http://{NAME}:8080/', '--rate', str(rate),
            '--duration', str(duration), '--timeout', '1', '--workers', '512'],
            stdout=subprocess.PIPE, stderr=stderr, text=True, bufsize=1)

        def read():
            try:
                for line in process.stdout:
                    row = json.loads(line)
                    if row.get('event'):
                        row['runner_received_s'] = time.monotonic() - origin
                        events.write(json.dumps(row) + '\n'); events.flush()
                        if row['event'] == 'client_started':
                            started[0] = time.monotonic(); handshake.set()
                    else:
                        requests.write(json.dumps(row) + '\n'); requests.flush()
            except Exception as exc:
                errors.append(str(exc))

        reader = threading.Thread(target=read, daemon=True)
        reader.start()
        try:
            if not handshake.wait(20):
                raise RuntimeError('No client-start handshake')
            save(folder / 'phase.json', {'name': name, 'rate': rate, 'duration_s': duration,
                'start_s': started[0] - origin, 'alignment': 'runner receipt of client-start message'})
            print(f'LOAD {name}: {rate:g} requests/s for {duration:g} s', flush=True)
            previous = None
            while True:
                tick = time.monotonic()
                deployment = obj('get', 'deployment', NAME)
                hpa = obj('get', 'hpa', NAME)
                pods = current_pods()
                try:
                    metrics = json.loads(k('get', '--raw',
                        f'/apis/metrics.k8s.io/v1beta1/namespaces/selfheal/pods?labelSelector=app%3D{NAME}'))
                except RuntimeError as exc:
                    metrics = {'error': str(exc)}
                state = {'elapsed_s': time.monotonic() - origin, 'phase': name,
                    'phase_elapsed_s': time.monotonic() - started[0],
                    'deployment': deployment, 'hpa': hpa, 'pods': pods, 'metrics': metrics}
                states.write(json.dumps(state) + '\n'); states.flush()
                counts = (deployment['spec']['replicas'], sum(ready(p) for p in pods))
                if counts != previous:
                    print(f'  t={state["elapsed_s"]:.1f}s desired={counts[0]} Ready={counts[1]}', flush=True)
                    previous = counts
                if process.poll() is not None:
                    break
                if time.monotonic() - started[0] > duration + 30:
                    raise RuntimeError('Client exceeded deadline')
                time.sleep(max(0, 5 - (time.monotonic() - tick)))
            reader.join(10)
            if process.returncode or errors or reader.is_alive():
                raise RuntimeError(f'Client/read failure: {process.returncode}, {errors}')
        finally:
            if process.poll() is None:
                process.terminate(); process.wait(10)
            reader.join(10)
            process.stdout.close()
    metadata = json.loads((folder / 'phase.json').read_text())
    metadata['end_s'] = time.monotonic() - origin
    save(folder / 'phase.json', metadata)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--low-rate', type=float, default=10)
    parser.add_argument('--high-rate', type=float, default=400)
    parser.add_argument('--low-seconds', type=float, default=60)
    parser.add_argument('--high-seconds', type=float, default=150)
    parser.add_argument('--recovery-seconds', type=float, default=150)
    args = parser.parse_args()
    if min(args.repeats, args.low_rate, args.high_rate, args.low_seconds,
           args.high_seconds, args.recovery_seconds) <= 0 or args.high_rate <= args.low_rate:
        parser.error('Use positive values and high-rate greater than low-rate')
    out = args.output or ROOT / 'experiments/autoscaling/results' / datetime.datetime.now(
        datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    out.mkdir(parents=True, exist_ok=False)
    hpa = obj('get', 'hpa', NAME)
    clean_hpa = {'apiVersion': hpa['apiVersion'], 'kind': hpa['kind'],
                 'metadata': {'name': NAME, 'namespace': 'selfheal'}, 'spec': hpa['spec']}
    config = obj('get', 'configmap', 'pressure-scripts')
    if config['data']['load.py'] != (ROOT / 'experiments/pressure/load.py').read_text():
        raise RuntimeError('pressure-scripts ConfigMap differs from client source')
    save(out / 'parameters.json', {**{k: v for k, v in vars(args).items() if k != 'output'},
        'sample_period_s': 5, 'workers': 512, 'timeout_s': 1,
        'runner_source': Path(__file__).read_text(), 'client_source': config['data']['load.py'],
        'app_manifest': (Path(__file__).with_name('app.yaml')).read_text()})
    for name, value in [('hpa-config', clean_hpa), ('nodes', obj('get', 'nodes')),
                        ('versions', obj('version')), ('loadgen-pod', obj('get', 'pod', 'loadgen-pressure')),
                        ('metrics-server', json.loads(k('get', 'deployment', 'metrics-server', '-n', 'kube-system', '-o', 'json'))),
                        ('original-demo-web', obj('get', 'deployment', 'demo-web'))]:
        save(out / f'{name}.json', value)
    print('Results:', out, flush=True)
    try:
        for repeat in range(1, args.repeats + 1):
            print('RESET repeat', repeat, flush=True)
            reset(clean_hpa)
            run = out / f'repeat-{repeat}'
            run.mkdir()
            save(run / 'initial-deployment.json', obj('get', 'deployment', NAME))
            origin = time.monotonic()
            with (run / 'states.jsonl').open('w') as states:
                for name, rate, duration in [('low', args.low_rate, args.low_seconds),
                    ('high', args.high_rate, args.high_seconds),
                    ('recovery', args.low_rate, args.recovery_seconds)]:
                    phase(run, name, rate, duration, origin, states)
            save(run / 'events.json', obj('get', 'events', '--field-selector',
                 f'involvedObject.name={NAME}'))
            save(run / 'final-deployment.json', obj('get', 'deployment', NAME))
    except Exception as exc:
        save(out / 'error.json', {'error': str(exc)})
        raise
    finally:
        reset(clean_hpa)
    print('DONE. Analyze this directory with experiments/autoscaling/analyze.py.', flush=True)


if __name__ == '__main__':
    main()
