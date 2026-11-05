"""Calibrate 1/2 replicas, then repeat A/B at one fixed load. Run from project root."""
import argparse
import datetime
import hashlib
import json
import math
from pathlib import Path
import subprocess
import threading
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
from experiment import percentile, ready, summarize

BASE = ['kubectl', '--context', 'kind-selfheal', '-n', 'selfheal']


def k(*args):
    result = subprocess.run(BASE + list(args), capture_output=True, text=True, timeout=180)
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    return result.stdout


def obj(*args):
    return json.loads(k(*args, '-o', 'json'))


def save(path, data):
    path.write_text(json.dumps(data, indent=2), encoding='utf-8')


def pods():
    return obj('get', 'pods', '-l', 'app=demo-web')['items']


def configure(replicas, strategy='a', image='selfheal-web:v1'):
    unavailable, surge = (1, 0) if strategy == 'b' else (0, 1)
    patch = {'spec': {'replicas': replicas,
             'strategy': {'type': 'RollingUpdate', 'rollingUpdate': {
                 'maxUnavailable': unavailable, 'maxSurge': surge}},
             'template': {'spec': {'containers': [{'name':'web', 'image':image}]}}}}
    k('patch', 'deployment', 'demo-web', '--type=strategic', '-p', json.dumps(patch))


def prepare(replicas):
    configure(replicas)
    k('rollout', 'status', 'deployment/demo-web', '--timeout=150s')
    deadline = time.monotonic()+150
    while True:
        current = pods()
        dep = obj('get', 'deployment', 'demo-web')
        if (len(current) == replicas and all(ready(p) for p in current)
                and all(p['spec']['containers'][0]['image'] == 'selfheal-web:v1' for p in current)
                and dep.get('status',{}).get('availableReplicas',0) == replicas):
            time.sleep(2)
            return current
        if time.monotonic() > deadline:
            raise RuntimeError('stable initial state not reached')
        time.sleep(1)


def app_cpu(current):
    result = {}
    code = ("import json,time;from pathlib import Path;p=Path('/sys/fs/cgroup');"
            "print(json.dumps({'monotonic_s':time.monotonic(),**{n:(p/n).read_text() "
            "for n in ['cpu.stat','cpu.max','memory.current'] if (p/n).exists()}}))")
    for pod in current:
        name = pod['metadata']['name']
        try:
            result[name] = json.loads(k('exec', name, '--', 'python', '-c', code))
        except Exception as exc:
            result[name] = {'error':str(exc)}
    return result


def run_one(session, name, rate, duration, replicas, mode, client_source):
    out = session/name
    out.mkdir(exist_ok=False)
    print('PREPARE', name, flush=True)
    initial = prepare(replicas)
    if mode in ('rollout-a','rollout-b'):
        configure(replicas, 'b' if mode=='rollout-b' else 'a')
    save(out/'initial-pods.json', initial)
    save(out/'initial-deployment.json', obj('get','deployment','demo-web'))
    save(out/'cpu-before.json', app_cpu(initial))
    save(out/'parameters.json', {'rate':rate,'duration':duration,'replicas':replicas,
         'mode':mode,'workers':512,'timeout_s':1,'action_at_client_start_receipt_s':15,
         'action_alignment':'runner receives client-start message; no UTC clock alignment',
         'client_source':Path(client_source).read_text(),
         'runner_source':Path(__file__).read_text()})
    configmap=obj('get','configmap','pressure-scripts')
    save(out/'load-script-configmap.json',configmap)
    if configmap['data']['load.py']!=Path(client_source).read_text():
        raise RuntimeError('ConfigMap source differs from the requested client source')
    first_message = threading.Event()
    started = [None]
    reader_errors = []
    with (out/'requests.jsonl').open('w') as request_file, \
         (out/'client-events.jsonl').open('w') as client_file, \
         (out/'client-stderr.txt').open('w') as error_file, \
         (out/'states.jsonl').open('w') as state_file:
        process = subprocess.Popen(BASE+['exec','loadgen-pressure','--','python','/scripts/load.py',
             '--rate',str(rate),'--duration',str(duration),'--workers','512','--timeout','1'],
             stdout=subprocess.PIPE, stderr=error_file, text=True, bufsize=1)

        def read_client():
            try:
                for line in process.stdout:
                    row = json.loads(line)
                    if row.get('event'):
                        row['runner_received_monotonic_s'] = time.monotonic()
                        client_file.write(json.dumps(row)+'\n'); client_file.flush()
                        if row['event']=='client_started':
                            started[0] = time.monotonic(); first_message.set()
                    else:
                        request_file.write(json.dumps(row)+'\n'); request_file.flush()
            except Exception as exc:
                reader_errors.append(str(exc))

        reader = threading.Thread(target=read_client, daemon=True)
        reader.start()
        action = None
        completion = None
        markers = []
        try:
            if not first_message.wait(30):
                raise RuntimeError('client-start handshake not received')
            print('LOAD', name, rate, 'req/s', duration, 'seconds', flush=True)
            while True:
                loop_start = time.monotonic()
                elapsed = loop_start-started[0]
                if mode.startswith('rollout') and action is None and elapsed >= 15:
                    action = time.monotonic()
                    markers.append({'event':'action_started','elapsed_s':action-started[0]})
                    save(out/'markers.json',markers)
                    k('set','image','deployment/demo-web','web=selfheal-web:v2')
                    print('ACTION', name, flush=True)
                current = pods()
                dep = obj('get','deployment','demo-web')
                endpoints = obj('get','endpointslices','-l','kubernetes.io/service-name=demo-web')
                state_file.write(json.dumps({'elapsed_s':time.monotonic()-started[0],
                    'pods':current,'deployment':dep,'endpointslices':endpoints})+'\n')
                state_file.flush()
                if action is not None and completion is None:
                    active = [p for p in current if not p['metadata'].get('deletionTimestamp')]
                    status = dep.get('status',{})
                    if (len(active)==replicas and all(ready(p) for p in active)
                        and all(p['spec']['containers'][0]['image']=='selfheal-web:v2' for p in active)
                        and status.get('updatedReplicas',0)==replicas
                        and status.get('availableReplicas',0)==replicas
                        and status.get('observedGeneration',0)>=dep['metadata']['generation']):
                        completion = time.monotonic()-action
                        markers.append({'event':'completion_observed','elapsed_s':time.monotonic()-started[0],
                                        'since_action_s':completion})
                        save(out/'markers.json',markers)
                if process.poll() is not None:
                    break
                if elapsed > duration+35:
                    raise RuntimeError('client exceeded deadline')
                time.sleep(max(0,1-(time.monotonic()-loop_start)))
            reader.join(10)
            if process.returncode or reader_errors or reader.is_alive():
                raise RuntimeError(f'client/read failure: {process.returncode} {reader_errors}')
        finally:
            if process.poll() is None:
                process.terminate()
                process.wait(10)
            reader.join(10)
    rows = [json.loads(line) for line in (out/'requests.jsonl').read_text().splitlines()]
    stats = summarize(rows)
    issues = []
    expected = round(rate*duration)
    if len(rows)!=expected or len({r['seq'] for r in rows})!=expected:
        issues.append('request accounting')
    if stats['generator_errors']:
        issues.append('generator saturation')
    if stats['schedule_delay_p95_ms'] is not None and stats['schedule_delay_p95_ms'] > 1000/rate:
        issues.append('p95 scheduling delay exceeds one arrival interval')
    if mode.startswith('rollout') and completion is None:
        issues.append('rollout completion not observed')
    states = [json.loads(line) for line in (out/'states.jsonl').read_text().splitlines()]
    stats.update({'mode':mode,'replicas':replicas,'rate':rate,'duration':duration,
        'completion_observed_s':completion,'validation_issues':issues,
        'min_ready':min(sum(ready(p) for p in s['pods']) for s in states),
        'max_ready':max(sum(ready(p) for p in s['pods']) for s in states),
        'min_available':min(s['deployment'].get('status',{}).get('availableReplicas',0) for s in states),
        'all_attempt_latency_p95_ms':percentile([r['latency_ms'] for r in rows if r.get('latency_ms') is not None],.95)})
    save(out/'summary.json',stats)
    save(out/'cpu-after.json',app_cpu(pods()))
    save(out/'final-pods.json',pods())
    save(out/'events.json',obj('get','events'))
    save(out/'markers.json',markers)
    print('RESULT',name,json.dumps({key:stats[key] for key in ['failed_requests','sent_requests',
       'successful_latency_p95_ms','schedule_delay_p95_ms','generator_errors','min_ready',
       'completion_observed_s','validation_issues']}),flush=True)
    return stats


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('phase', choices=['pilot','rollout'])
    parser.add_argument('--rates', default='50,100,200')
    parser.add_argument('--duration',type=float,default=20)
    parser.add_argument('--repeats',type=int,default=1)
    parser.add_argument('--client-source',default=str(Path(__file__).with_name('load.py')))
    parser.add_argument('--output',required=True)
    args = parser.parse_args()
    session = Path(args.output)
    session.mkdir(parents=True,exist_ok=False)
    initial = obj('get','deployment','demo-web')
    save(session/'original-deployment.json', initial)
    save(session/'nodes.json', obj('get','nodes'))
    save(session/'versions.json',obj('version'))
    save(session/'loadgen-pod.json',obj('get','pod','loadgen-pressure'))
    save(session/'service.json',obj('get','service','demo-web'))
    results=[]
    try:
        for rate_index,rate in enumerate(float(x) for x in args.rates.split(',')):
            for repeat in range(args.repeats):
                if args.phase=='pilot':
                    order=[1,2] if (rate_index+repeat)%2==0 else [2,1]
                    scenarios=[(r,'steady') for r in order]
                else:
                    order=['rollout-a','rollout-b'] if repeat%2==0 else ['rollout-b','rollout-a']
                    scenarios=[(2,m) for m in order]
                for replicas,mode in scenarios:
                    name=f'rate-{rate:g}_repeat-{repeat+1}_n-{replicas}_{mode}'
                    stats=run_one(session,name,rate,args.duration,replicas,mode,args.client_source)
                    results.append({'directory':name,**stats})
                    save(session/'results.json',results)
    except Exception as exc:
        save(session/'error.json',{'error':str(exc)})
        raise
    finally:
        spec=initial['spec']
        patch={'spec':{'replicas':spec['replicas'],'strategy':spec['strategy'],
               'template':{'spec':{'containers':[{'name':'web','image':spec['template']['spec']['containers'][0]['image']}]}}}}
        try:
            k('patch','deployment','demo-web','--type=strategic','-p',json.dumps(patch))
            k('rollout','status','deployment/demo-web','--timeout=150s')
            save(session/'restored-deployment.json',obj('get','deployment','demo-web'))
            print('RESTORED original replicas, image and rollout strategy',flush=True)
        except Exception as exc:
            save(session/'restore-error.json',{'error':str(exc)})
            raise


if __name__=='__main__':
    main()
