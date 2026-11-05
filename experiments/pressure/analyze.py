"""Recalculate the separate pressure series from raw records; optional matplotlib figures."""
import argparse
import collections
import csv
import hashlib
import json
import math
from pathlib import Path
import statistics


def percentile(values, fraction=.95):
    return sorted(values)[max(0,math.ceil(len(values)*fraction)-1)] if values else None


def parse_stat(raw):
    return {key:int(value) for key,value in (line.split() for line in raw.splitlines())}


def read_run(directory):
    rows=[json.loads(line) for line in (directory/'requests.jsonl').read_text().splitlines()]
    params=json.loads((directory/'parameters.json').read_text())
    summary=json.loads((directory/'summary.json').read_text())
    sent=[r for r in rows if not r['generator_error']]
    good=[r for r in sent if r['ok']]
    failed=len(sent)-len(good)
    issues=[]
    expected=round(params['rate']*params['duration'])
    if len(rows)!=expected or {r['seq'] for r in rows}!=set(range(expected)):
        issues.append('request accounting')
    if failed!=summary['failed_requests'] or len(sent)!=summary['sent_requests']:
        issues.append('summary mismatch')
    if len(sent)!=len(rows):
        issues.append('generator saturation')
    schedule_p95=percentile([r['schedule_delay_ms'] for r in sent])
    if schedule_p95 is not None and schedule_p95>1000/params['rate']:
        issues.append('generator schedule p95 exceeds one arrival interval')
    events=[json.loads(line) for line in (directory/'client-events.jsonl').read_text().splitlines()]
    first=next(e for e in events if e['event']=='client_started')
    last=next(e for e in events if e['event']=='client_finished')
    driver_cpu_before=parse_stat(first['cpu_stats']['cpu.stat'])
    driver_cpu_after=parse_stat(last['cpu_stats']['cpu.stat'])
    driver_cpu=(driver_cpu_after['usage_usec']-driver_cpu_before['usage_usec'])/1e6/last['elapsed_s']
    driver_throttled=driver_cpu_after['nr_throttled']-driver_cpu_before['nr_throttled']
    states=[json.loads(line) for line in (directory/'states.jsonl').read_text().splitlines()]
    def ready(p):
        return not p['metadata'].get('deletionTimestamp') and any(c['type']=='Ready' and c['status']=='True' for c in p.get('status',{}).get('conditions',[]))
    counts=[sum(ready(p) for p in state['pods']) for state in states]
    gaps=[b['elapsed_s']-a['elapsed_s'] for a,b in zip(states,states[1:])]
    tail=[r for r in sorted(good,key=lambda r:r['seq'])[-100:]]
    # The intervention is after the runner receives the start message +15 s.
    # Stop this diagnostic window at client time 14 s to leave a one-second margin.
    pre=[r for r in sent if r['start_s']<14]
    before=json.loads((directory/'cpu-before.json').read_text())
    after=json.loads((directory/'cpu-after.json').read_text())
    app_cpu=[]
    throttled=[]
    for name in before.keys() & after.keys():
        a,b=before[name],after[name]
        if 'cpu.stat' in a and 'cpu.stat' in b:
            aa,bb=parse_stat(a['cpu.stat']),parse_stat(b['cpu.stat'])
            app_cpu.append((bb['usage_usec']-aa['usage_usec'])/1e6/(b['monotonic_s']-a['monotonic_s']))
            throttled.append(bb['nr_throttled']-aa['nr_throttled'])
    if params['mode'].startswith('rollout'):
        if summary['completion_observed_s'] is None:
            issues.append('rollout not completed')
        if len(tail)!=100 or any(r['version']!='v2' for r in tail):
            issues.append('final 100 successful responses not all v2')
    return {'run':str(directory),'session':directory.parent.name,
        'driver':first.get('driver','threaded-urllib-v1'),
        'mode':params['mode'],'replicas':params['replicas'],
        'rate':params['rate'],'duration_s':params['duration'],'scheduled':len(rows),'sent':len(sent),
        'failed':failed,'failed_percent':100*failed/len(sent) if sent else None,
        'generator_errors':len(rows)-len(sent),'schedule_p95_ms':schedule_p95,
        'success_p95_ms':percentile([r['latency_ms'] for r in good]),
        'all_attempt_p95_ms':percentile([r['latency_ms'] for r in sent]),
        'completion_s':summary['completion_observed_s'],'min_ready':min(counts),'max_ready':max(counts),
        'pre_action_0_14s_sent':len(pre),
        'pre_action_0_14s_failed':sum(not r['ok'] for r in pre),
        'max_sample_gap_s':max(gaps,default=0),'driver_mean_cpu_cores':driver_cpu,
        'driver_throttled_periods':driver_throttled,'app_mean_cpu_cores':app_cpu,
        'app_throttled_periods':throttled,'error_types':dict(collections.Counter(r['error'] for r in sent if not r['ok'])),
        'issues':issues}


def number(value,digits=2):
    return '—' if value is None else f'{value:.{digits}f}'


def write_report(analysis_path,out_path,confirmation_session,rollout_session):
    args=argparse.Namespace(analysis=str(analysis_path),out=str(out_path),
        confirmation=confirmation_session,rollout=rollout_session)
    rows=json.loads(Path(args.analysis).read_text(encoding='utf-8'))
    confirmation=[r for r in rows if r['session']==args.confirmation]
    rollout=[r for r in rows if r['session']==args.rollout]
    if len(confirmation)!=6 or len(rollout)!=6:
        raise RuntimeError(f'Expected 6 confirmation and 6 rollout runs, got {len(confirmation)}, {len(rollout)}')
    selected=confirmation+rollout
    if any(r['issues'] for r in selected):
        raise RuntimeError('Selected series contains validation issues; review before final reporting')
    if len({r['rate'] for r in selected})!=1:
        raise RuntimeError('Selected conditions must use one identical offered load')
    rate=selected[0]['rate']
    a=[r for r in rollout if r['mode']=='rollout-a']
    b=[r for r in rollout if r['mode']=='rollout-b']
    if len(a)!=3 or len(b)!=3:
        raise RuntimeError('Need three repeats of each rollout configuration')
    mean=lambda group,key:statistics.mean(r[key] for r in group)
    failures=lambda group:sum(r['failed'] for r in group)
    sent=lambda group:sum(r['sent'] for r in group)
    pct=lambda group:100*failures(group)/sent(group)
    lines=['# Дополнительный эксперимент: A/B при повышенной нагрузке','',
      '## Исследовательский вопрос','',
      'Даёт ли сохранение двух готовых копий при обновлении практическое преимущество для клиента, когда одна копия уже испытывает затруднения? Число реплик задаётся вручную; HPA не применяется.','',
      '## Что выполнено','',
      f'- Исследовательская калибровка позволила выбрать {rate:g} запросов/с.',
      '- Выбранная точка проверена отдельно: по три прогона с одной и двумя копиями, в чередующемся порядке.',
      f'- Затем выполнены три A и три B, по {rollout[0]["duration_s"]:g} секунд каждый, с одним и тем же клиентом, нагрузкой и лимитами приложения.',
      '- A: maxUnavailable=0, maxSurge=1. B: maxUnavailable=1, maxSurge=0. Каждый rollout начинается с двух готовых v1; действие запускается примерно на 15-й секунде.',
      '- Во всех выбранных прогонах учтены все запланированные попытки, насыщения генератора и нарушений порога p95 опоздания не обнаружено.','',
      '## Независимая проверка одной и двух копий','',
      '| Копии | Повтор | Запросы | Ошибки | Ошибки, % | p95 успешных, мс | p95 опоздания клиента, мс |',
      '|---:|---:|---:|---:|---:|---:|---:|']
    for replicas in [1,2]:
        for i,r in enumerate([r for r in confirmation if r['replicas']==replicas],1):
            lines.append(f'| {replicas} | {i} | {r["sent"]} | {r["failed"]} | {number(r["failed_percent"])} | {number(r["success_p95_ms"])} | {number(r["schedule_p95_ms"])} |')
    lines+=['','## Основное сравнение обновлений','',
      '| Стратегия | Повтор | Запросы | Ошибки | Ошибки, % | До действия: ошибки / запросы (0–14 с) | p95 успешных, мс | Завершение, с | Готовые Pod |',
      '|---|---:|---:|---:|---:|---|---:|---:|---|']
    for label,group in [('A',a),('B',b)]:
        for i,r in enumerate(group,1):
            lines.append(f'| {label} | {i} | {r["sent"]} | {r["failed"]} | {number(r["failed_percent"])} | {r["pre_action_0_14s_failed"]} / {r["pre_action_0_14s_sent"]} | {number(r["success_p95_ms"])} | {number(r["completion_s"])} | {r["min_ready"]}–{r["max_ready"]} |')
    lines+=['',f'Всего A: {failures(a)} ошибок из {sent(a)} запросов ({number(pct(a))}%).',
      f'Всего B: {failures(b)} ошибок из {sent(b)} запросов ({number(pct(b))}%).','',
      f'Среднее наблюдаемое время обновления: A {number(mean(a,"completion_s"))} с; B {number(mean(b,"completion_s"))} с.',
      f'Среднее отдельных p95 успешных запросов: A {number(mean(a,"success_p95_ms"))} мс; B {number(mean(b,"success_p95_ms"))} мс. Это не общий p95 объединённой выборки.','',
      '## Вывод по данным','']
    if pct(a)<pct(b):
        lines.append('В этой записанной серии суммарная доля ошибок A меньше, чем B. По наблюдаемому числу готовых копий стратегии действительно отличаются. Однако одно сравнение суммарных ошибок не изолирует причинный вклад стратегии: ниже отдельно проверены ошибки до действия. Нельзя объявлять A универсально безошибочной или приписывать каждую ошибку только обновлению.')
    elif pct(a)==pct(b):
        lines.append('По доле ошибок эти прогоны не показали преимущества одной стратегии. Сохранять следует именно этот результат; увеличение нагрузки не гарантирует заранее желаемого различия.')
    else:
        lines.append('В этой серии A дала больше ошибок, чем B. Это не подтверждает ожидаемое преимущество A; нужно отдельно исследовать состояние копий, маршрутизацию, генератор и ресурсы, не подменяя наблюдение ожиданием.')
    baseline_bad=[r for r in rollout if r['pre_action_0_14s_failed']]
    if baseline_bad:
        lines+=['',f'**В {len(baseline_bad)} из 6 прогонов ошибки наблюдались уже до действия**, в первые 14 секунд клиента при двух готовых копиях. Это контрольный признак нестабильности условия «две копии справляются» между прогонами. Эти записи сохранены и не исключены задним числом. Для строгого причинного утверждения о влиянии A/B нужны дополнительные контролируемые измерения; текущая серия показывает наблюдаемую разницу и её ограничение.']
    if any(r['min_ready']==0 for r in b):
        lines+=['','В одном B наблюдалось даже 0 готовых незавершающихся Pod. Бюджет maxUnavailable=1 ограничивает плановые действия обновления, но не гарантирует готовность оставшейся копии при нагрузке: сохранённые состояния показывают её переход в Ready=False, пока новая копия ещё не готова. Это отдельное наблюдение; утверждение «B всегда оставляет одну готовую копию» было бы неверным.']
    lines+=['','## Что ограничивает вывод','',
      '- Используется один локальный узел kind на общем компьютере и по три повтора. Нагрузка, клиент и состояние среды отличаются от исходной серии 10 запросов/с: напрямую объединять её задержки с новой серией нельзя.',
      '- Клиент асинхронный, использует фиксированную ставку запросов, до 512 одновременных соединений и один общий тайм-аут 1 с. Лимиты клиента 2 CPU / 512 MiB; лимиты каждой копии приложения 250m CPU / 128 MiB.',
      '- Ранние потоковые пилоты сохранены, включая невалидные по расписанию/насыщению. Они исключены из подтверждающих таблиц.',
      '- p95 успешных запросов не включает тайм-ауты; ошибки необходимо рассматривать отдельно. Длительности всех фактически отправленных попыток также сохранены и анализируются.',
      '- Опрос состояния примерно раз в секунду; момент завершения включает задержку API и первого наблюдения. Цель сравнения не сводится к поиску самого быстрого обновления.',
      '- Календарные часы не используются для совмещения шкал времени. Запуск действия отсчитывается от получения стартового сообщения клиента; доставка сообщения вносит небольшую неопределённость.',
      '- CPU/throttling-снимки поддерживают диагностику, но эксперимент сам по себе не изолирует вклад CPU, очереди HTTP-сервера, логирования и сети. Из результатов нельзя получить универсальное оптимальное число реплик.',
      '- После каждой серии runner восстанавливает исходные replicas, image и strategy. Подтверждения сохранены в restored-deployment.json.','',
      '## Как это объяснить на защите','',
      'Первоначально при 10 запросах/с обе стратегии дали ноль ошибок, хотя B временно оставляла одну готовую копию. Поэтому мы отдельно подобрали и подтвердили нагрузку, на которой одна копия испытывает затруднения. Затем сравнили A и B при одинаковых условиях. Главный вопрос — влияние сохранённой доступной ёмкости на запросы клиента; время обновления рассматривается вместе с этим результатом.','',
      '## Воспроизводимость и помощь ИИ','',
      'Сырые данные: experiments/pressure/results/. Описание запуска и пересчёта: раздел Run the pressure extension в корневом README.md. JSON/CSV содержат показатели каждого прогона и контрольные суммы исходных файлов.','',
      'Codex подготовил и запустил эту дополнительную серию по запросу студентки, проверил клиент и пересчитал результаты. Первоначальные 12 кластерных прогонов выполняла студентка. Это различие следует сохранить в итоговом описании использования ИИ.','']
    Path(args.out).write_text('\n'.join(lines),encoding='utf-8')
    print(json.dumps({'report':args.out,'rate':rate,'A_failures':failures(a),'A_requests':sent(a),
       'B_failures':failures(b),'B_requests':sent(b),'A_failure_percent':pct(a),'B_failure_percent':pct(b)},ensure_ascii=False))

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--results',default='experiments/pressure/results')
    parser.add_argument('--out',default='experiments/pressure/analysis')
    parser.add_argument('--report',action='store_true')
    parser.add_argument('--confirmation-session',default='20261029_async_confirmation_04')
    parser.add_argument('--rollout-session',default='20261029_async_rollout_05')
    args=parser.parse_args()
    root,out=Path(args.results),Path(args.out)
    out.mkdir(parents=True,exist_ok=True)
    runs=[read_run(p.parent) for p in sorted(root.glob('*/*/summary.json'))]
    (out/'analysis.json').write_text(json.dumps(runs,indent=2),encoding='utf-8')
    if runs:
        with (out/'runs.csv').open('w',newline='',encoding='utf-8') as file:
            writer=csv.DictWriter(file,fieldnames=list(runs[0]));writer.writeheader();writer.writerows(runs)
    checksums=[]
    for file in sorted(root.rglob('*')):
        if file.is_file():
            checksums.append({'path':str(file),'sha256':hashlib.sha256(file.read_bytes()).hexdigest()})
    (out/'raw-sha256.json').write_text(json.dumps(checksums,indent=2),encoding='utf-8')
    if args.report:
        write_report(out/'analysis.json',out/'report-ru.md',args.confirmation_session,args.rollout_session)
    for row in runs:
        print(Path(row['run']).name, 'failed',row['failed'],'/',row['sent'],
              'p95_ms',round(row['success_p95_ms'],2) if row['success_p95_ms'] is not None else None,
              'driver_issues',row['issues'],flush=True)
    try:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
    except ImportError:
        print('Matplotlib unavailable; numerical analysis and checksums saved.')
        return
    plt.rcParams.update({'font.family':'DejaVu Sans','font.size':11})
    pilots=[r for r in runs if r['mode']=='steady' and r['driver']=='async-http1-v1' and not r['issues']]
    if pilots:
        fig,axes=plt.subplots(1,2,figsize=(11.5,4.1),layout='constrained')
        for n,color in [(1,'#B36625'),(2,'#087F73')]:
            points=sorted([r for r in pilots if r['replicas']==n],key=lambda r:r['rate'])
            for ax,key in zip(axes,['failed_percent','success_p95_ms']):
                ax.scatter([r['rate'] for r in points],[r[key] for r in points],color=color,label=f'{n} ready replica(s)',s=36)
                ax.set_xlabel('Offered requests/s');ax.grid(alpha=.18);ax.legend()
        axes[0].set_ylabel('Failed requests (%)');axes[1].set_ylabel('Successful request p95 (ms)')
        fig.suptitle('Calibration: one and two replicas, identical per-Pod limits')
        for suffix in ['png','svg']:
            fig.savefig(out/f'calibration.{suffix}',dpi=180)
        plt.close(fig)
    rollouts=[r for r in runs if r['mode'].startswith('rollout') and not r['issues']]
    if rollouts:
        fig,axes=plt.subplots(1,3,figsize=(13,4),layout='constrained')
        for ax,key,title in zip(axes,['failed_percent','success_p95_ms','completion_s'],['Request failures (%)','Successful request p95 (ms)','Observed completion (s)']):
            for x,mode,color in [(0,'rollout-a','#355FD2'),(1,'rollout-b','#087F73')]:
                values=[r[key] for r in rollouts if r['mode']==mode]
                ax.scatter([x+(i-(len(values)-1)/2)*.06 for i in range(len(values))],values,color=color,s=40)
                if values:
                    ax.hlines(statistics.mean(values),x-.18,x+.18,color=color,lw=2)
            ax.set_xticks([0,1],['A','B']);ax.set_title(title);ax.grid(axis='y',alpha=.18)
        for suffix in ['png','svg']:
            fig.savefig(out/f'rollout-comparison.{suffix}',dpi=180)
        plt.close(fig)


if __name__=='__main__':
    main()
