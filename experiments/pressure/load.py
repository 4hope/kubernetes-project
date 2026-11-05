"""Fixed-rate HTTP/1 client using asyncio, one TCP connection per request."""
import argparse
import asyncio
import json
from pathlib import Path
import socket
import time
from urllib.parse import urlsplit


def cpu_stats():
    root=Path('/sys/fs/cgroup')
    return {name:(root/name).read_text().strip()
            for name in ('cpu.stat','cpu.max','memory.current') if (root/name).exists()}


async def run(url, duration, rate, timeout, workers):
    target=urlsplit(url)
    if target.scheme!='http':
        raise ValueError('This experiment uses HTTP only')
    host=target.hostname
    port=target.port or 80
    address=socket.gethostbyname(host)
    path=target.path or '/'
    if target.query:
        path+='?'+target.query
    message=(f'GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\n'
             'Connection: close\r\n\r\n').encode('ascii')
    start=time.monotonic()
    active=set()

    def emit(row):
        print(json.dumps(row),flush=True)

    async def request(seq,scheduled):
        began=time.monotonic()
        row={'seq':seq,'scheduled_s':scheduled,'start_s':began-start,
             'schedule_delay_ms':max(0,(began-start-scheduled)*1000),
             'status':None,'version':None,'pod':None,'ok':False,
             'error':None,'generator_error':False}
        writer=None
        try:
            async with asyncio.timeout(timeout):
                reader,writer=await asyncio.open_connection(address,port)
                writer.write(message)
                await writer.drain()
                head=await reader.readuntil(b'\r\n\r\n')
                lines=head.decode('iso-8859-1').split('\r\n')
                row['status']=int(lines[0].split()[1])
                headers={key.lower().strip():value.strip() for key,value
                         in (line.split(':',1) for line in lines[1:] if ':' in line)}
                if 'content-length' in headers:
                    raw=await reader.readexactly(int(headers['content-length']))
                else:
                    raw=await reader.read()
                body=json.loads(raw)
                row['version'],row['pod']=body.get('version'),body.get('pod')
                row['ok']=(row['status']==200 and body.get('ready') is True
                           and isinstance(row['version'],str) and isinstance(row['pod'],str))
                if not row['ok']:
                    row['error']='unexpected_response'
        except Exception as exc:
            row['error']=type(exc).__name__+': '+str(exc)
        finally:
            # Measure the entire request, including unsuccessful timeout attempts.
            row['latency_ms']=(time.monotonic()-began)*1000
            row['finish_s']=time.monotonic()-start
            if writer is not None:
                writer.close()
            emit(row)

    emit({'event':'client_started','driver':'async-http1-v1','cpu_stats':cpu_stats(),
          'workers':workers,'rate':rate,'duration':duration,'timeout':timeout,
          'resolved_service_ip':address,'host_header':host,
          'timeout_definition':'one total deadline covers connection and response'})
    for seq in range(round(duration*rate)):
        scheduled=seq/rate
        await asyncio.sleep(max(0,start+scheduled-time.monotonic()))
        if len(active)<workers:
            task=asyncio.create_task(request(seq,scheduled))
            active.add(task)
            task.add_done_callback(active.discard)
        else:
            emit({'seq':seq,'scheduled_s':scheduled,'start_s':time.monotonic()-start,
                  'ok':False,'generator_error':True,'error':'generator_saturated',
                  'status':None,'version':None,'pod':None,
                  'latency_ms':None,'schedule_delay_ms':None})
    await asyncio.sleep(max(0,start+duration-time.monotonic()))
    if active:
        await asyncio.gather(*active)
    emit({'event':'client_finished','elapsed_s':time.monotonic()-start,'cpu_stats':cpu_stats()})


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url',default='http://demo-web:8080/')
    parser.add_argument('--duration',type=float,default=20)
    parser.add_argument('--rate',type=float,default=50)
    parser.add_argument('--timeout',type=float,default=1)
    parser.add_argument('--workers',type=int,default=512)
    args=parser.parse_args()
    if min(args.duration,args.rate,args.timeout,args.workers)<=0:
        parser.error('all numeric parameters must be positive')
    asyncio.run(run(args.url,args.duration,args.rate,args.timeout,args.workers))
