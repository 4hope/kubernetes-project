import json
import os
import signal
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

VERSION = os.environ.get("APP_VERSION", "unknown")
POD = os.environ.get("POD_NAME", "local")
STARTUP_SECONDS = float(os.environ.get("STARTUP_SECONDS", "8"))
STARTED = time.monotonic()


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        ready = time.monotonic() - STARTED >= STARTUP_SECONDS

        if self.path not in ("/", "/ready"):
            status = 404
        else:
            status = 200 if ready else 503

        body = (json.dumps({
            "version": VERSION,
            "pod": POD,
            "ready": ready,
        }) + "\n").encode()

        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        if self.path != "/ready":
            super().log_message(fmt, *args)


server = ThreadingHTTPServer(("0.0.0.0", 8080), Handler)
server.daemon_threads = False


def stop(signum, frame):
    print(f"Stopping pod={POD}", flush=True)
    threading.Thread(target=server.shutdown, daemon=True).start()


signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)

print(f"Starting version={VERSION} pod={POD}", flush=True)
try:
    server.serve_forever()
finally:
    server.server_close()
