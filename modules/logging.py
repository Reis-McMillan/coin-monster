import logging
import logging.handlers
import queue
import sys
import threading
import httpx

from config import config

_listener = None

class OpenObserveHandler(logging.Handler):
    def __init__(self, endpoint, token, org="default", stream="coin-monster",
                 batch_size=10, flush_interval=5.0):
        super().__init__()
        self.url = f"{endpoint}/api/{org}/{stream}/_json"
        self.token = token
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self._buffer = []
        self._lock = threading.Lock()
        self._client = httpx.Client(timeout=10)
        self._start_flush_timer()

    def _start_flush_timer(self):
        self._timer = threading.Timer(self.flush_interval, self._timed_flush)
        self._timer.daemon = True
        self._timer.start()

    def _timed_flush(self):
        self.flush()
        self._start_flush_timer()

    def emit(self, record):
        entry = {
            "level": record.levelname,
            "message": self.format(record),
            "_timestamp": int(record.created * 1_000_000),
            "logger": record.name,
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        if hasattr(record, "extra_data") and isinstance(record.extra_data, dict):
            entry.update(record.extra_data)

        with self._lock:
            self._buffer.append(entry)
            if len(self._buffer) >= self.batch_size:
                self._send()

    def _send(self):
        if not self._buffer:
            return
        batch = self._buffer[:]
        self._buffer.clear()
        resp = self._client.post(
            self.url,
            json=batch,
            headers={
                "Authorization": f"Basic {self.token}",
                "Content-Type": "application/json",
            },
        )
        resp.raise_for_status()

    def flush(self):
        with self._lock:
            self._send()

    def close(self):
        self._timer.cancel()
        self.flush()
        self._client.close()
        super().close()


def setup_logging(level=logging.INFO):
    global _listener

    root = logging.getLogger()
    root.setLevel(level)

    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    stream_handler = logging.StreamHandler(stream=sys.stdout)
    stream_handler.setFormatter(formatter)

    handlers = [stream_handler]

    if getattr(config, "LOGGING_ENABLED", False):
        token = getattr(config, "OPENOBSERVE_TOKEN", None)
        endpoint = getattr(config, "OPENOBSERVE_ENDPOINT", None)
        if token and endpoint:
            oo_handler = OpenObserveHandler(endpoint, token)
            oo_handler.setFormatter(formatter)
            handlers.append(oo_handler)

    log_queue = queue.Queue(-1)
    queue_handler = logging.handlers.QueueHandler(log_queue)
    root.addHandler(queue_handler)

    _listener = logging.handlers.QueueListener(
        log_queue, *handlers, respect_handler_level=True
    )
    _listener.start()


def shutdown_logging():
    global _listener
    if _listener:
        _listener.stop()
        _listener = None
