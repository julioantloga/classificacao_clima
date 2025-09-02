import json, time, threading, queue
from typing import Dict, Any

class ProgressBus:
    """
    Canal de mensagens por job_id para SSE.
    """
    def __init__(self):
        self._channels: Dict[str, "queue.Queue[dict]"] = {}
        self._lock = threading.Lock()

    def open(self, job_id: str):
        with self._lock:
            self._channels[job_id] = queue.Queue()

    def close(self, job_id: str):
        with self._lock:
            self._channels.pop(job_id, None)

    def put(self, job_id: str, payload: Dict[str, Any]):
        ch = self._channels.get(job_id)
        if ch:
            ch.put(payload)

    def stream(self, job_id: str):
        """
        Generator para SSE: vai emitindo eventos enquanto houver mensagens.
        """
        ch = self._channels.get(job_id)
        if ch is None:
            # stream vazio encerra
            yield "event: done\ndata: {}\n\n"
            return

        # Mensagem inicial
        yield "event: start\ndata: {}\n\n"

        # loop de consumo
        while True:
            try:
                msg = ch.get(timeout=30)  # fecha se 30s sem eventos
            except queue.Empty:
                yield "event: ping\ndata: {}\n\n"
                continue
            if msg.get("event") == "done":
                # última mensagem
                yield "event: done\ndata: {}\n\n"
                break
            yield f"data: {json.dumps(msg, ensure_ascii=False)}\n\n"

progress_bus = ProgressBus()

def timed_step(job_id: str, name: str, fn, *args, **kwargs):
    """
    Executa uma função, publica eventos de início/fim com tempo.
    """
    t0 = time.perf_counter()
    progress_bus.put(job_id, {"event": "step_start", "step": name})
    result = fn(*args, **kwargs)
    dt = time.perf_counter() - t0
    progress_bus.put(job_id, {"event": "step_done", "step": name, "elapsed_sec": round(dt, 3)})
    return result
