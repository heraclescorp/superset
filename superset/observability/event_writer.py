# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""A bounded PID-owned JSON stdout queue, independent of OTLP transport.

Only pre-encoded safe JSON strings enter this queue. Business threads never
write/flush stdout or wait for delivery. A separate daemon owns output so a full
pipe cannot stall requests, transactions, metrics or trace exporting. Shutdown
waits only its configured deadline and accounts for discarded pending events;
an already-blocked write is not falsely reported as a confirmed drop. No retry
can duplicate a partly written event. Children discard inherited queues/locks
and create a fresh writer lazily. Unlike QueueListener.stop(), shutdown never
performs an unbounded join on a listener that might be blocked in stdout.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from collections import deque
from typing import TextIO

_LOCK = threading.Lock()
_WRITER: EventWriter | None = None
# Keep parent-owned TextIO handles inert: cyclic GC must not invoke an inherited
# flush/close lock in a child. The OS reclaims these descriptors at process exit;
# child delivery always owns a fresh handle/queue and never exports parent data.
_INHERITED_WRITERS: list[EventWriter] = []
_SINK: TextIO | None = None
_MAX_QUEUE_SIZE = 2048
_SHUTDOWN_TIMEOUT = 0.25
_FAILED_ENQUEUES = 0


class EventWriter:
    """Serialize bounded output without holding producer locks across any I/O."""

    def __init__(
        self, sink: TextIO, max_queue_size: int, *, owns_sink: bool = False
    ) -> None:
        self.pid = os.getpid()
        self.sink = sink
        self.owns_sink = owns_sink
        self.limit = max_queue_size
        self.queue: deque[str] = deque()
        self.condition = threading.Condition()
        self.stopped = False
        self.inflight = False
        self.dropped = 0
        self.failures = 0
        self.thread = threading.Thread(
            target=self.run, name="SupersetEventWriter", daemon=True
        )
        self.thread.start()

    def enqueue(self, encoded: str) -> None:
        """Never wait for stdout; reject new records once the queue is full."""
        if self.pid != os.getpid():
            return
        with self.condition:
            if self.stopped or len(self.queue) >= self.limit:
                self.dropped += 1
                return
            self.queue.append(encoded)
            self.condition.notify()

    def run(self) -> None:
        """Only this daemon may write, flush or close its private stdout handle."""
        try:
            while True:
                with self.condition:
                    while not self.queue and not self.stopped:
                        self.condition.wait()
                    if not self.queue:
                        return
                    encoded = self.queue.popleft()
                    self.inflight = True
                try:
                    self.sink.write(encoded + "\n")
                    self.sink.flush()
                except Exception:  # pylint: disable=broad-except
                    # No recursive logging of stream exceptions or payloads.
                    with self.condition:
                        self.failures += 1
                finally:
                    with self.condition:
                        self.inflight = False
                        self.condition.notify_all()
        finally:
            if self.owns_sink:
                with self.condition:
                    self.inflight = True
                try:
                    self.sink.close()
                except Exception:  # pylint: disable=broad-except
                    with self.condition:
                        self.failures += 1
                finally:
                    with self.condition:
                        self.inflight = False
                        self.condition.notify_all()

    def flush(self, timeout: float) -> bool:
        """Wait a bounded time for already-enqueued output, never on callers."""
        if self.pid != os.getpid():
            return False
        deadline = time.monotonic() + max(0, timeout)
        with self.condition:
            while self.queue or self.inflight:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self.condition.wait(remaining)
            return True

    def shutdown(self, timeout: float) -> None:
        """Bound shutdown even when the operating system's stdout pipe is full."""
        if self.pid != os.getpid():
            return
        with self.condition:
            self.stopped = True
            self.condition.notify_all()
        self.thread.join(timeout=max(0, timeout))
        if self.thread.is_alive():
            with self.condition:
                self.dropped += len(self.queue)
                self.queue.clear()
                self.condition.notify_all()

    def diagnostics(self) -> dict[str, int]:
        """Read delivery health without waiting on the output stream."""
        with self.condition:
            return {
                "queue_size": len(self.queue),
                "inflight": int(self.inflight),
                "dropped": self.dropped,
                "failures": self.failures,
            }


def configure(
    *, max_queue_size: object = 2048, shutdown_timeout: object = 0.25
) -> None:
    """Store bounded settings only; Flask/Gunicorn preload starts no thread."""
    global _MAX_QUEUE_SIZE, _SHUTDOWN_TIMEOUT
    try:
        _MAX_QUEUE_SIZE = max(64, min(16384, int(max_queue_size)))  # type: ignore[arg-type]
        _SHUTDOWN_TIMEOUT = max(0, min(2, float(shutdown_timeout)))  # type: ignore[arg-type]
    except (TypeError, ValueError, OverflowError):
        _MAX_QUEUE_SIZE = 2048
        _SHUTDOWN_TIMEOUT = 0.25


def _get_writer() -> EventWriter:
    global _WRITER
    if _WRITER is not None and _WRITER.pid == os.getpid():
        return _WRITER
    with _LOCK:
        if _WRITER is None or _WRITER.pid != os.getpid():
            # A fresh postfork TextIO handle avoids inheriting Python stdout
            # locks held by a parent's writer. FD1 also bypasses Celery's
            # LoggingProxy/root ERROR level without changing process stdout.
            sink = _SINK
            owns_sink = sink is None
            if sink is None:
                descriptor = os.dup(1)
                try:
                    sink = os.fdopen(descriptor, "w", encoding="utf-8", buffering=1)
                except Exception:
                    os.close(descriptor)
                    raise
            try:
                _WRITER = EventWriter(sink, _MAX_QUEUE_SIZE, owns_sink=owns_sink)
            except Exception:
                if owns_sink:
                    sink.close()
                raise
        return _WRITER


class EventQueueHandler(logging.Handler):
    """The named logger only enqueues pre-encoded JSON; never stdout I/O."""

    def emit(self, record: logging.LogRecord) -> None:
        global _FAILED_ENQUEUES
        try:
            if isinstance(record.msg, str) and not record.args:
                _get_writer().enqueue(record.msg)
        except Exception:  # pylint: disable=broad-except
            with _LOCK:
                _FAILED_ENQUEUES += 1


def flush(timeout: float = 1) -> bool:
    """Bounded delivery barrier for shutdown/tests, not business emit paths."""
    writer = _WRITER
    return writer is None or writer.flush(timeout)


def shutdown(timeout: float | None = None) -> None:
    """Stop only this PID's writer, without closing shared process stdout."""
    writer = _WRITER
    if writer is not None and writer.pid == os.getpid():
        writer.shutdown(_SHUTDOWN_TIMEOUT if timeout is None else timeout)


def diagnostics() -> dict[str, int]:
    """Native SDK callbacks can observe health without creating a writer."""
    with _LOCK:
        writer = _WRITER
        failed_enqueues = _FAILED_ENQUEUES
    values = (
        writer.diagnostics()
        if writer is not None and writer.pid == os.getpid()
        else {"queue_size": 0, "inflight": 0, "dropped": 0, "failures": 0}
    )
    values["dropped"] += failed_enqueues
    values["failures"] += failed_enqueues
    return values


def _after_fork() -> None:
    global _LOCK, _WRITER, _FAILED_ENQUEUES
    if _WRITER is not None:
        _INHERITED_WRITERS.append(_WRITER)
    _LOCK = threading.Lock()
    _WRITER = None
    _FAILED_ENQUEUES = 0


configure(
    max_queue_size=os.getenv("SUPERSET_OBSERVABILITY_EVENT_MAX_QUEUE_SIZE", "2048"),
    shutdown_timeout=os.getenv(
        "SUPERSET_OBSERVABILITY_EVENT_SHUTDOWN_TIMEOUT_SECONDS", "0.25"
    ),
)

if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork)
