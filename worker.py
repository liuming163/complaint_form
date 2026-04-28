#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import atexit
import signal
import threading
import time

from app import (
    UC_WORKER_LOCK_TTL,
    acquire_worker_lock,
    dequeue_uc_task,
    refresh_worker_lock,
    release_worker_lock,
    run_complaint_script,
)


def run_redis_worker():
    token = acquire_worker_lock()
    if not token:
        print('Another Redis worker is already running. Exiting.')
        return 1

    stop_event = threading.Event()

    def cleanup(*_args):
        stop_event.set()
        release_worker_lock(token)
        raise SystemExit(0)

    def keep_lock_alive():
        interval = max(1, UC_WORKER_LOCK_TTL // 3)
        while not stop_event.wait(interval):
            if not refresh_worker_lock(token):
                stop_event.set()
                break

    atexit.register(lambda: release_worker_lock(token))
    signal.signal(signal.SIGTERM, cleanup)
    signal.signal(signal.SIGINT, cleanup)

    heartbeat = threading.Thread(target=keep_lock_alive, daemon=True, name='uc-worker-lock-heartbeat')
    heartbeat.start()

    print('Redis worker started.')
    try:
        while not stop_event.is_set():
            if not refresh_worker_lock(token):
                print('Worker lock lost. Exiting.')
                return 1
            task_payload = dequeue_uc_task(timeout=5)
            if not task_payload:
                continue
            if not refresh_worker_lock(token):
                print('Worker lock lost before task execution. Exiting.')
                return 1
            run_complaint_script(
                task_payload['task_id'],
                task_payload['excel_files'],
                task_payload['cookie'],
                task_payload['proof_file'],
                task_payload.get('other_proof_files', []),
                task_payload['description'],
                task_payload['identity'],
                task_payload['agent'],
                task_payload['rights_holder'],
                task_payload['complaint_category'],
                task_payload.get('copyright_type', ''),
                task_payload['module'],
                task_payload['content_type'],
                task_payload['batch_metadata'],
            )
    finally:
        stop_event.set()
        release_worker_lock(token)


if __name__ == '__main__':
    raise SystemExit(run_redis_worker())
