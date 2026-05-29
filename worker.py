#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import atexit
import signal
import threading

from app import (
    UC_WORKER_LOCK_TTL,
    acquire_worker_lock,
    dequeue_unified_task,
    refresh_worker_lock,
    release_worker_lock,
    run_baidu_complaint_script,
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

    heartbeat = threading.Thread(target=keep_lock_alive, daemon=True, name='worker-lock-heartbeat')
    heartbeat.start()

    print('Redis worker started (unified queue).')
    try:
        while not stop_event.is_set():
            if not refresh_worker_lock(token):
                print('Worker lock lost. Exiting.')
                return 1

            task_payload = dequeue_unified_task(timeout=2)
            if not task_payload:
                continue

            if not refresh_worker_lock(token):
                print('Worker lock lost before task execution. Exiting.')
                return 1

            platform = task_payload.get('platform', '')

            if platform == 'uc':
                print(f"[UC] 执行任务: {task_payload.get('task_id')}")
                run_complaint_script(task_payload)
            elif platform == 'baidu':
                print(f"[Baidu] 执行任务: {task_payload.get('task_id')}")
                run_baidu_complaint_script(
                    task_payload['task_id'],
                    task_payload['cookie'],
                    task_payload['complaint_product'],
                    task_payload['complaint_type_code'],
                    task_payload['works_config'],
                    task_payload['total_batches'],
                )
            else:
                print(f"[Unknown] 未知平台: {platform}, task_id={task_payload.get('task_id')}")

    finally:
        stop_event.set()
        release_worker_lock(token)


if __name__ == '__main__':
    raise SystemExit(run_redis_worker())
