#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import atexit
import signal
import threading
import time

from app import (
    UC_WORKER_LOCK_TTL,
    acquire_worker_lock,
    dequeue_baidu_task,
    dequeue_uc_task,
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

    print('Redis worker started (UC + Baidu).')
    try:
        while not stop_event.is_set():
            if not refresh_worker_lock(token):
                print('Worker lock lost. Exiting.')
                return 1

            # 尝试 UC 队列
            task_payload = dequeue_uc_task(timeout=1)
            if task_payload:
                if not refresh_worker_lock(token):
                    print('Worker lock lost before UC task execution. Exiting.')
                    return 1
                run_complaint_script(task_payload)
                continue

            # 尝试百度队列
            task_payload = dequeue_baidu_task(timeout=1)
            if task_payload:
                if not refresh_worker_lock(token):
                    print('Worker lock lost before Baidu task execution. Exiting.')
                    return 1
                run_baidu_complaint_script(
                    task_payload['task_id'],
                    task_payload['cookie'],
                    task_payload['complaint_product'],
                    task_payload['complaint_type_code'],
                    task_payload['works_config'],
                    task_payload['total_batches'],
                )
                continue

            # 两个队列都为空，brpop 已等待过，直接继续循环

    finally:
        stop_event.set()
        release_worker_lock(token)


if __name__ == '__main__':
    raise SystemExit(run_redis_worker())
