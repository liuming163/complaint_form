# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import math
import os
import queue
import shutil
import subprocess
import threading
import zipfile
import io
import redis
from datetime import datetime
from pathlib import Path
from openpyxl import load_workbook
from uuid import uuid4

import pandas as pd
from flask import Flask, render_template, request, jsonify, send_file
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from werkzeug.utils import secure_filename

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

BASE_DIR = os.path.dirname(__file__)
if load_dotenv:
    load_dotenv(os.path.join(BASE_DIR, '.env'))

app = Flask(__name__)
app.config['SECRET_KEY'] = 'complaint-form-secret'
app.config['UPLOAD_FOLDER'] = os.path.join(os.path.dirname(__file__), 'uploads')
app.config['UC_SUBMISSION_FOLDER'] = os.path.join(app.config['UPLOAD_FOLDER'], 'uc_submissions')
app.config['TASK_RESULT_FOLDER'] = os.path.join(os.path.dirname(__file__), 'task_results')
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0

DB_HOST = os.getenv('DB_HOST', '127.0.0.1')
DB_PORT = os.getenv('DB_PORT', '3306')
DB_NAME = os.getenv('DB_NAME', 'complaint_form')
DB_USER = os.getenv('DB_USER', 'navicat')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'navicat123')
DATABASE_URL = os.getenv(
    'DATABASE_URL',
    f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4'
)
REDIS_URL = os.getenv('REDIS_URL', 'redis://127.0.0.1:6379/0')
UC_QUEUE_NAME = os.getenv('UC_QUEUE_NAME', 'uc_complaint_queue')
UC_WORKER_LOCK_KEY = os.getenv('UC_WORKER_LOCK_KEY', 'uc_complaint_worker_lock')
UC_WORKER_LOCK_TTL = int(os.getenv('UC_WORKER_LOCK_TTL', '15'))

engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

# 任务状态存储（生产环境建议用数据库）
tasks = {}

@app.after_request
def no_cache(response):
    response.headers['Cache-Control'] = 'no-store'
    return response

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['UC_SUBMISSION_FOLDER'], exist_ok=True)
os.makedirs(app.config['TASK_RESULT_FOLDER'], exist_ok=True)


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def save_uploaded_file(file_storage, target_dir, prefix=None):
    if not file_storage or not file_storage.filename:
        return None

    original_name = Path(file_storage.filename).name
    suffix = Path(original_name).suffix.lower()
    stem = secure_filename(Path(original_name).stem)

    if not suffix:
        suffix = Path(original_name).suffix

    if prefix:
        filename = f"{prefix}{suffix}" if suffix else prefix
    else:
        filename = f"{stem}{suffix}" if stem else None

    if not filename:
        return None

    save_path = os.path.join(target_dir, filename)
    file_storage.save(save_path)
    return filename




def create_submission_dir():
    submission_id = datetime.now().strftime('%Y%m%d_%H%M%S_') + uuid4().hex[:8]
    submission_dir = os.path.join(app.config['UC_SUBMISSION_FOLDER'], submission_id)
    ensure_dir(submission_dir)
    return submission_id, submission_dir


def split_excel_into_batches(df, batch_dir, batch_size=200):
    ensure_dir(batch_dir)
    batches = []

    for index, start in enumerate(range(0, len(df), batch_size), start=1):
        end = min(start + batch_size, len(df))
        batch_df = df.iloc[start:end].copy()
        batch_filename = f'part_{index:03d}.xlsx'
        batch_path = os.path.join(batch_dir, batch_filename)
        batch_df.to_excel(batch_path, index=False)
        batches.append({
            'batch_no': index,
            'filename': batch_filename,
            'path': batch_path,
            'start_row': start + 1,
            'end_row': end,
            'rows': len(batch_df),
        })

    return batches


def load_task_result(task_id):
    result_path = os.path.join(app.config['TASK_RESULT_FOLDER'], f'{task_id}.json')
    if not os.path.exists(result_path):
        return None

    with open(result_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def get_redis_client():
    return redis.Redis.from_url(REDIS_URL, decode_responses=True)


def enqueue_uc_task(task_payload):
    client = get_redis_client()
    client.lpush(UC_QUEUE_NAME, json.dumps(task_payload, ensure_ascii=False))


def dequeue_uc_task(timeout=0):
    client = get_redis_client()
    item = client.brpop(UC_QUEUE_NAME, timeout=timeout)
    if not item:
        return None
    _, payload = item
    return json.loads(payload)


def acquire_worker_lock(ttl_seconds=None):
    client = get_redis_client()
    token = uuid4().hex
    ttl = ttl_seconds or UC_WORKER_LOCK_TTL
    acquired = client.set(UC_WORKER_LOCK_KEY, token, nx=True, ex=ttl)
    return token if acquired else None


def refresh_worker_lock(token, ttl_seconds=None):
    client = get_redis_client()
    ttl = ttl_seconds or UC_WORKER_LOCK_TTL
    current = client.get(UC_WORKER_LOCK_KEY)
    if current != token:
        return False
    client.expire(UC_WORKER_LOCK_KEY, ttl)
    return True


def release_worker_lock(token):
    client = get_redis_client()
    current = client.get(UC_WORKER_LOCK_KEY)
    if current == token:
        client.delete(UC_WORKER_LOCK_KEY)


@app.route('/')
def index():
    return render_template('index.html', is_index=True)


@app.route('/works')
def works():
    return render_template('works.html')


# 投诉账号管理数据文件
ACCOUNTS_FILE = os.path.join(os.path.dirname(__file__), 'task_results', 'accounts.json')

# 被代理人信息数据文件
PRINCIPALS_FILE = os.path.join(os.path.dirname(__file__), 'task_results', 'principals.json')

# 平台映射
PLATFORM_MAP = {
    'uc': {'platform_name': 'UC', 'pingtai': 'UC'},
    'quark': {'platform_name': '夸克', 'pingtai': '夸克'},
}


def get_db_session():
    return SessionLocal()


def normalize_datetime(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def row_to_account_dict(row):
    return {
        'id': row.account_id,
        'platform_code': row.platform_code,
        'platform_name': row.platform_name,
        'pingtai': row.platform_label,
        'user': row.account_user,
        'cookie': row.cookie_text,
        'account_purpose': row.account_purpose,
        'status': row.status,
        'created_at': normalize_datetime(row.created_at),
        'updated_at': normalize_datetime(row.updated_at),
    }


def guess_mime_type(filename):
    suffix = Path(filename).suffix.lower()
    mapping = {
        '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        '.xls': 'application/vnd.ms-excel',
        '.png': 'image/png',
        '.jpg': 'image/jpeg',
        '.jpeg': 'image/jpeg',
        '.pdf': 'application/pdf',
        '.bmp': 'image/bmp',
    }
    return mapping.get(suffix, 'application/octet-stream')


def build_file_asset_row(business_type, business_id, category, local_path, saved_name, original_name=None):
    path_obj = Path(local_path)
    return {
        'asset_id': uuid4().hex[:12],
        'business_type': business_type,
        'business_id': business_id,
        'category': category,
        'storage_type': 'local',
        'bucket_name': None,
        'object_key': None,
        'local_path': str(path_obj),
        'original_name': original_name or saved_name,
        'saved_name': saved_name,
        'mime_type': guess_mime_type(saved_name),
        'file_size': path_obj.stat().st_size if path_obj.exists() else 0,
        'file_hash': None,
        'created_at': datetime.now(),
    }


def insert_file_asset(asset_row):
    with get_db_session() as session:
        exists = session.execute(text("""
            SELECT 1 FROM file_assets
            WHERE business_type = :business_type
              AND business_id = :business_id
              AND category = :category
              AND saved_name = :saved_name
            LIMIT 1
        """), {
            'business_type': asset_row['business_type'],
            'business_id': asset_row['business_id'],
            'category': asset_row['category'],
            'saved_name': asset_row['saved_name'],
        }).first()
        if exists:
            return False
        session.execute(text("""
            INSERT INTO file_assets (
                asset_id, business_type, business_id, category, storage_type,
                bucket_name, object_key, local_path, original_name, saved_name,
                mime_type, file_size, file_hash, created_at
            ) VALUES (
                :asset_id, :business_type, :business_id, :category, :storage_type,
                :bucket_name, :object_key, :local_path, :original_name, :saved_name,
                :mime_type, :file_size, :file_hash, :created_at
            )
        """), asset_row)
        session.commit()
        return True


def register_submission_files(submission_id, submission_dir, saved_files, batches, original_names=None):
    original_names = original_names or {}

    if saved_files.get('excel_file'):
        insert_file_asset(build_file_asset_row(
            'submission', submission_id, 'excel_source',
            os.path.join(submission_dir, saved_files['excel_file']),
            saved_files['excel_file'],
            original_names.get('excel_file')
        ))

    if saved_files.get('proof_file'):
        insert_file_asset(build_file_asset_row(
            'submission', submission_id, 'proof_file',
            os.path.join(submission_dir, saved_files['proof_file']),
            saved_files['proof_file'],
            original_names.get('proof_file')
        ))

    other_original_names = original_names.get('other_proof_files', [])
    for index, saved_name in enumerate(saved_files.get('other_proof_files', [])):
        insert_file_asset(build_file_asset_row(
            'submission', submission_id, 'other_proof_file',
            os.path.join(submission_dir, saved_name),
            saved_name,
            other_original_names[index] if index < len(other_original_names) else saved_name
        ))

    for batch in batches:
        insert_file_asset(build_file_asset_row(
            'batch', submission_id, 'excel_batch',
            os.path.join(submission_dir, 'batches', batch['filename']),
            batch['filename'],
            batch['filename']
        ))


def migrate_submission_file_assets_if_needed():
    submissions_root = Path(app.config['UC_SUBMISSION_FOLDER'])
    if not submissions_root.exists():
        return

    for item in sorted(submissions_root.iterdir()):
        if not item.is_dir() or item.name.startswith('.'):
            continue
        submission_file = item / 'submission.json'
        if not submission_file.exists():
            continue
        with submission_file.open('r', encoding='utf-8') as f:
            payload = json.load(f)

        submission_id = payload.get('submission_id', item.name)
        files = payload.get('files', {})
        saved_files = {
            'excel_file': files.get('excel_file'),
            'proof_file': files.get('proof_file'),
            'other_proof_files': files.get('other_proof_files', []),
        }
        register_submission_files(
            submission_id,
            str(item),
            saved_files,
            payload.get('batches', []),
            {
                'excel_file': files.get('excel_file'),
                'proof_file': files.get('proof_file'),
                'other_proof_files': files.get('other_proof_files', []),
            }
        )


def load_accounts():
    with get_db_session() as session:
        rows = session.execute(text("""
            SELECT account_id, platform_code, platform_name, platform_label,
                   account_user, cookie_text, account_purpose, status,
                   created_at, updated_at
            FROM accounts
            ORDER BY id ASC
        """)).mappings().all()
        return [row_to_account_dict(row) for row in rows]


def load_principals_map():
    with get_db_session() as session:
        rows = session.execute(text("""
            SELECT g.group_id, g.platform_code, g.platform_name, g.account_user,
                   i.principal_name, g.created_at, g.updated_at
            FROM principal_groups g
            LEFT JOIN principal_items i ON i.group_id = g.group_id
            ORDER BY g.id ASC, i.id ASC
        """)).mappings().all()

    principals_map = {}
    for row in rows:
        key = f"{row.platform_code}:{row.account_user}"
        entry = principals_map.setdefault(key, {
            'group_id': row.group_id,
            'platform_code': row.platform_code,
            'platform_name': row.platform_name,
            'account_user': row.account_user,
            'principals': [],
            'created_at': normalize_datetime(row.created_at),
            'updated_at': normalize_datetime(row.updated_at),
        })
        if row.principal_name:
            entry['principals'].append(row.principal_name)
    return principals_map


def serialize_complaint_numbers(value):
    if value is None:
        return json.dumps([])
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False)


def deserialize_complaint_numbers(value):
    if not value:
        return []
    if isinstance(value, (list, tuple)):
        return list(value)
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return []


def map_task_status_label(status):
    if status == 'running':
        return '执行中'
    if status == 'completed':
        return '已完成'
    if status == 'failed':
        return '失败'
    if status == 'pending' or status == 'queued':
        return '等待中'
    if status == 'partial_failed':
        return '部分失败'
    return status or '未知'


def insert_complaint_submission(payload, rights_holder):
    submitted_at = datetime.fromisoformat(payload['submitted_at'])
    work_name = payload['form'].get('作品名称') or None
    with get_db_session() as session:
        session.execute(text("""
            INSERT INTO complaint_submissions (
                submission_id, platform_code, collect_account, cookie_snapshot,
                identity_type, agent_name, principal_name, rights_holder_name,
                complaint_category, complaint_type, module_name, content_type,
                description_text, work_name, excel_rows, batch_size, batch_count,
                status, submitted_at, created_at, updated_at
            ) VALUES (
                :submission_id, :platform_code, :collect_account, :cookie_snapshot,
                :identity_type, :agent_name, :principal_name, :rights_holder_name,
                :complaint_category, :complaint_type, :module_name, :content_type,
                :description_text, :work_name, :excel_rows, :batch_size, :batch_count,
                :status, :submitted_at, :created_at, :updated_at
            )
        """), {
            'submission_id': payload['submission_id'],
            'platform_code': 'uc',
            'collect_account': payload['form'].get('collect_account', ''),
            'cookie_snapshot': payload['form'].get('cookie', ''),
            'identity_type': payload['form'].get('identity', ''),
            'agent_name': payload['form'].get('agent', ''),
            'principal_name': payload['form'].get('principal') or None,
            'rights_holder_name': rights_holder,
            'complaint_category': payload['form'].get('complaint_category', ''),
            'complaint_type': payload['form'].get('complaint_type', ''),
            'module_name': payload['form'].get('module', ''),
            'content_type': payload['form'].get('content_type', ''),
            'description_text': payload['form'].get('description', ''),
            'work_name': work_name,
            'excel_rows': payload.get('excel_rows', 0),
            'batch_size': payload.get('batch_size', 0),
            'batch_count': payload.get('batch_count', 0),
            'status': 'pending',
            'submitted_at': submitted_at,
            'created_at': submitted_at,
            'updated_at': submitted_at,
        })
        session.commit()


def insert_complaint_task(task_id, submission_id, submitted_at, batch_count, excel_rows):
    dt = datetime.fromisoformat(submitted_at)
    with get_db_session() as session:
        session.execute(text("""
            INSERT INTO complaint_tasks (
                task_id, submission_id, queue_name, status, current_batch, batch_count,
                completed_batches, failed_batches, complaint_numbers_json, error_message,
                submitted_at, queued_at, created_at, updated_at
            ) VALUES (
                :task_id, :submission_id, :queue_name, :status, :current_batch, :batch_count,
                :completed_batches, :failed_batches, :complaint_numbers_json, :error_message,
                :submitted_at, :queued_at, :created_at, :updated_at
            )
        """), {
            'task_id': task_id,
            'submission_id': submission_id,
            'queue_name': 'uc_complaint',
            'status': 'pending',
            'current_batch': 0,
            'batch_count': batch_count,
            'completed_batches': 0,
            'failed_batches': 0,
            'complaint_numbers_json': serialize_complaint_numbers([]),
            'error_message': None,
            'submitted_at': dt,
            'queued_at': dt,
            'created_at': dt,
            'updated_at': dt,
        })
        session.commit()


def insert_complaint_batches(submission_id, batches):
    with get_db_session() as session:
        for batch in batches:
            now = datetime.now()
            session.execute(text("""
                INSERT INTO complaint_batches (
                    batch_id, submission_id, batch_no, source_asset_id, batch_filename,
                    start_row, end_row, row_count, status, complaint_number,
                    error_message, created_at, updated_at
                ) VALUES (
                    :batch_id, :submission_id, :batch_no, :source_asset_id, :batch_filename,
                    :start_row, :end_row, :row_count, :status, :complaint_number,
                    :error_message, :created_at, :updated_at
                )
            """), {
                'batch_id': uuid4().hex[:12],
                'submission_id': submission_id,
                'batch_no': batch['batch_no'],
                'source_asset_id': None,
                'batch_filename': batch.get('filename'),
                'start_row': batch.get('start_row', 0),
                'end_row': batch.get('end_row', 0),
                'row_count': batch.get('rows', 0),
                'status': 'pending',
                'complaint_number': None,
                'error_message': None,
                'created_at': now,
                'updated_at': now,
            })
        session.commit()


def update_complaint_task(task_id, **fields):
    if not fields:
        return
    allowed = {
        'status', 'current_batch', 'batch_count', 'completed_batches', 'failed_batches',
        'complaint_numbers_json', 'error_message', 'worker_name', 'redis_job_id',
        'submitted_at', 'queued_at', 'started_at', 'completed_at'
    }
    updates = {k: v for k, v in fields.items() if k in allowed}
    if 'complaint_numbers_json' in updates:
        updates['complaint_numbers_json'] = serialize_complaint_numbers(updates['complaint_numbers_json'])
    if not updates:
        return
    updates['updated_at'] = datetime.now()
    set_clause = ', '.join(f"{key} = :{key}" for key in updates.keys())
    updates['task_id'] = task_id
    with get_db_session() as session:
        session.execute(text(f"UPDATE complaint_tasks SET {set_clause} WHERE task_id = :task_id"), updates)
        session.commit()


def update_complaint_batch(submission_id, batch_no, **fields):
    if not fields:
        return
    allowed = {'status', 'complaint_number', 'error_message'}
    updates = {k: v for k, v in fields.items() if k in allowed}
    if not updates:
        return
    updates['updated_at'] = datetime.now()
    updates['submission_id'] = submission_id
    updates['batch_no'] = batch_no
    set_clause = ', '.join(f"{key} = :{key}" for key in updates.keys() if key not in {'submission_id', 'batch_no'})
    with get_db_session() as session:
        session.execute(text(f"""
            UPDATE complaint_batches
            SET {set_clause}
            WHERE submission_id = :submission_id AND batch_no = :batch_no
        """), updates)
        session.commit()


def get_complaint_task(task_id):
    with get_db_session() as session:
        task = session.execute(text("""
            SELECT task_id, submission_id, status, current_batch, batch_count,
                   completed_batches, failed_batches, complaint_numbers_json,
                   error_message, submitted_at, queued_at, started_at, completed_at
            FROM complaint_tasks
            WHERE task_id = :task_id
            LIMIT 1
        """), {'task_id': task_id}).mappings().first()
        if not task:
            return None
        batches = session.execute(text("""
            SELECT batch_no, row_count, start_row, end_row, batch_filename,
                   status, complaint_number, error_message
            FROM complaint_batches
            WHERE submission_id = :submission_id
            ORDER BY batch_no ASC
        """), {'submission_id': task['submission_id']}).mappings().all()

    complaint_numbers = deserialize_complaint_numbers(task.get('complaint_numbers_json'))
    batch_items = []
    for batch in batches:
        batch_items.append({
            'batch_no': batch['batch_no'],
            'rows': batch['row_count'],
            'start_row': batch['start_row'],
            'end_row': batch['end_row'],
            'filename': batch.get('batch_filename'),
            'status': batch['status'],
            'error': batch.get('error_message'),
            'complaint_number': batch.get('complaint_number'),
        })
    complaint_number = complaint_numbers[0] if complaint_numbers else None
    return {
        'task_id': task['task_id'],
        'submission_id': task['submission_id'],
        'status': task['status'],
        'complaint_number': complaint_number,
        'complaint_numbers': complaint_numbers,
        'batch_count': task['batch_count'],
        'completed_batches': task['completed_batches'],
        'failed_batches': task['failed_batches'],
        'current_batch': task['current_batch'],
        'batches': batch_items,
        'error': task.get('error_message'),
        'submitted_at': normalize_datetime(task.get('submitted_at')),
        'started_at': normalize_datetime(task.get('started_at')),
        'completed_at': normalize_datetime(task.get('completed_at')),
    }


def get_submission_status_list():
    with get_db_session() as session:
        rows = session.execute(text("""
            SELECT s.submission_id, s.submitted_at, s.collect_account, s.work_name,
                   s.excel_rows, s.batch_count, t.status, t.complaint_numbers_json
            FROM complaint_submissions s
            LEFT JOIN complaint_tasks t ON t.submission_id = s.submission_id
            WHERE s.platform_code = 'uc'
            ORDER BY s.submitted_at DESC
        """)).mappings().all()

    items = []
    for row in rows:
        items.append({
            'submission_id': row['submission_id'],
            'submitted_at': normalize_datetime(row.get('submitted_at')),
            'collect_account': row.get('collect_account') or '',
            'work_name': row.get('work_name') or '',
            'excel_rows': row.get('excel_rows') or 0,
            'batch_count': row.get('batch_count') or 0,
            'status': map_task_status_label(row.get('status')),
            'complaint_numbers': deserialize_complaint_numbers(row.get('complaint_numbers_json')),
        })
    return items


def migrate_submission_and_task_data_if_needed():
    submissions_root = Path(app.config['UC_SUBMISSION_FOLDER'])
    results_root = Path(app.config['TASK_RESULT_FOLDER'])
    if not submissions_root.exists():
        return

    with get_db_session() as session:
        existing_submission_ids = {
            row[0] for row in session.execute(text("SELECT submission_id FROM complaint_submissions")).all()
        }
        existing_task_ids = {
            row[0] for row in session.execute(text("SELECT task_id FROM complaint_tasks")).all()
        }
        existing_batch_keys = {
            (row[0], row[1]) for row in session.execute(text("SELECT submission_id, batch_no FROM complaint_batches")).all()
        }

    for item in sorted(submissions_root.iterdir()):
        if not item.is_dir() or item.name.startswith('.'):
            continue
        submission_file = item / 'submission.json'
        if not submission_file.exists():
            continue
        with submission_file.open('r', encoding='utf-8') as f:
            payload = json.load(f)

        submission_id = payload.get('submission_id', item.name)
        task_id = f'uc_{submission_id}'
        rights_holder = payload.get('form', {}).get('principal') or payload.get('form', {}).get('agent') or ''

        if submission_id not in existing_submission_ids:
            insert_complaint_submission(payload, rights_holder)
            existing_submission_ids.add(submission_id)

        if task_id not in existing_task_ids:
            insert_complaint_task(task_id, submission_id, payload.get('submitted_at', datetime.now().isoformat()), payload.get('batch_count', 0), payload.get('excel_rows', 0))
            existing_task_ids.add(task_id)

        pending_batch_inserts = []
        for batch in payload.get('batches', []):
            key = (submission_id, batch['batch_no'])
            if key in existing_batch_keys:
                continue
            pending_batch_inserts.append(batch)
            existing_batch_keys.add(key)
        if pending_batch_inserts:
            insert_complaint_batches(submission_id, pending_batch_inserts)

        result_file = results_root / f'{task_id}.json'
        if result_file.exists():
            with result_file.open('r', encoding='utf-8') as f:
                task_result = json.load(f)
            update_fields = {
                'status': task_result.get('status'),
                'current_batch': task_result.get('current_batch'),
                'completed_batches': task_result.get('completed_batches'),
                'failed_batches': task_result.get('failed_batches'),
                'complaint_numbers_json': task_result.get('complaint_numbers', []),
                'error_message': task_result.get('error'),
            }
            if task_result.get('started_at'):
                update_fields['started_at'] = datetime.fromisoformat(task_result['started_at'])
            if task_result.get('completed_at'):
                update_fields['completed_at'] = datetime.fromisoformat(task_result['completed_at'])
            update_complaint_task(task_id, **update_fields)
            for batch in task_result.get('batches', []):
                update_complaint_batch(
                    submission_id,
                    batch['batch_no'],
                    status=batch.get('status'),
                    complaint_number=batch.get('complaint_number'),
                    error_message=batch.get('error')
                )

    with get_db_session() as session:
        session.execute(text("""
            UPDATE complaint_submissions s
            JOIN complaint_tasks t ON t.submission_id = s.submission_id
            SET s.status = t.status, s.updated_at = NOW()
            WHERE s.platform_code = 'uc'
        """))
        session.commit()


def migrate_json_seed_data_if_needed():
    with get_db_session() as session:
        account_count = session.execute(text("SELECT COUNT(*) FROM accounts")).scalar_one()
        group_count = session.execute(text("SELECT COUNT(*) FROM principal_groups")).scalar_one()

        if account_count == 0 and os.path.exists(ACCOUNTS_FILE):
            with open(ACCOUNTS_FILE, 'r', encoding='utf-8') as f:
                accounts_data = json.load(f)
            for item in accounts_data:
                created_at = datetime.fromisoformat(item['created_at']) if item.get('created_at') else datetime.now()
                updated_at = datetime.fromisoformat(item['updated_at']) if item.get('updated_at') else created_at
                session.execute(text("""
                    INSERT INTO accounts (
                        account_id, platform_code, platform_name, platform_label,
                        account_user, cookie_text, account_purpose, status,
                        created_at, updated_at
                    ) VALUES (
                        :account_id, :platform_code, :platform_name, :platform_label,
                        :account_user, :cookie_text, :account_purpose, :status,
                        :created_at, :updated_at
                    )
                """), {
                    'account_id': item['id'],
                    'platform_code': item['platform_code'],
                    'platform_name': item.get('platform_name') or PLATFORM_MAP.get(item['platform_code'], {}).get('platform_name', item['platform_code']),
                    'platform_label': item.get('pingtai') or item.get('platform_name'),
                    'account_user': item['user'],
                    'cookie_text': item['cookie'],
                    'account_purpose': item.get('account_purpose') or None,
                    'status': 'active' if item.get('status') in {'0', 0, 'active', None, ''} else str(item.get('status')),
                    'created_at': created_at,
                    'updated_at': updated_at,
                })

        if group_count == 0 and os.path.exists(PRINCIPALS_FILE):
            with open(PRINCIPALS_FILE, 'r', encoding='utf-8') as f:
                principals_data = json.load(f)
            for key, item in principals_data.items():
                group_id = uuid4().hex[:12]
                created_at = datetime.fromisoformat(item['created_at']) if item.get('created_at') else datetime.now()
                updated_at = datetime.fromisoformat(item['updated_at']) if item.get('updated_at') else created_at
                session.execute(text("""
                    INSERT INTO principal_groups (
                        group_id, platform_code, platform_name, account_user, created_at, updated_at
                    ) VALUES (
                        :group_id, :platform_code, :platform_name, :account_user, :created_at, :updated_at
                    )
                """), {
                    'group_id': group_id,
                    'platform_code': item['platform_code'],
                    'platform_name': item.get('platform_name') or PLATFORM_MAP.get(item['platform_code'], {}).get('platform_name', item['platform_code']),
                    'account_user': item['account_user'],
                    'created_at': created_at,
                    'updated_at': updated_at,
                })
                for principal_name in item.get('principals', []):
                    session.execute(text("""
                        INSERT INTO principal_items (
                            item_id, group_id, principal_name, created_at
                        ) VALUES (
                            :item_id, :group_id, :principal_name, :created_at
                        )
                    """), {
                        'item_id': uuid4().hex[:12],
                        'group_id': group_id,
                        'principal_name': principal_name,
                        'created_at': created_at,
                    })

        session.commit()


migrate_json_seed_data_if_needed()
migrate_submission_and_task_data_if_needed()
migrate_submission_file_assets_if_needed()


@app.route('/accounts')
def accounts():
    return render_template('accounts.html')


@app.route('/principals')
def principals():
    return render_template('principals.html')


@app.route('/api/accounts/list')
def accounts_list():
    platform_code = request.args.get('platform_code')
    accounts = load_accounts()
    if platform_code:
        accounts = [a for a in accounts if a.get('platform_code') == platform_code]
    return jsonify({'success': True, 'data': accounts})


@app.route('/api/accounts/add', methods=['POST'])
def accounts_add():
    data = request.get_json()
    platform_code = data.get('platform_code', '').strip()
    user = data.get('user', '').strip()
    cookie = data.get('cookie', '').strip()
    if not platform_code or not user or not cookie:
        return jsonify({'success': False, 'error': '平台名称、投诉账号、Cookie都不能为空'}), 400
    if platform_code not in PLATFORM_MAP:
        return jsonify({'success': False, 'error': '平台编码无效'}), 400

    with get_db_session() as session:
        exists = session.execute(text("""
            SELECT 1 FROM accounts
            WHERE platform_code = :platform_code AND account_user = :account_user
            LIMIT 1
        """), {'platform_code': platform_code, 'account_user': user}).first()
        if exists:
            return jsonify({'success': False, 'error': f'该平台下投诉账号「{user}」已存在'}), 400

        new_id = uuid4().hex[:12]
        now = datetime.now()
        session.execute(text("""
            INSERT INTO accounts (
                account_id, platform_code, platform_name, platform_label,
                account_user, cookie_text, account_purpose, status,
                created_at, updated_at
            ) VALUES (
                :account_id, :platform_code, :platform_name, :platform_label,
                :account_user, :cookie_text, :account_purpose, :status,
                :created_at, :updated_at
            )
        """), {
            'account_id': new_id,
            'platform_code': platform_code,
            'platform_name': PLATFORM_MAP[platform_code]['platform_name'],
            'platform_label': PLATFORM_MAP[platform_code]['pingtai'],
            'account_user': user,
            'cookie_text': cookie,
            'account_purpose': data.get('account_purpose', '').strip() or None,
            'status': 'active',
            'created_at': now,
            'updated_at': now,
        })
        session.commit()

        row = session.execute(text("""
            SELECT account_id, platform_code, platform_name, platform_label,
                   account_user, cookie_text, account_purpose, status,
                   created_at, updated_at
            FROM accounts
            WHERE account_id = :account_id
            LIMIT 1
        """), {'account_id': new_id}).mappings().one()

    return jsonify({'success': True, 'data': row_to_account_dict(row)})


@app.route('/api/accounts/update_cookie', methods=['POST'])
def accounts_update_cookie():
    data = request.get_json()
    acc_id = data.get('id')
    cookie = data.get('cookie', '').strip()
    if not cookie:
        return jsonify({'success': False, 'error': 'Cookie不能为空'}), 400

    with get_db_session() as session:
        result = session.execute(text("""
            UPDATE accounts
            SET cookie_text = :cookie_text, updated_at = :updated_at
            WHERE account_id = :account_id
        """), {
            'cookie_text': cookie,
            'updated_at': datetime.now(),
            'account_id': acc_id,
        })
        session.commit()
        if result.rowcount == 0:
            return jsonify({'success': False, 'error': '账号不存在'}), 404

    return jsonify({'success': True})


@app.route('/api/principals/list')
def principals_list():
    """返回所有账号及其被代理人信息，每行一个被代理人"""
    principals_data = load_principals_map()
    accounts = load_accounts()
    results = []
    for acc in accounts:
        key = f"{acc['platform_code']}:{acc['user']}"
        entry = principals_data.get(key, {})
        principals = entry.get('principals', [])
        count = len(principals) if principals else 1
        if principals:
            for i, name in enumerate(principals):
                results.append({
                    'platform_code': acc['platform_code'],
                    'platform_name': acc.get('platform_name', ''),
                    'account_user': acc['user'],
                    'account_purpose': acc.get('account_purpose', ''),
                    'principal_name': name,
                    'rowspan': count if i == 0 else 0,
                })
        else:
            results.append({
                'platform_code': acc['platform_code'],
                'platform_name': acc.get('platform_name', ''),
                'account_user': acc['user'],
                'account_purpose': acc.get('account_purpose', ''),
                'principal_name': '-',
                'rowspan': 1,
            })
    return jsonify({'success': True, 'data': results})


@app.route('/api/principals/add', methods=['POST'])
def principals_add():
    """添加被代理人信息，按 (platform_code + account_user) 分组"""
    data = request.get_json()
    platform_code = data.get('platform_code', '').strip()
    account_user = data.get('account_user', '').strip()
    principal_name = data.get('principal_name', '').strip()

    if not platform_code or not account_user or not principal_name:
        return jsonify({'success': False, 'error': '平台名称、投诉账号、被代理人信息都不能为空'}), 400
    if platform_code not in PLATFORM_MAP:
        return jsonify({'success': False, 'error': '平台编码无效'}), 400

    with get_db_session() as session:
        account_exists = session.execute(text("""
            SELECT platform_name FROM accounts
            WHERE platform_code = :platform_code AND account_user = :account_user
            LIMIT 1
        """), {
            'platform_code': platform_code,
            'account_user': account_user,
        }).mappings().first()
        if not account_exists:
            return jsonify({'success': False, 'error': '投诉账号不存在'}), 400

        group = session.execute(text("""
            SELECT group_id, platform_name
            FROM principal_groups
            WHERE platform_code = :platform_code AND account_user = :account_user
            LIMIT 1
        """), {
            'platform_code': platform_code,
            'account_user': account_user,
        }).mappings().first()

        if not group:
            group_id = uuid4().hex[:12]
            now = datetime.now()
            session.execute(text("""
                INSERT INTO principal_groups (
                    group_id, platform_code, platform_name, account_user, created_at, updated_at
                ) VALUES (
                    :group_id, :platform_code, :platform_name, :account_user, :created_at, :updated_at
                )
            """), {
                'group_id': group_id,
                'platform_code': platform_code,
                'platform_name': PLATFORM_MAP[platform_code]['platform_name'],
                'account_user': account_user,
                'created_at': now,
                'updated_at': now,
            })
        else:
            group_id = group['group_id']

        exists = session.execute(text("""
            SELECT 1 FROM principal_items
            WHERE group_id = :group_id AND principal_name = :principal_name
            LIMIT 1
        """), {
            'group_id': group_id,
            'principal_name': principal_name,
        }).first()
        if exists:
            return jsonify({'success': False, 'error': '该被代理人信息已存在'}), 400

        session.execute(text("""
            INSERT INTO principal_items (
                item_id, group_id, principal_name, created_at
            ) VALUES (
                :item_id, :group_id, :principal_name, :created_at
            )
        """), {
            'item_id': uuid4().hex[:12],
            'group_id': group_id,
            'principal_name': principal_name,
            'created_at': datetime.now(),
        })
        session.execute(text("""
            UPDATE principal_groups
            SET updated_at = :updated_at
            WHERE group_id = :group_id
        """), {
            'updated_at': datetime.now(),
            'group_id': group_id,
        })
        session.commit()

    return jsonify({'success': True, 'data': {
        'platform_code': platform_code,
        'platform_name': PLATFORM_MAP[platform_code]['platform_name'],
        'account_user': account_user,
        'principal_name': principal_name,
    }})


@app.route('/api/works/list')
def works_list():
    """返回 static/imgs/剧名/ 下的所有文件夹名称"""
    drama_base = os.path.join(os.path.dirname(__file__), 'static', 'imgs', '剧名')
    if not os.path.isdir(drama_base):
        return jsonify({'success': True, 'data': []})
    folders = sorted([d for d in os.listdir(drama_base) if os.path.isdir(os.path.join(drama_base, d)) and not d.startswith('.')])
    return jsonify({'success': True, 'data': folders})


@app.route('/kuake')
def kuake():
    return render_template('kuake.html')


@app.route('/uc')
def uc():
    return render_template('uc.html')


@app.route('/api/check_excel', methods=['POST'])
def check_excel():
    if 'file' not in request.files:
        return jsonify({'success': False, 'error': '未上传文件'})
    f = request.files['file']
    if not f.filename:
        return jsonify({'success': False, 'error': '文件为空'})

    filename = secure_filename(f.filename)
    save_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    f.save(save_path)

    try:
        df = pd.read_excel(save_path)
        rows = len(df)
        if rows <= 0:
            return jsonify({'success': False, 'error': 'Excel 文件没有可提交的数据行'}), 400
        batch_size = 200
        batch_count = math.ceil(rows / batch_size)
        return jsonify({
            'success': True,
            'rows': rows,
            'filename': filename,
            'batch_size': batch_size,
            'batch_count': batch_count,
        })
    except Exception as e:
        return jsonify({'success': False, 'error': f'文件解析失败：{str(e)}'})
    finally:
        if os.path.exists(save_path):
            os.remove(save_path)


@app.route('/api/uc/submit', methods=['POST'])
def submit_uc_form():
    required_fields = {
        'collect_account': '采集账号',
        'cookie': 'Cookie',
        'identity': '您的身份',
        'agent': '代理人/权利人',
        'complaint_category': '投诉大类',
        'complaint_type': '投诉类型',
        'module': '功能模块',
        'content_type': '内容类型',
        'description': '投诉内容描述',
    }

    identity = request.form.get('identity', '').strip()
    missing_fields = [label for key, label in required_fields.items() if not request.form.get(key, '').strip()]

    if identity == '代理人' and not request.form.get('principal', '').strip():
        missing_fields.append('被代理人（权利人）信息')

    if not request.files.get('excel_file') or not request.files['excel_file'].filename:
        missing_fields.append('Excel批量导入')
    if not request.files.get('proof_file') or not request.files['proof_file'].filename:
        missing_fields.append('证明文件')

    if missing_fields:
        return jsonify({'success': False, 'error': '缺少必填项：' + '、'.join(missing_fields)}), 400

    excel_file = request.files['excel_file']
    excel_name = secure_filename(excel_file.filename)
    excel_ext = Path(excel_name).suffix.lower()
    if excel_ext not in {'.xlsx', '.xls'}:
        return jsonify({'success': False, 'error': 'Excel 文件格式不正确，请上传 .xlsx 或 .xls 文件'}), 400

    submission_id, submission_dir = create_submission_dir()

    try:
        excel_filename = save_uploaded_file(excel_file, submission_dir, 'excel')
        excel_path = os.path.join(submission_dir, excel_filename)
        df = pd.read_excel(excel_path)
        rows = len(df)
        if rows <= 0:
            return jsonify({'success': False, 'error': 'Excel 文件没有可提交的数据行'}), 400
        batch_size = 200
        batch_dir = os.path.join(submission_dir, 'batches')
        batches = split_excel_into_batches(df, batch_dir, batch_size=batch_size)

        saved_files = {
            'excel_file': excel_filename,
            'proof_file': save_uploaded_file(request.files.get('proof_file'), submission_dir, 'proof'),
            'other_proof_files': []
        }

        if not saved_files['proof_file']:
            return jsonify({'success': False, 'error': '证明文件保存失败'}), 400

        for index, file_storage in enumerate(request.files.getlist('other_proof_file')):
            saved_name = save_uploaded_file(file_storage, submission_dir, f'other_{index + 1}')
            if saved_name:
                saved_files['other_proof_files'].append(saved_name)

        payload = {
            'submission_id': submission_id,
            'submitted_at': datetime.now().isoformat(),
            'form': {
                'collect_account': request.form.get('collect_account', '').strip(),
                'cookie': request.form.get('cookie', '').strip(),
                'identity': request.form.get('identity', '').strip(),
                'agent': request.form.get('agent', '').strip(),
                'principal': request.form.get('principal', '').strip(),
                'complaint_category': request.form.get('complaint_category', '').strip(),
                'complaint_type': request.form.get('complaint_type', '').strip(),
                'module': request.form.get('module', '').strip(),
                'content_type': request.form.get('content_type', '').strip(),
                'description': request.form.get('description', '').strip(),
                '作品名称': request.form.get('作品名称', '').strip(),
            },
            'excel_rows': rows,
            'batch_size': batch_size,
            'batch_count': len(batches),
            'batches': [
                {
                    'batch_no': batch['batch_no'],
                    'filename': batch['filename'],
                    'start_row': batch['start_row'],
                    'end_row': batch['end_row'],
                    'rows': batch['rows'],
                }
                for batch in batches
            ],
            'files': saved_files,
        }

        rights_holder = payload['form']['principal'] if identity == '代理人' else payload['form']['agent']

        metadata_path = os.path.join(submission_dir, 'submission.json')
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        task_id = f"uc_{submission_id}"
        task_state = {
            'status': 'pending',
            'submission_id': submission_id,
            'submitted_at': payload['submitted_at'],
            'queued_at': payload['submitted_at'],
            'excel_rows': rows,
            'batch_count': len(batches),
            'completed_batches': 0,
            'failed_batches': 0,
            'current_batch': 0,
            'complaint_numbers': [],
            'batches': [
                {
                    'batch_no': batch['batch_no'],
                    'rows': batch['rows'],
                    'start_row': batch['start_row'],
                    'end_row': batch['end_row'],
                    'filename': batch['filename'],
                    'status': 'pending',
                    'error': None,
                }
                for batch in batches
            ],
        }
        tasks[task_id] = task_state
        insert_complaint_submission(payload, rights_holder)
        insert_complaint_task(task_id, submission_id, payload['submitted_at'], len(batches), rows)
        insert_complaint_batches(submission_id, payload['batches'])
        register_submission_files(
            submission_id,
            submission_dir,
            saved_files,
            payload['batches'],
            {
                'excel_file': excel_file.filename,
                'proof_file': request.files.get('proof_file').filename if request.files.get('proof_file') else saved_files['proof_file'],
                'other_proof_files': [f.filename for f in request.files.getlist('other_proof_file') if f and f.filename],
            }
        )

        proof_file_path = os.path.join(submission_dir, saved_files['proof_file'])
        other_proof_paths = [os.path.join(submission_dir, f) for f in saved_files['other_proof_files']]

        # 追加自定义模板中匹配到的其他证明文件（来自 static/imgs/）
        static_imgs_dir = os.path.join(os.path.dirname(__file__), 'static', 'imgs')
        template_other_proof = request.form.get('other_proof_files_from_template', '').strip()
        if template_other_proof:
            try:
                template_other_proof_list = json.loads(template_other_proof)
                for rel_path in template_other_proof_list:
                    abs_path = os.path.join(static_imgs_dir, rel_path)
                    if os.path.exists(abs_path):
                        other_proof_paths.append(abs_path)
            except json.JSONDecodeError:
                pass

        complaint_category = payload['form']['complaint_category']
        complaint_type = payload['form']['complaint_type']
        copyright_type = complaint_type if complaint_category == '知识产权' else ''
        batch_files = [batch['path'] for batch in batches]
        task_payload = {
            'task_id': task_id,
            'excel_files': batch_files,
            'cookie': payload['form']['cookie'],
            'proof_file': proof_file_path,
            'other_proof_files': other_proof_paths,
            'description': payload['form']['description'],
            'identity': payload['form']['identity'],
            'agent': payload['form']['agent'],
            'rights_holder': rights_holder,
            'complaint_category': complaint_category,
            'copyright_type': copyright_type,
            'module': payload['form']['module'],
            'content_type': payload['form']['content_type'],
            'batch_metadata': payload['batches'],
        }

        enqueue_uc_task(task_payload)
        update_complaint_task(task_id, status='queued')
        tasks[task_id]['status'] = 'queued'

        return jsonify({
            'success': True,
            'task_id': task_id,
            'message': '任务已创建，正在排队执行投诉',
            'excel_rows': rows,
            'batch_count': len(batches),
        })
    except Exception as e:
        return jsonify({'success': False, 'error': f'提交失败：{str(e)}'}), 500


# 自定义模板上传后的临时文件目录
CUSTOM_TEMPLATE_FOLDER = os.path.join(os.path.dirname(__file__), 'uploads', 'custom_templates')


def cleanup_old_template_files(max_age_hours=24):
    """清理超过指定时间的临时模板目录"""
    import time
    if not os.path.exists(CUSTOM_TEMPLATE_FOLDER):
        return
    now = time.time()
    for item in os.listdir(CUSTOM_TEMPLATE_FOLDER):
        item_path = os.path.join(CUSTOM_TEMPLATE_FOLDER, item)
        if os.path.isdir(item_path):
            # 检查目录修改时间
            mtime = os.path.getmtime(item_path)
            age_hours = (now - mtime) / 3600
            if age_hours > max_age_hours:
                shutil.rmtree(item_path, ignore_errors=True)


def extract_zip_with_correct_encoding(zip_file_storage, extract_dir):
    """使用unzip命令解压ZIP文件以保留正确的中文文件名"""
    import tempfile
    import subprocess

    # 先保存上传的ZIP到临时文件
    with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as tmp_zip:
        zip_path = tmp_zip.name
        zip_file_storage.save(zip_path)

    # 使用unzip命令解压
    try:
        subprocess.run(['unzip', '-o', '-q', zip_path, '-d', extract_dir],
                      check=True, capture_output=True)
    finally:
        os.unlink(zip_path)  # 删除临时ZIP文件


@app.route('/api/download_custom_template', methods=['GET'])
def download_custom_template():
    """下载自定义模板Excel（3个Sheet）"""
    try:
        # Sheet1: 表单内容（除采集账号和Cookie外，删除委托代理文件）
        sheet1_data = {
            '字段': [
                '您的身份', '代理人/权利人', '被代理人（权利人）信息', '投诉大类',
                '投诉类型', '功能模块', '内容类型', '投诉内容描述', '作品名称'
            ],
            '值': [
                '代理人', '北京和晞科技有限公司', '', '',
                '', '', '', '', ''
            ],
            '可选值': [
                '权利人、代理人', '北京和晞科技有限公司',
                '', '知识产权、人身权',
                '著作权（含视频、图文、图集等）、商标、专利、其他知识产权',
                '头条内容、大鱼号账号、UC网盘、神马搜索',
                '影视剧集、其他视频、小说、漫画、图片、文章、软件/游戏、其他',
                '',
                '填写原创作品名称，用于匹配证明文件，例如： 乔家的儿女'
            ]
        }
        df_sheet1 = pd.DataFrame(sheet1_data)

        # Sheet2: 批量导入的Excel表格
        sheet2_headers = ['侵权链接', '对应原创链接/对应访问码', '作品名称']
        sheet2_data = [
            ['', '', ''],
            ['', '', ''],
            ['', '', ''],
        ]
        df_sheet2 = pd.DataFrame(sheet2_data, columns=sheet2_headers)

        # Sheet3: 填写要求说明
        sheet3_lines = [
            ['Sheet1 表单填写说明'],
            [''],
            ['字段名', '填写说明'],
            ['您的身份', '必填，选择「权利人」或「代理人」'],
            ['代理人/权利人', '必填，选择代理人或权利人名称'],
            ['被代理人（权利人）信息', '代理人身份时必填，选择被代理人，名称必须与投诉平台选项中的值保持一致'],
            ['投诉大类', '必填，选择「知识产权」或「人身权」'],
            ['投诉类型', '必填，根据投诉大类选择具体类型'],
            ['功能模块', '必填，选择功能模块'],
            ['内容类型', '必填，选择内容类型'],
            ['投诉内容描述', '必填，根据公司要求填写'],
            [''],
            ['Sheet2 批量导入Excel填写说明'],
            [''],
            ['字段名', '填写说明'],
            ['侵权链接', '必填，填写需要投诉的侵权内容链接'],
            ['对应原创链接/对应访问码', '必填，填写原创内容链接或访问码'],
            ['作品名称', '必填，填写原创作品名称，用于自动匹配证明文件'],
            [''],
            ['Sheet3 证明文件说明'],
            [''],
            ['上传Excel后，系统会根据以下规则自动匹配证明文件：'],
            [''],
            ['证明文件', '根据「作品名称」在 static/imgs/剧名/作品名称/ 目录下查找「证明文件_*」文件'],
            ['其他证明[1]', '根据「被代理人」在 static/imgs/授权委托书/ 目录下查找「委托授权书_被代理人」文件'],
            ['其他证明[2]', '根据「被代理人」在 static/imgs/营业执照/ 目录下查找「营业执照_被代理人」文件'],
            ['其他证明[3]', '根据「代理人」在 static/imgs/营业执照/ 目录下查找「营业执照_代理人」文件'],
            [''],
            ['注意事项'],
            [''],
            ['1. 上传自定义模板时，只需上传Excel文件（.xlsx或.xls）'],
            ['2. 作品名称必须与 static/imgs/剧名/ 下的文件夹名称完全一致'],
            ['3. 文件格式支持：jpg、png、jpeg、bmp、pdf'],
        ]
        df_sheet3 = pd.DataFrame(sheet3_lines)

        # 创建Excel文件
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_sheet1.to_excel(writer, sheet_name='表单内容', index=False)
            df_sheet2.to_excel(writer, sheet_name='批量导入Excel', index=False)
            df_sheet3.to_excel(writer, sheet_name='填写说明', index=False, header=False)

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_sheet1.to_excel(writer, sheet_name='表单内容', index=False)
            df_sheet2.to_excel(writer, sheet_name='批量导入Excel', index=False)
            df_sheet3.to_excel(writer, sheet_name='填写说明', index=False, header=False)

        output.seek(0)
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name='custom_template.xlsx'
        )
    except Exception as e:
        return jsonify({'success': False, 'error': f'生成模板失败：{str(e)}'}), 500


@app.route('/api/upload_custom_template', methods=['POST'])
def upload_custom_template():
    """上传自定义模板Excel，自动匹配证明文件"""
    import glob

    if 'file' not in request.files:
        return jsonify({'success': False, 'error': '未上传文件'}), 400

    excel_file = request.files['file']
    if not excel_file.filename:
        return jsonify({'success': False, 'error': '文件为空'}), 400

    filename = excel_file.filename.lower()
    if not (filename.endswith('.xlsx') or filename.endswith('.xls')):
        return jsonify({'success': False, 'error': '请上传Excel格式文件（.xlsx或.xls）'}), 400

    try:
        # 保存上传的Excel到临时位置
        ensure_dir(CUSTOM_TEMPLATE_FOLDER)
        template_id = datetime.now().strftime('%Y%m%d_%H%M%S_') + uuid4().hex[:8]
        template_dir = os.path.join(CUSTOM_TEMPLATE_FOLDER, template_id)
        ensure_dir(template_dir)

        excel_path = os.path.join(template_dir, 'template.xlsx')
        excel_file.save(excel_path)

        # 读取Excel
        try:
            xls = pd.ExcelFile(excel_path)
            sheet1_data = pd.read_excel(xls, sheet_name='表单内容')
            sheet2_data = pd.read_excel(xls, sheet_name='批量导入Excel')
        except Exception as e:
            shutil.rmtree(template_dir, ignore_errors=True)
            return jsonify({'success': False, 'error': f'Excel解析失败：{str(e)}'}), 400

        # 检测Sheet2是否有空行（通过openpyxl读取真实行数，与pandas读取结果对比）
        try:
            wb_check = load_workbook(excel_path)
            ws_sheet2 = wb_check['批量导入Excel']
            actual_data_rows = ws_sheet2.max_row - 1  # 减去标题行
            pandas_data_rows = len(sheet2_data)
            if actual_data_rows > pandas_data_rows:
                empty_count = actual_data_rows - pandas_data_rows
                shutil.rmtree(template_dir, ignore_errors=True)
                return jsonify({'success': False, 'error': f'Sheet2中存在 {empty_count} 行空行，请删除空行后再上传'}), 400
        except Exception:
            pass  # 忽略openpyxl读取错误，以pandas结果为准

        # 辅助函数：标准化括号（全角转半角）
        def normalize_paren(s):
            return s.replace('（', '(').replace('）', ')')

        # 辅助函数：检查公司名是否匹配（支持括号中英文模糊匹配）
        def company_match(principal, filename):
            # 标准化后精确匹配
            return normalize_paren(principal) in normalize_paren(filename)

        # 解析Sheet1表单数据
        form_data = {}
        try:
            for _, row in sheet1_data.iterrows():
                field = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ''
                value = str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else ''
                if field and field != 'nan':
                    form_data[field] = value
        except Exception as e:
            shutil.rmtree(template_dir, ignore_errors=True)
            return jsonify({'success': False, 'error': f'Sheet1解析失败：{str(e)}'}), 400

        # Sheet1 必填字段校验
        required_fields_sheet1 = {
            '您的身份': '您的身份',
            '代理人/权利人': '代理人/权利人',
            '被代理人（权利人）信息': '被代理人（权利人）信息',
            '投诉大类': '投诉大类',
            '投诉类型': '投诉类型',
            '功能模块': '功能模块',
            '内容类型': '内容类型',
            '投诉内容描述': '投诉内容描述',
            '作品名称': '作品名称',
        }
        missing_fields = [label for field, label in required_fields_sheet1.items() if not form_data.get(field, '').strip()]
        if missing_fields:
            shutil.rmtree(template_dir, ignore_errors=True)
            return jsonify({'success': False, 'error': '以下必填项未填写：' + '、'.join(missing_fields)}), 400

        # 解析Sheet2批量导入数据
        excel_rows = []
        try:
            for i, row in sheet2_data.iterrows():
                # 检测空行：侵权链接列为空或仅空白字符
                if not (pd.notna(row.iloc[0]) and str(row.iloc[0]).strip()):
                    continue  # 空行已被前面的 openpyxl 检测捕获，这里静默跳过
                excel_rows.append({
                    '侵权链接': str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else '',
                    '对应原创链接/对应访问码': str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else '',
                    '作品名称': str(row.iloc[2]).strip() if pd.notna(row.iloc[2]) else ''
                })
        except Exception as e:
            shutil.rmtree(template_dir, ignore_errors=True)
            return jsonify({'success': False, 'error': f'Sheet2解析失败：{str(e)}'}), 400

        if not excel_rows:
            shutil.rmtree(template_dir, ignore_errors=True)
            return jsonify({'success': False, 'error': 'Sheet2为空，请填写后重新上传'}), 400

        # 校验Sheet2 A列（侵权链接）格式：每行必须以 http 或 https 开头
        invalid_rows = []
        for i, row in enumerate(excel_rows, start=2):  # start=2 因为第1行是标题
            link = row.get('侵权链接', '')
            if link and not link.startswith('http://') and not link.startswith('https://'):
                invalid_rows.append(f'第{i}行')
        if invalid_rows:
            shutil.rmtree(template_dir, ignore_errors=True)
            return jsonify({'success': False, 'error': 'Sheet2 A列（侵权链接）格式错误：' + '、'.join(invalid_rows) + '，必须以 http:// 或 https:// 开头'}), 400

        # 获取基本信息
        principal = form_data.get('被代理人（权利人）信息', '')  # 如 "北京uc"
        agent = form_data.get('代理人/权利人', '')  # 如 "北京和晞科技有限公司"

        # 定义静态文件目录
        static_imgs_dir = os.path.join(os.path.dirname(__file__), 'static', 'imgs')

        # 1. 查找证明文件: static/imgs/剧名/[作品名称]/证明文件_*
        # 优先从Sheet1的"作品名称"字段获取，其次从Sheet2第一行的作品名称
        proof_file = None
        work_name = form_data.get('作品名称', '')
        if not work_name and excel_rows:
            work_name = excel_rows[0].get('作品名称', '')
        # 校验作品名称目录是否存在
        drama_dir = os.path.join(static_imgs_dir, '剧名', work_name)
        if not os.path.isdir(drama_dir):
            shutil.rmtree(template_dir, ignore_errors=True)
            return jsonify({'success': False, 'error': f'「{work_name}」作品没有匹配到，请检查剧名是否正确'}), 400
        other_proof_files = []
        # 查找以"证明文件_"开头的文件
        for f in os.listdir(drama_dir):
            if f.startswith('证明文件_') and not f.startswith('._'):
                proof_file = os.path.join('剧名', work_name, f)
                break

        # 查找以"其他证明_"开头的文件
        for f in os.listdir(drama_dir):
            if f.startswith('其他证明_') and not f.startswith('._'):
                other_proof_files.append(os.path.join('剧名', work_name, f))

        # 2.1 授权委托书: static/imgs/授权委托书/委托授权书_[被代理人].*
        proxy_file = None
        if principal:
            auth_dir = os.path.join(static_imgs_dir, '授权委托书')
            if os.path.isdir(auth_dir):
                # 精确匹配公司名（支持括号中英文模糊匹配）
                for f in os.listdir(auth_dir):
                    if (f.startswith('委托授权书_') or f.startswith('授权委托书_')) and not f.startswith('._'):
                        if company_match(principal, f):
                            proxy_file = os.path.join('授权委托书', f)
                            break

        # 2.2 营业执照(被代理人): static/imgs/营业执照/营业执照_[被代理人].*
        biz_license_principal = None
        if principal:
            biz_dir = os.path.join(static_imgs_dir, '营业执照')
            if os.path.isdir(biz_dir):
                for f in os.listdir(biz_dir):
                    if f.startswith('营业执照_') and not f.startswith('._'):
                        if company_match(principal, f):
                            biz_license_principal = os.path.join('营业执照', f)
                            break

        # 2.3 营业执照(代理人): static/imgs/营业执照/营业执照_[代理人].*
        biz_license_agent = None
        if agent:
            biz_dir = os.path.join(static_imgs_dir, '营业执照')
            if os.path.isdir(biz_dir):
                # 精确匹配代理人公司名（支持括号中英文模糊匹配）
                for f in os.listdir(biz_dir):
                    if f.startswith('营业执照_') and not f.startswith('._'):
                        if company_match(agent, f):
                            biz_license_agent = os.path.join('营业执照', f)
                            break

        # 组装其他证明文件列表
        if proxy_file:
            other_proof_files.append(proxy_file)
        if biz_license_principal:
            other_proof_files.append(biz_license_principal)
        if biz_license_agent:
            other_proof_files.append(biz_license_agent)

        # 校验必须匹配到的证明文件
        missing_proofs = []
        if not proof_file:
            missing_proofs.append('证明文件（侵权证明）')
        if not proxy_file:
            missing_proofs.append('授权委托书（委托授权书_被代理人）')
        if not biz_license_principal:
            missing_proofs.append('营业执照（被代理人）')
        if not biz_license_agent:
            missing_proofs.append('营业执照（代理人）')

        if missing_proofs:
            shutil.rmtree(template_dir, ignore_errors=True)
            return jsonify({'success': False, 'error': '以下证明文件未匹配到，请检查被代理人信息或文件是否齐全：' + '、'.join(missing_proofs)}), 400

        # 准备返回数据
        result = {
            'success': True,
            'template_id': template_id,
            'form_data': form_data,
            'excel_rows': excel_rows,
            'files': {
                'proof_file': proof_file,
                'other_proof_files': other_proof_files
            }
        }

        return jsonify(result)

    except Exception as e:
        if 'template_dir' in dir() and template_dir:
            shutil.rmtree(template_dir, ignore_errors=True)
        return jsonify({'success': False, 'error': f'处理失败：{str(e)}'}), 500


@app.route('/api/proof_file/<path:filename>', methods=['GET'])
def serve_proof_file(filename):
    """服务证明文件（从static/imgs目录）"""
    # 安全检查：防止路径遍历
    static_dir = os.path.join(os.path.dirname(__file__), 'static', 'imgs')
    file_path = os.path.normpath(os.path.join(static_dir, filename))

    # 确保文件仍在static_dir内
    if not file_path.startswith(os.path.abspath(static_dir) + os.sep):
        return jsonify({'success': False, 'error': '无效的文件路径'}), 400

    if not os.path.exists(file_path):
        return jsonify({'success': False, 'error': '文件不存在'}), 404

    return send_file(file_path)


@app.route('/api/custom_template_file/<template_id>/<path:filename>', methods=['GET'])
def serve_custom_template_file(template_id, filename):
    """服务自定义模板的临时文件"""
    # 安全检查：防止路径遍历
    template_dir = os.path.join(CUSTOM_TEMPLATE_FOLDER, template_id)
    file_path = os.path.normpath(os.path.join(template_dir, filename))

    # 确保文件仍在template_dir内
    if not file_path.startswith(os.path.abspath(template_dir) + os.sep):
        return jsonify({'success': False, 'error': '无效的文件路径'}), 400

    if not os.path.exists(file_path):
        return jsonify({'success': False, 'error': '文件不存在'}), 404

    return send_file(file_path)


def run_complaint_script(task_id, excel_files, cookie, proof_file, other_proof_files, description,
                         identity, agent, rights_holder, complaint_category, copyright_type, module, content_type,
                         batch_metadata):
    """在后台线程中执行UC投诉自动化脚本"""
    import sys

    script_path = os.path.join(os.path.dirname(__file__), 'uc_complaint_from_backend.py')
    submission_id = task_id[3:] if task_id.startswith('uc_') else task_id

    cmd = [
        sys.executable,
        script_path,
        '--task-id', task_id,
        '--excel-files', json.dumps(excel_files, ensure_ascii=False),
        '--proof-file', proof_file if proof_file else '',
        '--description', description,
        '--identity', identity,
        '--agent', agent,
        '--rights-holder', rights_holder,
        '--module', module,
        '--content-type', content_type,
        '--cookie', cookie,
        '--batch-metadata', json.dumps(batch_metadata, ensure_ascii=False),
    ]

    if other_proof_files:
        other_proof_str = ','.join([f for f in other_proof_files if f])
        cmd.extend(['--other-proof-files', other_proof_str])

    if complaint_category == '知识产权' and copyright_type:
        cmd.extend(['--complaint-type', complaint_category, '--copyright-type', copyright_type])

    print(f"[{task_id}] 执行命令: {' '.join(cmd)}")

    try:
        started_at = datetime.now().isoformat()
        if task_id in tasks:
            tasks[task_id]['status'] = 'running'
            tasks[task_id]['started_at'] = started_at
        update_complaint_task(task_id, status='running', started_at=datetime.fromisoformat(started_at))

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=max(600, len(excel_files) * 300)
        )

        print(f"[{task_id}] stdout: {result.stdout}")
        print(f"[{task_id}] stderr: {result.stderr}")

        task_result = None
        try:
            start_idx = result.stdout.find('JSON_RESULT_START')
            end_idx = result.stdout.find('JSON_RESULT_END')
            if start_idx != -1 and end_idx != -1:
                json_str = result.stdout[start_idx + 17:end_idx].strip()
                task_result = json.loads(json_str)
        except Exception:
            pass

        if task_result:
            if task_id in tasks:
                tasks[task_id].update(task_result)
            update_fields = {
                'status': task_result.get('status'),
                'current_batch': task_result.get('current_batch'),
                'completed_batches': task_result.get('completed_batches'),
                'failed_batches': task_result.get('failed_batches'),
                'complaint_numbers_json': task_result.get('complaint_numbers', []),
                'error_message': task_result.get('error'),
            }
            if task_result.get('started_at'):
                update_fields['started_at'] = datetime.fromisoformat(task_result['started_at'])
            if task_result.get('completed_at'):
                update_fields['completed_at'] = datetime.fromisoformat(task_result['completed_at'])
            update_complaint_task(task_id, **update_fields)
            for batch in task_result.get('batches', []):
                update_complaint_batch(
                    submission_id,
                    batch['batch_no'],
                    status=batch.get('status'),
                    complaint_number=batch.get('complaint_number'),
                    error_message=batch.get('error')
                )
        else:
            if task_id in tasks:
                tasks[task_id]['status'] = 'failed'
                tasks[task_id]['error'] = result.stderr or '执行失败'
            update_complaint_task(
                task_id,
                status='failed',
                error_message=result.stderr or '执行失败',
                completed_at=datetime.now()
            )
    except subprocess.TimeoutExpired:
        if task_id in tasks:
            tasks[task_id]['status'] = 'failed'
            tasks[task_id]['error'] = '执行超时'
        update_complaint_task(task_id, status='failed', error_message='执行超时', completed_at=datetime.now())
    except Exception as e:
        if task_id in tasks:
            tasks[task_id]['status'] = 'failed'
            tasks[task_id]['error'] = str(e)
        update_complaint_task(task_id, status='failed', error_message=str(e), completed_at=datetime.now())


@app.route('/api/uc/task/<task_id>', methods=['GET'])
def get_task_status(task_id):
    """查询任务状态"""
    task = get_complaint_task(task_id)
    if not task:
        task = tasks.get(task_id)
    if not task:
        task = load_task_result(task_id)

    if not task:
        return jsonify({'success': False, 'error': '任务不存在'}), 404

    return jsonify({
        'success': True,
        'task_id': task_id,
        'status': task.get('status'),
        'complaint_number': task.get('complaint_number'),
        'complaint_numbers': task.get('complaint_numbers', []),
        'batch_count': task.get('batch_count'),
        'completed_batches': task.get('completed_batches'),
        'current_batch': task.get('current_batch'),
        'batches': task.get('batches', []),
        'error': task.get('error'),
        'submitted_at': task.get('submitted_at'),
        'started_at': task.get('started_at'),
        'completed_at': task.get('completed_at'),
    })


@app.route('/api/uc/status_list', methods=['GET'])
def get_uc_status_list():
    """获取UC投诉状态列表"""
    submissions = get_submission_status_list()
    if submissions:
        return jsonify({'success': True, 'data': submissions})

    submissions = []
    uc_submissions_path = app.config['UC_SUBMISSION_FOLDER']

    if not os.path.exists(uc_submissions_path):
        return jsonify({'success': True, 'data': []})

    for item in sorted(os.listdir(uc_submissions_path), reverse=True):
        item_path = os.path.join(uc_submissions_path, item)
        if not os.path.isdir(item_path):
            continue

        submission_file = os.path.join(item_path, 'submission.json')
        if not os.path.exists(submission_file):
            continue

        try:
            with open(submission_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            task_id = f"uc_{data.get('submission_id', item)}"
            task_info = tasks.get(task_id)
            if not task_info:
                task_info = load_task_result(task_id)

            status = '未知'
            if task_info:
                status = map_task_status_label(task_info.get('status', '未知'))

            complaint_numbers = []
            if task_info and task_info.get('complaint_numbers'):
                complaint_numbers = task_info.get('complaint_numbers', [])
            elif task_info and task_info.get('complaint_number'):
                complaint_numbers = [task_info.get('complaint_number')]

            submissions.append({
                'submission_id': data.get('submission_id', item),
                'submitted_at': data.get('submitted_at', ''),
                'collect_account': data.get('form', {}).get('collect_account', ''),
                'work_name': data.get('form', {}).get('作品名称', ''),
                'excel_rows': data.get('excel_rows', 0),
                'batch_count': data.get('batch_count', 0),
                'status': status,
                'complaint_numbers': complaint_numbers,
            })
        except Exception:
            continue

    return jsonify({'success': True, 'data': submissions})


@app.route('/api/uc/verify_cookie', methods=['POST'])
def verify_cookie():
    """验证Cookie是否有效"""
    data = request.get_json()
    cookie = data.get('cookie', '').strip()

    if not cookie:
        return jsonify({'success': False, 'error': 'Cookie不能为空'}), 400

    from playwright.sync_api import sync_playwright

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--lang=zh-CN,en-US",
                ],
            )
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080},
            )

            # 设置Cookie
            if cookie.startswith('[') or cookie.startswith('{'):
                cookies = json.loads(cookie) if isinstance(cookie, str) else cookie
                context.add_cookies(cookies)
            else:
                for pair in cookie.split(';'):
                    pair = pair.strip()
                    if '=' in pair:
                        key, value = pair.split('=', 1)
                        context.add_cookies([{
                            "name": key,
                            "value": value,
                            "domain": ".uc.cn",
                            "path": "/"
                        }])

            # 访问UC投诉平台检查登录状态
            page = context.new_page()
            page.goto("https://ipp.uc.cn/#/home", wait_until="load", timeout=15000)
            page.wait_for_timeout(2000)

            # 检查是否出现登录对话框
            login_dialog = page.locator("text=UC账号登录").first
            if login_dialog.count() > 0 and login_dialog.is_visible():
                browser.close()
                return jsonify({'success': False, 'error': 'Cookie已过期，请重新登录'}), 401

            browser.close()
            return jsonify({'success': True, 'message': 'Cookie有效'})

    except Exception as e:
        return jsonify({'success': False, 'error': f'验证失败：{str(e)}'}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)
