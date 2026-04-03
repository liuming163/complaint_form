# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import subprocess
import threading
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import pandas as pd
from flask import Flask, render_template, request, jsonify
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.config['SECRET_KEY'] = 'complaint-form-secret'
app.config['UPLOAD_FOLDER'] = os.path.join(os.path.dirname(__file__), 'uploads')
app.config['UC_SUBMISSION_FOLDER'] = os.path.join(app.config['UPLOAD_FOLDER'], 'uc_submissions')
app.config['TASK_RESULT_FOLDER'] = os.path.join(os.path.dirname(__file__), 'task_results')
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0

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
    submission_id = f"uc_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}"
    submission_dir = os.path.join(app.config['UC_SUBMISSION_FOLDER'], submission_id)
    ensure_dir(submission_dir)
    return submission_id, submission_dir


def load_task_result(task_id):
    result_path = os.path.join(app.config['TASK_RESULT_FOLDER'], f'{task_id}.json')
    if not os.path.exists(result_path):
        return None

    with open(result_path, 'r', encoding='utf-8') as f:
        return json.load(f)


@app.route('/')
def index():
    return render_template('index.html')


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
        if rows > 200:
            os.remove(save_path)
            return jsonify({'success': False, 'error': f'文件共 {rows} 行，超过200行限制，请拆分后上传'})
        return jsonify({'success': True, 'rows': rows, 'filename': filename})
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
    if identity == '代理人' and (not request.files.get('proxy_file') or not request.files['proxy_file'].filename):
        missing_fields.append('委托代理文件')

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
        if rows > 200:
            return jsonify({'success': False, 'error': f'文件共 {rows} 行，超过200行限制，请拆分后上传'}), 400

        saved_files = {
            'excel_file': excel_filename,
            'proxy_file': save_uploaded_file(request.files.get('proxy_file'), submission_dir, 'proxy'),
            'proof_file': save_uploaded_file(request.files.get('proof_file'), submission_dir, 'proof'),
            'other_proof_files': []
        }

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
            },
            'excel_rows': rows,
            'files': saved_files,
        }

        rights_holder = payload['form']['principal'] if identity == '代理人' else payload['form']['agent']

        metadata_path = os.path.join(submission_dir, 'submission.json')
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        # 创建任务并异步执行自动化脚本
        task_id = f"uc_{submission_id}"
        tasks[task_id] = {
            'status': 'pending',
            'submission_id': submission_id,
            'submitted_at': datetime.now().isoformat(),
        }

        # 启动后台线程执行自动化脚本
        script_path = os.path.join(os.path.dirname(__file__), 'uc_complaint_from_backend.py')
        proof_file_path = os.path.join(submission_dir, saved_files['proof_file'])
        other_proof_paths = [os.path.join(submission_dir, f) for f in saved_files['other_proof_files']]

        # 处理投诉类型
        complaint_category = payload['form']['complaint_category']
        complaint_type = payload['form']['complaint_type']
        copyright_type = complaint_type if complaint_category == '知识产权' else ''

        thread = threading.Thread(
            target=run_complaint_script,
            args=(
                task_id,
                payload['form']['cookie'],
                excel_path,
                proof_file_path,
                other_proof_paths,
                payload['form']['description'],
                payload['form']['identity'],
                rights_holder,
                complaint_category,
                copyright_type,
                payload['form']['module'],
                payload['form']['content_type'],
            )
        )
        thread.start()

        return jsonify({
            'success': True,
            'task_id': task_id,
            'message': '任务已创建，正在执行投诉',
            'excel_rows': rows,
        })
    except Exception as e:
        return jsonify({'success': False, 'error': f'提交失败：{str(e)}'}), 500


def run_complaint_script(task_id, cookie, excel_file, proof_file, other_proof_files, description,
                         identity, rights_holder, complaint_category, copyright_type, module, content_type):
    """在后台线程中执行UC投诉自动化脚本"""
    import sys

    script_path = os.path.join(os.path.dirname(__file__), 'uc_complaint_from_backend.py')

    cmd = [
        sys.executable,
        script_path,
        '--task-id', task_id,
        '--cookie', cookie,
        '--excel-file', excel_file,
        '--proof-file', proof_file if proof_file else '',
        '--description', description,
        '--identity', identity,
        '--rights-holder', rights_holder,
        '--module', module,
        '--content-type', content_type,
    ]

    if other_proof_files:
        other_proof_str = ','.join([f for f in other_proof_files if f])
        cmd.extend(['--other-proof-files', other_proof_str])

    # 处理投诉类型
    if complaint_category == '知识产权' and copyright_type:
        cmd.extend(['--complaint-type', complaint_category, '--copyright-type', copyright_type])

    print(f"[{task_id}] 执行命令: {' '.join(cmd)}")

    try:
        tasks[task_id]['status'] = 'running'
        tasks[task_id]['started_at'] = datetime.now().isoformat()

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600  # 10分钟超时
        )

        print(f"[{task_id}] stdout: {result.stdout}")
        print(f"[{task_id}] stderr: {result.stderr}")

        # 解析JSON结果
        task_result = None
        try:
            # 从输出中提取JSON结果
            start_idx = result.stdout.find('JSON_RESULT_START')
            end_idx = result.stdout.find('JSON_RESULT_END')
            if start_idx != -1 and end_idx != -1:
                json_str = result.stdout[start_idx + 17:end_idx].strip()
                task_result = json.loads(json_str)
        except:
            pass

        if task_result:
            tasks[task_id].update(task_result)
        else:
            tasks[task_id]['status'] = 'failed'
            tasks[task_id]['error'] = result.stderr or '执行失败'

    except subprocess.TimeoutExpired:
        tasks[task_id]['status'] = 'failed'
        tasks[task_id]['error'] = '执行超时（10分钟）'
    except Exception as e:
        tasks[task_id]['status'] = 'failed'
        tasks[task_id]['error'] = str(e)


@app.route('/api/uc/task/<task_id>', methods=['GET'])
def get_task_status(task_id):
    """查询任务状态"""
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
        'error': task.get('error'),
        'submitted_at': task.get('submitted_at'),
        'started_at': task.get('started_at'),
        'completed_at': task.get('completed_at'),
    })


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)
