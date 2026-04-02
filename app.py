# !/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
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
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0

@app.after_request
def no_cache(response):
    response.headers['Cache-Control'] = 'no-store'
    return response

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['UC_SUBMISSION_FOLDER'], exist_ok=True)


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def save_uploaded_file(file_storage, target_dir, prefix=None):
    if not file_storage or not file_storage.filename:
        return None

    filename = secure_filename(file_storage.filename)
    if not filename:
        return None

    if prefix:
        filename = f"{prefix}_{filename}"

    save_path = os.path.join(target_dir, filename)
    file_storage.save(save_path)
    return filename


def create_submission_dir():
    submission_id = f"uc_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}"
    submission_dir = os.path.join(app.config['UC_SUBMISSION_FOLDER'], submission_id)
    ensure_dir(submission_dir)
    return submission_id, submission_dir


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
        missing_fields.append('证明材料')
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

        metadata_path = os.path.join(submission_dir, 'submission.json')
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        return jsonify({
            'success': True,
            'submission_id': submission_id,
            'message': 'UC投诉数据已保存',
            'excel_rows': rows,
            'saved_files': {
                'excel_file': saved_files['excel_file'],
                'proxy_file': saved_files['proxy_file'],
                'proof_file': saved_files['proof_file'],
                'other_proof_count': len(saved_files['other_proof_files'])
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'error': f'提交失败：{str(e)}'}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)
