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
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import pandas as pd
from flask import Flask, render_template, request, jsonify, send_file
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.config['SECRET_KEY'] = 'complaint-form-secret'
app.config['UPLOAD_FOLDER'] = os.path.join(os.path.dirname(__file__), 'uploads')
app.config['UC_SUBMISSION_FOLDER'] = os.path.join(app.config['UPLOAD_FOLDER'], 'uc_submissions')
app.config['TASK_RESULT_FOLDER'] = os.path.join(os.path.dirname(__file__), 'task_results')
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0

# 任务状态存储（生产环境建议用数据库）
tasks = {}
task_queue = queue.Queue()
worker_thread = None
worker_lock = threading.Lock()

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


def split_excel_into_batches(df, batch_dir, batch_size=2):
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


def queue_worker():
    while True:
        task_args = task_queue.get()
        try:
            run_complaint_script(*task_args)
        finally:
            task_queue.task_done()


def start_queue_worker():
    global worker_thread

    with worker_lock:
        if worker_thread and worker_thread.is_alive():
            return

        worker_thread = threading.Thread(target=queue_worker, daemon=True, name='uc-complaint-worker')
        worker_thread.start()


start_queue_worker()


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/works')
def works():
    return render_template('works.html')


# 投诉账号管理数据文件
ACCOUNTS_FILE = os.path.join(os.path.dirname(__file__), 'task_results', 'accounts.json')

# 平台映射
PLATFORM_MAP = {
    'uc': {'platform_name': 'UC', 'pingtai': 'UC'},
    'quark': {'platform_name': '夸克', 'pingtai': '夸克'},
}

def load_accounts():
    if not os.path.exists(ACCOUNTS_FILE):
        return []
    with open(ACCOUNTS_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

def save_accounts(accounts):
    with open(ACCOUNTS_FILE, 'w', encoding='utf-8') as f:
        json.dump(accounts, f, ensure_ascii=False, indent=2)


@app.route('/accounts')
def accounts():
    return render_template('accounts.html')


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
    accounts = load_accounts()
    if any(a.get('platform_code') == platform_code and a.get('user') == user for a in accounts):
        return jsonify({'success': False, 'error': f'该平台下投诉账号「{user}」已存在'}), 400
    new_id = uuid4().hex[:12]
    now = datetime.now().isoformat()
    accounts.append({
        'id': new_id,
        'platform_code': platform_code,
        'platform_name': PLATFORM_MAP[platform_code]['platform_name'],
        'pingtai': PLATFORM_MAP[platform_code]['pingtai'],
        'user': user,
        'cookie': cookie,
        'status': '0',
        'created_at': now,
        'updated_at': now,
    })
    save_accounts(accounts)
    return jsonify({'success': True, 'data': accounts[-1]})


@app.route('/api/accounts/update_cookie', methods=['POST'])
def accounts_update_cookie():
    data = request.get_json()
    acc_id = data.get('id')
    cookie = data.get('cookie', '').strip()
    if not cookie:
        return jsonify({'success': False, 'error': 'Cookie不能为空'}), 400
    accounts = load_accounts()
    updated = False
    for a in accounts:
        if a.get('id') == acc_id:
            a['cookie'] = cookie
            a['updated_at'] = datetime.now().isoformat()
            updated = True
            break
    if not updated:
        return jsonify({'success': False, 'error': '账号不存在'}), 404
    save_accounts(accounts)
    return jsonify({'success': True})


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
        batch_size = 2
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
        batch_size = 2
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
        tasks[task_id] = {
            'status': 'pending',
            'submission_id': submission_id,
            'submitted_at': datetime.now().isoformat(),
            'queued_at': datetime.now().isoformat(),
            'excel_rows': rows,
            'batch_count': len(batches),
            'completed_batches': 0,
            'current_batch': 0,
            'complaint_numbers': [],
            'batches': [
                {
                    'batch_no': batch['batch_no'],
                    'rows': batch['rows'],
                    'start_row': batch['start_row'],
                    'end_row': batch['end_row'],
                    'status': 'pending',
                    'error': None,
                }
                for batch in batches
            ],
        }

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

        task_queue.put((
            task_id,
            batch_files,
            payload['form']['cookie'],
            proof_file_path,
            other_proof_paths,
            payload['form']['description'],
            payload['form']['identity'],
            payload['form']['agent'],
            rights_holder,
            complaint_category,
            copyright_type,
            payload['form']['module'],
            payload['form']['content_type'],
            payload['batches'],
        ))

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
                '', '', '', '',
                '', '', '', '', ''
            ],
            '可选值': [
                '权利人、代理人', '北京和晞科技有限公司、刘明',
                '腾讯科技（北京）有限公司、深圳市腾讯计算机系统有限公司、上海腾讯企鹅影视文化传播有限公司、上海宽娱数码科技有限公司、北京卡路里科技有限公司', '知识产权、人身权',
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
            ['被代理人（权利人）信息', '代理人身份时必填，选择被代理人'],
            ['投诉大类', '必填，选择「知识产权」或「人身权」'],
            ['投诉类型', '必填，根据投诉大类选择具体类型'],
            ['功能模块', '必填，选择功能模块'],
            ['内容类型', '必填，选择内容类型'],
            ['投诉内容描述', '必填，客观公正描述侵权所在，最多1000字'],
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
            ['1. 上传自定义模板时，只需上传Excel文件（.xlsx或.xls），无需ZIP打包'],
            ['2. 作品名称必须与 static/imgs/剧名/ 下的文件夹名称完全一致'],
            ['3. 代理人默认为「北京和晞科技有限公司」'],
            ['4. 文件格式支持：jpg、png、jpeg、bmp、pdf'],
        ]
        df_sheet3 = pd.DataFrame(sheet3_lines)

        # 创建Excel文件
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
            attachment_filename='custom_template.xlsx'
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
            for _, row in sheet2_data.iterrows():
                if pd.notna(row.iloc[0]) and str(row.iloc[0]).strip():
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
                    if f.startswith('委托授权书_') and not f.startswith('._'):
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
            timeout=max(600, len(excel_files) * 300)
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
        tasks[task_id]['error'] = '执行超时'
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

            # 获取任务状态
            task_id = f"uc_{data.get('submission_id', item)}"
            task_info = tasks.get(task_id)
            if not task_info:
                task_info = load_task_result(task_id)

            status = '未知'
            if task_info:
                status = task_info.get('status', '未知')
                if status == 'running':
                    status = '执行中'
                elif status == 'completed':
                    status = '已完成'
                elif status == 'failed':
                    status = '失败'
                elif status == 'pending':
                    status = '等待中'
                elif status == 'partial_failed':
                    status = '部分失败'

            # 获取投诉单号
            complaint_numbers = []
            if task_info and task_info.get('complaint_numbers'):
                complaint_numbers = task_info.get('complaint_numbers', [])
            elif task_info and task_info.get('complaint_number'):
                complaint_numbers = [task_info.get('complaint_number')]

            submissions.append({
                'submission_id': data.get('submission_id', item),
                'submitted_at': data.get('submitted_at', ''),
                'collect_account': data.get('form', {}).get('collect_account', ''),
                'excel_rows': data.get('excel_rows', 0),
                'batch_count': data.get('batch_count', 0),
                'status': status,
                'complaint_numbers': complaint_numbers,
            })
        except Exception as e:
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
