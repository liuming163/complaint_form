#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""夸克版权投诉自动化脚本"""

import argparse
import json
import sys
import time
import os
import requests
from datetime import datetime

BASE_URL = 'https://ipp.quark.cn'
MAX_LINKS_PER_BATCH = 200


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def _extract_xtstk(cookie: str) -> str:
    """从 cookie 字符串中提取 cmptstk 值作为 xtstk header"""
    for part in cookie.split(';'):
        part = part.strip()
        if part.startswith('cmptstk='):
            return part[len('cmptstk='):]
    return ''


def make_headers(cookie: str) -> dict:
    return {
        'Cookie': cookie,
        'x-requested-with': 'XMLHttpRequest',
        'xtstk': _extract_xtstk(cookie),
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Referer': 'https://ipp.quark.cn/',
        'Origin': 'https://ipp.quark.cn',
    }


def verify_cookie(cookie: str) -> None:
    resp = requests.get(f'{BASE_URL}/api/complain/accuse', headers=make_headers(cookie), timeout=10)
    data = resp.json()
    if data.get('code') != 200:
        raise RuntimeError(f"Cookie无效或已过期: code={data.get('code')}")
    log('Cookie验证通过')


def get_uid(cookie: str) -> str:
    resp = requests.get(f'{BASE_URL}/api/front-end/config', headers=make_headers(cookie), timeout=10)
    data = resp.json()
    uid = data.get('base', {}).get('user', {}).get('uid')
    if not uid:
        raise RuntimeError('无法获取uid，Cookie可能已失效')
    return str(uid)


def get_identities(cookie: str, uid: str) -> dict:
    """返回 {'obligee_id': int, 'proxy_map': {name: {'id': int, 'delegation_file': str}}}"""
    resp = requests.get(
        f'{BASE_URL}/api/identity',
        headers=make_headers(cookie),
        params={'uid': uid, 'platform': 'quark'},
        timeout=10,
    )
    data = resp.json()
    if data.get('code') != 200:
        raise RuntimeError(f"获取身份列表失败: code={data.get('code')}")
    rows = data.get('data', {}).get('rows', [])
    obligee_id = None
    proxy_map = {}
    for row in rows:
        if row.get('is_proxy') == 0:
            obligee_id = row['id']
        else:
            proxy_map[row['obligee_name']] = {
                'id': row['id'],
                'delegation_file': row.get('proxy_delegation_file', ''),
            }
    if obligee_id is None:
        raise RuntimeError('未找到账号自身的权利人身份记录')
    return {'obligee_id': obligee_id, 'proxy_map': proxy_map}


def upload_image(cookie: str, file_path: str) -> str:
    """上传图片，返回永久URL。若文件不存在则返回空字符串。"""
    if not file_path or not os.path.exists(file_path):
        return ''
    headers = make_headers(cookie)
    # 上传时不带 Content-Type，让 requests 自动设置 multipart boundary
    headers.pop('Content-Type', None)
    with open(file_path, 'rb') as f:
        resp = requests.post(
            f'{BASE_URL}/api/files/uploadImg',
            headers=headers,
            files={'avatar': (os.path.basename(file_path), f)},
            timeout=30,
        )
    data = resp.json()
    # 上传接口返回 {'data': {'filePath': '...', 'o_url': '...'}}，无 code 字段
    resp_data = data.get('data') or {}
    permanent = resp_data.get('o_url', '') or resp_data.get('filePath', '')
    if not permanent:
        raise RuntimeError(f"图片上传失败: raw={str(data)[:200]}")
    log(f"图片上传成功: {os.path.basename(file_path)} → {permanent[:60]}...")
    return permanent


def submit_batch(cookie: str, links: list, originals: list, work_name: str,
                 obligee_id: int, proxy_id: int, proxy_delegation_file: str,
                 module: int, content_type: int,
                 complaint_type: int, complaint_sub_type: int,
                 description: str,
                 copyright_url: str, other_urls: list) -> str:
    """提交一批投诉，返回投诉单号"""
    import time as _time
    copy_article = [
        {
            'plagiarize': {'type': '', 'url': link, 'id': '', 'wm_id': ''},
            'original': {'type': '', 'url': originals[i] if i < len(originals) else '', 'id': '', 'wm_id': ''},
            'work': {'url': work_name, 'placeholder': '请输入作品名称（必填）', 'max': 100},
        }
        for i, link in enumerate(links)
    ]
    other_evidences = [
        {'key': int(_time.time() * 1000) + i, 'required': False, 'value': url, 'error': False}
        for i, url in enumerate(other_urls)
    ]
    payload = {
        'data': {
            'is_proxy': 1,
            'obligee_id': obligee_id,
            'proxy_id': proxy_id,
            'proxy_delegation_file': proxy_delegation_file,
            'platform': 'quark',
            'module': module,
            'content_type': content_type,
            'type': complaint_type,
            'sub_type': complaint_sub_type,
            'copy_article': copy_article,
            'description': description,
            'evidences': {
                'copyright': {'required': True, 'error': False, 'value': copyright_url},
                'other': other_evidences,
            },
        }
    }
    headers = {**make_headers(cookie), 'Content-Type': 'application/json;charset=UTF-8'}
    resp = requests.post(f'{BASE_URL}/api/complain/accuse', headers=headers, json=payload, timeout=30)
    data = resp.json()
    if data.get('code') != 200:
        raise RuntimeError(f"提交失败: {data.get('message', '')} code={data.get('code')}")
    complaint_no = str(data.get('data', {}).get('complaint_no', '') or data.get('data', '') or '')
    return complaint_no


def fetch_complaint_number(cookie: str, work_name: str, after_ts: float,
                           submitted_links: list = None, retries: int = 3) -> str:
    """提交后查询列表，按作品名+前2条侵权链接匹配，返回 complain_id"""
    if not submitted_links:
        return ''
    check_links = set(submitted_links[:2])
    headers = {**make_headers(cookie), 'Content-Type': 'application/json;charset=UTF-8'}
    for _ in range(retries):
        time.sleep(2)
        try:
            resp = requests.get(
                f'{BASE_URL}/api/complain/accuse',
                headers=headers,
                params={'pageNo': 1, 'pageSize': 20, 'platform': 'quark'},
                timeout=15,
            )
            data = resp.json()
            items = data.get('data', []) if isinstance(data.get('data'), list) else []
            for item in items:
                contents = item.get('evidence_contents', [])
                if not contents:
                    continue
                if contents[0].get('work', {}).get('url', '') != work_name:
                    continue
                item_links = {c.get('plagiarize', {}).get('url', '') for c in contents}
                if not check_links.issubset(item_links):
                    continue
                return str(item.get('complain_id', ''))
        except Exception:
            pass
    return ''


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--task-id', required=True)
    parser.add_argument('--cookie', required=True)
    parser.add_argument('--works-config-file', required=True)
    parser.add_argument('--module', type=int, default=3)
    parser.add_argument('--content-type', type=int, default=6)
    args = parser.parse_args()

    with open(args.works_config_file, encoding='utf-8') as f:
        works_config = json.load(f)

    result = {
        'task_id': args.task_id,
        'status': 'completed',
        'completed_batches': 0,
        'failed_batches': 0,
        'feedback_numbers': [],
        'batch_results': [],
        'error_message': '',
    }

    try:
        verify_cookie(args.cookie)
        uid = get_uid(args.cookie)
        identities = get_identities(args.cookie, uid)
    except Exception as e:
        result['status'] = 'failed'
        result['error_message'] = str(e)
        print('JSON_RESULT_START')
        print(json.dumps(result, ensure_ascii=False))
        print('JSON_RESULT_END')
        return 1

    obligee_id = identities['obligee_id']
    proxy_map = identities['proxy_map']

    batch_no = 0
    for work in works_config:
        work_name = work['work_name']
        proxy_name = work.get('proxy_name', '')
        links = work.get('links', [])
        image_paths = work.get('image_paths', [])

        # 匹配被代理人
        proxy_info = proxy_map.get(proxy_name)
        if proxy_info is None:
            available = '、'.join(proxy_map.keys()) or '无'
            for chunk_start in range(0, max(len(links), 1), MAX_LINKS_PER_BATCH):
                batch_no += 1
                result['failed_batches'] += 1
                result['batch_results'].append({
                    'batch_no': batch_no,
                    'work_name': work_name,
                    'status': 'failed',
                    'error': f"未找到被代理人「{proxy_name}」，可用：{available}",
                })
            log(f"[{work_name}] 跳过：未找到被代理人「{proxy_name}」")
            continue

        proxy_id = proxy_info['id']
        proxy_delegation_file = proxy_info['delegation_file']
        description = work.get('description', '')
        proof_path = work.get('proof_path', '')
        other_paths = work.get('other_paths', [])
        originals = work.get('originals', [])
        complaint_type = work.get('complaint_type', 9)
        complaint_sub_type = work.get('complaint_sub_type', 11)

        # 上传证明文件
        copyright_url = ''
        if proof_path:
            try:
                copyright_url = upload_image(args.cookie, proof_path)
            except Exception as e:
                log(f"版权证明上传失败: {e}")

        other_urls = []
        for p in other_paths:
            try:
                url = upload_image(args.cookie, p)
                if url:
                    other_urls.append(url)
            except Exception as e:
                log(f"其他证明上传失败（跳过）: {p} — {e}")

        if not copyright_url:
            for chunk_start in range(0, max(len(links), 1), MAX_LINKS_PER_BATCH):
                batch_no += 1
                result['failed_batches'] += 1
                result['batch_results'].append({
                    'batch_no': batch_no,
                    'work_name': work_name,
                    'status': 'failed',
                    'error': '版权证明文件上传失败，无法提交',
                })
            log(f"[{work_name}] 跳过：版权证明上传失败")
            continue

        for chunk_start in range(0, len(links), MAX_LINKS_PER_BATCH):
            batch_no += 1
            chunk = links[chunk_start:chunk_start + MAX_LINKS_PER_BATCH]
            log(f"[{work_name}/{proxy_name}] 第{batch_no}批 ({len(chunk)}条链接)")
            try:
                submit_ts = time.time()
                submit_batch(
                    args.cookie, chunk, originals[chunk_start:chunk_start + MAX_LINKS_PER_BATCH], work_name,
                    obligee_id, proxy_id, proxy_delegation_file,
                    args.module, args.content_type,
                    complaint_type, complaint_sub_type,
                    description, copyright_url, other_urls,
                )
                complaint_no = fetch_complaint_number(args.cookie, work_name, submit_ts, chunk)
                result['completed_batches'] += 1
                result['feedback_numbers'].append(complaint_no)
                result['batch_results'].append({
                    'batch_no': batch_no,
                    'work_name': work_name,
                    'status': 'completed',
                    'feedback_number': complaint_no,
                })
                log(f"[{work_name}] 第{batch_no}批成功，单号: {complaint_no}")
            except Exception as e:
                result['failed_batches'] += 1
                result['batch_results'].append({
                    'batch_no': batch_no,
                    'work_name': work_name,
                    'status': 'failed',
                    'error': str(e),
                })
                log(f"[{work_name}] 第{batch_no}批失败: {e}")
            time.sleep(2)

    if result['failed_batches'] > 0:
        result['status'] = 'partial_failed' if result['completed_batches'] > 0 else 'failed'

    print('JSON_RESULT_START')
    print(json.dumps(result, ensure_ascii=False))
    print('JSON_RESULT_END')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
