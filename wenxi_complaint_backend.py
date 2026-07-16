#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""腾讯文犀(ri.qq.com)版权投诉自动化脚本 - 纯API版本

与其他平台最大差异：**无 cookie，用 token 鉴权**。3 个请求头：
  Authorization: <token>（裸 JWT，无 Bearer 前缀）、sessionId、uid
token 约 2.5h 过期，失效时 API 返回 {code:20002}。

一单=1部作品；单次侵权链接上限 20 条(rightUrls 数组)，超过自动拆多单。
提交无验证码。附件(权属证明)走腾讯云 COS 直传：
  cosKey=<32位随机>.<ext> → /cos-proxy/pre-token(带签名) → <serviceUrl>/api/v1/push-file/stream
  payload.right.attach[].cosId = 上传返回 key 去扩展名。

委托方 group 对象在提交时用账号 token 实时拉 /delegate/{code}/details 构建，
保证授权材料(cosId/有效期)最新。账号自身主体 subjectGroup 为账号固定信息，
由 config 传入（每账号固化一份），仅注入所选委托方的 delegateId/delegateCode。

成功后从 /complaint/list 按 作品名+链接 匹配 complaintId 作为单号。
"""

import argparse
import hashlib
import json
import os
import random
import string
import sys
import time
from datetime import datetime

import requests

BASE_URL = 'https://ri.qq.com/api/v1'
MAX_LINKS_PER_SUBMISSION = 20      # 单次提交 rightUrls 上限，超出自动拆多单
COS_SIGN_SALT = 'cl_law_complaint'  # pre-token 签名固定盐（前端硬编码）

_UA = ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 '
       '(KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36')


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def make_headers(auth: dict, extra: dict = None) -> dict:
    """文犀鉴权三件套 + 通用头。auth={token,sessionId,uid}。"""
    headers = {
        'Authorization': auth['token'],
        'sessionId': auth.get('sessionId', ''),
        'uid': auth.get('uid', ''),
        'User-Agent': _UA,
        'Origin': 'https://ri.qq.com',
        'Referer': 'https://ri.qq.com/initiate-complaint',
        'Accept': 'application/json, text/plain, */*',
    }
    if extra:
        headers.update(extra)
    return headers


def check_login(auth: dict) -> None:
    """校验 token 是否有效。/message/unread-total 返回 code:0 即有效，20002 为过期。"""
    resp = requests.get(f'{BASE_URL}/message/unread-total', headers=make_headers(auth), timeout=15)
    try:
        data = resp.json()
    except Exception:
        raise RuntimeError(f'登录校验失败: HTTP {resp.status_code}')
    if data.get('code') == 20002:
        raise RuntimeError('token 已失效，请更新文犀 token')
    if data.get('code') != 0:
        raise RuntimeError(f"登录校验失败: {data.get('message', data)}")
    log('token 校验通过')


def _rand_str(n: int = 32) -> str:
    """生成 n 位随机字符串（COS key 前缀，镜像前端 j()(32)）。"""
    alphabet = string.ascii_lowercase + string.digits
    return ''.join(random.choice(alphabet) for _ in range(n))


def _cos_nonce() -> str:
    """镜像前端：两段 Math.random().toString(36).substring(2,15) 拼接。"""
    def _seg():
        return ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(13))
    return _seg() + _seg()


def upload_cos(auth: dict, file_path: str) -> dict:
    """上传单个附件到 COS，返回 attach 项 {id,cosId,fileName,fileLength,fileAddress,uid,status}。

    1) cosKey = <32位随机>.<ext>
    2) POST /cos-proxy/pre-token {cosKey}，带 x-cl-timestamp/nonce/signature
       signature = md5(t + cosKey + t + nonce + t + salt + t)
       → 返回 {serviceUrl, token}
    3) POST <serviceUrl>/api/v1/push-file/stream，头 {token,cosKey,forbidOverwrite:1}，body=文件
       → 返回 {key}；cosId = key 去扩展名
    """
    if not file_path or not os.path.exists(file_path):
        raise RuntimeError(f'附件不存在: {file_path}')

    file_name = os.path.basename(file_path)
    ext = os.path.splitext(file_name)[1].lstrip('.').lower()
    cos_key = f'{_rand_str(32)}.{ext}' if ext else _rand_str(32)
    file_size = os.path.getsize(file_path)

    # 2) pre-token（带签名）
    # 签名算法：SHA256（非 MD5），对应前端 CryptoJS module 94f8
    t = str(int(time.time()))
    nonce = _cos_nonce()
    raw = f'{t}{cos_key}{t}{nonce}{t}{COS_SIGN_SALT}{t}'
    signature = hashlib.sha256(raw.encode('utf-8')).hexdigest()
    pre_headers = make_headers(auth, {
        'x-cl-timestamp': t,
        'x-cl-nonce': nonce,
        'x-cl-signature': signature,
        'Content-Type': 'application/json',
    })
    resp = requests.post(f'{BASE_URL}/cos-proxy/pre-token', headers=pre_headers,
                         json={'cosKey': cos_key}, timeout=30)
    data = resp.json()
    if data.get('code') != 0:
        raise RuntimeError(f"COS pre-token 失败({file_name}): {data.get('message', data)}")
    d = data.get('data') or {}
    service_url = d.get('serviceUrl', '')
    push_token  = d.get('token', '')
    # 服务端会在 cosKey 前加 '/'（如 /abc.png），上传时必须用服务端返回的 cosKey，
    # 否则 token 与 cosKey 不匹配报错。
    server_cos_key = d.get('cosKey') or cos_key
    if not service_url or not push_token:
        raise RuntimeError(f'COS pre-token 未返回 serviceUrl/token: {data}')

    # 3) push-file/stream（真正上传二进制）
    with open(file_path, 'rb') as f:
        file_bytes = f.read()
    push_headers = {
        'Authorization': auth['token'],
        'sessionId': auth.get('sessionId', ''),
        'uid': auth.get('uid', ''),
        'User-Agent': _UA,
        'Origin': 'https://ri.qq.com',
        'Referer': 'https://ri.qq.com/initiate-complaint',
        'token': push_token,
        'cosKey': server_cos_key,   # ← 用服务端返回的 cosKey（含前缀 /）
        'forbidOverwrite': '1',
        'Content-Type': 'application/octet-stream',
    }
    resp2 = requests.post(f'{service_url}/api/v1/push-file/stream', headers=push_headers,
                          data=file_bytes, timeout=120)
    data2 = resp2.json()
    if data2.get('code') != 0:
        raise RuntimeError(f"COS 上传失败({file_name}): {data2.get('message', data2)}")
    # push-file 返回的 key 去掉前缀 / 和扩展名得到 cosId
    key = (data2.get('data') or {}).get('key') or server_cos_key
    cos_id = os.path.splitext(key.lstrip('/'))[0]

    log(f'  附件上传成功: {file_name} → cosId={cos_id}')
    return {
        'id': 0,
        'cosId': cos_id,
        'fileName': file_name,
        'fileLength': file_size,
        'fileAddress': '',
        'uid': int(time.time() * 1000),
        'status': 'success',
    }


def fetch_delegate_details(auth: dict, delegate_code: str) -> dict:
    """按 code 拉委托方完整详情，构建 payload.group 对象。"""
    resp = requests.get(f'{BASE_URL}/delegate/{delegate_code}/details',
                        headers=make_headers(auth), timeout=20)
    data = resp.json()
    if data.get('code') != 0 or not data.get('data'):
        raise RuntimeError(f"获取委托方详情失败({delegate_code}): {data.get('message', data)}")
    d = data['data']
    paper = d.get('delegatePaper') or {}
    group = {
        'name': d.get('name', ''),
        'identityNum': d.get('identityNumber', ''),
        'contactName': d.get('contactName', ''),
        'contactNo': d.get('contactNo', ''),
        'email': d.get('email', ''),
        'certificate': None,
        'authPaper': {
            'id': paper.get('id'),
            'cosId': paper.get('cosId', ''),
            'fileName': paper.get('fileName', ''),
            'fileAddress': paper.get('fileAddress', ''),
            'fileLength': paper.get('fileLength'),
        },
        'subjectType': d.get('subjectType'),
        'authEnd': d.get('authEnd', ''),
        'delegateType': d.get('delegateType', 3300),
        'id': d.get('id'),
        'orgType': d.get('identityType', 6600),
    }
    return group, d.get('id'), d.get('code', delegate_code)


def format_check(auth: dict, app_key: str, origin_url: str, right_urls: list) -> dict:
    """提交前 URL 校验。返回响应 dict（code:0 通过）。"""
    resp = requests.post(f'{BASE_URL}/url/format-check',
                         headers=make_headers(auth, {'Content-Type': 'application/json'}),
                         json={'appKey': app_key, 'originUrl': origin_url, 'rightUrls': right_urls},
                         timeout=30)
    try:
        return resp.json()
    except Exception:
        return {'code': -1, 'message': f'HTTP {resp.status_code}'}


def submit_complaint(auth: dict, payload: dict) -> dict:
    """提交投诉。返回响应 dict（含 code/message/_http_status/_raw_text）。"""
    resp = requests.post(f'{BASE_URL}/complaint/request',
                         headers=make_headers(auth, {'Content-Type': 'application/json'}),
                         json=payload, timeout=60)
    try:
        data = resp.json()
    except Exception:
        data = {}
    data['_http_status'] = resp.status_code
    data['_raw_text'] = resp.text[:500]
    return data


def build_payload(meta: dict, group: dict, delegate_id, delegate_code,
                  subject_group: dict, work_name: str, origin_url: str,
                  right_urls: list, attach: list) -> dict:
    """组装 /complaint/request 提交体。

    meta: {appId,appName,appKey,contentType,rightType,workType,description}
    group: fetch_delegate_details 构建的委托方对象
    subject_group: 账号固定主体（config 传入），注入本次 delegateId/delegateCode
    """
    # subjectGroup 是账号固定主体（config 传入，从真实提交固化），
    # 仅注入本次所选委托方的 delegateId/delegateCode，其余字段（含 subjectType）保持不变。
    sg = dict(subject_group)
    sg['delegateId'] = delegate_id
    sg['delegateCode'] = delegate_code
    if 'isAgency' not in sg:
        sg['isAgency'] = 1
    if 'agencyType' not in sg:
        sg['agencyType'] = meta.get('agencyType', 801)

    return {
        'rightCode': None,
        'domain': [''],
        'appId': meta['appId'],
        'appName': meta['appName'],
        'appKey': meta['appKey'],
        'contentType': meta['contentType'],
        'workType': meta.get('workType'),
        'rightType': meta['rightType'],
        'description': meta.get('description', ''),
        'right': {
            'name': work_name,
            'fromCinemas': 0,
            'originUrl': origin_url,
            'rightUrls': right_urls,
            'attach': attach,
            'tmType': None,
            'tmName': None,
            'tmTime': [],
            'tmStartDt': '',
            'tmEndDt': '',
            'urlBatchId': None,
            'successCount': 0,
            'isHot': None,
            'isLong': None,
        },
        'isAgency': 1,
        'agencyType': meta.get('agencyType', 801),
        'person': None,
        'group': group,
        'subjectPerson': None,
        'subjectGroup': sg,
        'accType': meta.get('agencyType', 801),
        'delegateId': delegate_id,
    }


def match_complaint_id(auth: dict, work_name: str, submitted_urls: list,
                       already_matched: set, retries: int = 3) -> str:
    """提交成功后从 /complaint/list 按作品名匹配 ticketNo 作为单号。

    接口参数：limit/offset（非 pageNum/pageSize），无 keyword 过滤，列表按时间倒序。
    单号字段：ticketNo（如 T71526001300471552）。
    作品名字段：originName。
    匹配策略：originName == work_name，取最新未认领的一条。
    URL 字段在提交后短期内为空，不做 URL 匹配。
    """
    for attempt in range(retries):
        time.sleep(2)
        try:
            resp = requests.get(f'{BASE_URL}/complaint/list',
                                headers=make_headers(auth),
                                params={'limit': 10, 'offset': 0,
                                        'delegateSubjectType': '', 'originName': ''},
                                timeout=20)
            data = resp.json()
        except Exception as e:
            log(f'  查询 complaint/list 异常(第{attempt+1}次): {e}')
            continue

        results = ((data.get('data') or {}).get('results')) or []
        for rec in results:
            ticket_no = rec.get('ticketNo', '')
            if not ticket_no or ticket_no in already_matched:
                continue
            # 按作品名匹配（originName）
            origin_name = rec.get('originName', '')
            if origin_name != work_name:
                continue
            already_matched.add(ticket_no)
            log(f'  匹配到单号(ticketNo): {ticket_no} (作品: {work_name})')
            return ticket_no
    return ''


def save_partial_result(task_id, result):
    """增量落盘进度（与其它平台一致），供 app.py 超时回收。"""
    try:
        result_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'task_results')
        os.makedirs(result_dir, exist_ok=True)
        with open(os.path.join(result_dir, f'{task_id}.json'), 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log(f'  ⚠️ 增量保存进度失败: {e}')


def _rebuild_numbers(result, works_config, failed_works, matched_by_work):
    """按 works_config 顺序重建 feedback_numbers(扁平) 和 feedback_numbers_by_work(分组)。"""
    ordered, by_work = [], []
    for w in works_config:
        wn = w['work_name']
        if matched_by_work.get(wn):
            nums = [str(n) for n in matched_by_work[wn]]
            ordered.extend(nums)
            st = 'completed'
        elif wn in failed_works:
            nums = [f'投诉失败:{wn}']
            ordered.append(nums[0])
            st = 'failed'
        else:
            nums = [f'未获取到单号:{wn}']
            ordered.append(nums[0])
            st = 'partial_failed'
        by_work.append({'work_name': wn, 'numbers': nums, 'status': st})
    result['feedback_numbers'] = ordered
    result['feedback_numbers_by_work'] = by_work


def main():
    parser = argparse.ArgumentParser(description='腾讯文犀版权投诉自动化脚本')
    parser.add_argument('--task-id', required=True)
    parser.add_argument('--config-file', required=True,
                        help='JSON: {"auth":{token,sessionId,uid}, "meta":{...}, '
                             '"subject_group":{...}, "delegate_code":"...", "works":[...]}')
    args = parser.parse_args()

    with open(args.config_file, encoding='utf-8') as f:
        config = json.load(f)

    auth = config['auth']
    meta = config['meta']
    subject_group = config.get('subject_group') or {}
    delegate_code = config.get('delegate_code', '')
    works_config = config.get('works', [])

    result = {
        'task_id': args.task_id,
        'status': 'running',
        'started_at': datetime.now().isoformat(),
        'completed_batches': 0,
        'failed_batches': 0,
        'feedback_numbers': [],
        'feedback_numbers_by_work': [],
        'batch_results': [],
        'works_detail': [],
        'error_message': '',
    }
    task_id = args.task_id
    batch_no = 0
    already_matched = set()
    matched_by_work = {}
    failed_works = set()

    try:
        log('开始执行腾讯文犀投诉任务...')
        check_login(auth)

        # 委托方 group 对象：整个任务共用一个委托方，拉一次即可
        log(f'获取委托方详情: {delegate_code}')
        group, delegate_id, delegate_code_r = fetch_delegate_details(auth, delegate_code)
        log(f"委托方: {group.get('name', '')} (delegateId={delegate_id})")

        for work_idx, work in enumerate(works_config):
            work_name = work['work_name']
            origin_url = work.get('origin_url', '')
            links = work.get('links', [])
            proof_path = work.get('proof_path', '')
            log(f"[{work_idx+1}/{len(works_config)}] 处理作品: {work_name} ({len(links)}条链接)")

            try:
                # 权属证明（每作品单独上传，得 attach 项）
                attach_item = upload_cos(auth, proof_path)
                attach = [attach_item]

                result['works_detail'].append({
                    'work_index': work_idx, 'work_name': work_name, 'status': 'processing',
                })

                work_matched = []
                for chunk_start in range(0, len(links), MAX_LINKS_PER_SUBMISSION):
                    batch_no += 1
                    chunk = links[chunk_start:chunk_start + MAX_LINKS_PER_SUBMISSION]
                    log(f'  提交批次 {batch_no}: {len(chunk)}条链接 '
                        f'(行{chunk_start+1}-{chunk_start+len(chunk)})')

                    # 提交前 URL 校验（失败不阻断，仅告警）
                    fc = format_check(auth, meta['appKey'], origin_url, chunk)
                    if fc.get('code') != 0:
                        log(f"  ⚠️ URL 校验未通过: {fc.get('message', fc)}（继续尝试提交）")

                    payload = build_payload(meta, group, delegate_id, delegate_code_r,
                                            subject_group, work_name, origin_url, chunk, attach)
                    resp_data = submit_complaint(auth, payload)

                    if resp_data.get('code') == 0:
                        log(f'  批次 {batch_no} 提交成功')
                        result['completed_batches'] += 1
                        result['batch_results'].append({
                            'batch_no': batch_no, 'work_name': work_name,
                            'status': 'completed', 'link_count': len(chunk),
                        })
                        rid = match_complaint_id(auth, work_name, chunk, already_matched)
                        if rid:
                            work_matched.append(rid)
                    else:
                        err = resp_data.get('message') or resp_data.get('_raw_text') or '提交失败'
                        log(f"  批次 {batch_no} 失败: {err} "
                            f"(code={resp_data.get('code')}, HTTP={resp_data.get('_http_status')})")
                        result['failed_batches'] += 1
                        result['batch_results'].append({
                            'batch_no': batch_no, 'work_name': work_name,
                            'status': 'failed', 'error': err,
                        })
                    time.sleep(2)

                if work_matched:
                    matched_by_work[work_name] = work_matched
                if not work_matched and not any(
                        b['work_name'] == work_name and b['status'] == 'completed'
                        for b in result['batch_results']):
                    failed_works.add(work_name)

                _rebuild_numbers(result, works_config, failed_works, matched_by_work)
                save_partial_result(task_id, result)

            except Exception as e:
                log(f"  ❌ 作品「{work_name}」处理异常，跳过: {e}")
                failed_works.add(work_name)
                for chunk_start in range(0, max(len(links), 1), MAX_LINKS_PER_SUBMISSION):
                    batch_no += 1
                    result['failed_batches'] += 1
                    result['batch_results'].append({
                        'batch_no': batch_no, 'work_name': work_name,
                        'status': 'failed', 'error': str(e),
                    })
                _rebuild_numbers(result, works_config, failed_works, matched_by_work)
                save_partial_result(task_id, result)
                continue

        _rebuild_numbers(result, works_config, failed_works, matched_by_work)
        if result['failed_batches'] == 0:
            result['status'] = 'completed'
        elif result['completed_batches'] > 0:
            result['status'] = 'partial_failed'
        else:
            result['status'] = 'failed'
        result['completed_at'] = datetime.now().isoformat()
        log(f"任务完成: 状态={result['status']}, 成功={result['completed_batches']}, "
            f"失败={result['failed_batches']}, 单号={result['feedback_numbers']}")

    except Exception as e:
        result['error_message'] = str(e)
        result['status'] = 'partial_failed' if result['completed_batches'] > 0 else 'failed'
        result['completed_at'] = datetime.now().isoformat()
        try:
            save_partial_result(result.get('task_id'), result)
        except Exception:
            pass
        log(f'任务异常终止: {e}')

    print('JSON_RESULT_START')
    print(json.dumps(result, ensure_ascii=False))
    print('JSON_RESULT_END')
    return 0 if result['status'] in ('completed', 'partial_failed') else 1


if __name__ == '__main__':
    sys.exit(main())

