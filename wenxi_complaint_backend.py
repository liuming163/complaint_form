#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""腾讯文犀(ri.qq.com)版权投诉自动化脚本 - 纯API版本

与其他平台最大差异：**无 cookie，用 token 鉴权**。3 个请求头：
  Authorization: <token>（裸 JWT，无 Bearer 前缀）、sessionId、uid
token 约 2.5h 过期，失效时 API 返回 {code:20002}。

一单=1部作品；侵权链接直接放进 payload.right.rightUrls 数组提交；
每批最多1000条链接（平台无硬限制，实测22条正常），超出自动拆单。
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
import io
import json
import math
import os
import random
import re
import signal
import string
import sys
import time
from datetime import datetime, timezone

import requests

BASE_URL = 'https://ri.qq.com/api/v1'
MAX_LINKS_PER_BATCH = 1000  # 每批最多链接数（平台无硬限制，实测22条正常，保守取1000）
COS_SIGN_SALT = 'cl_law_complaint'  # pre-token 签名固定盐（前端硬编码）
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
WENXI_COS_CA_BUNDLE = os.path.join(PROJECT_ROOT, 'certs', 'wenxi_cos_ca_bundle.pem')

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


def _cos_verify_path() -> str:
    """COS 代理服务漏发 DigiCert 中间证书，使用项目内补链后的 CA bundle。"""
    if os.path.exists(WENXI_COS_CA_BUNDLE):
        return WENXI_COS_CA_BUNDLE
    log(f'  ⚠️ 文犀 COS CA bundle 不存在，改用 requests 默认 CA: {WENXI_COS_CA_BUNDLE}')
    return True


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
                          data=file_bytes, timeout=120, verify=_cos_verify_path())
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
            'successCount': len(right_urls),
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


def _normalize_url(u: str) -> str:
    """链接比对用：仅去首尾空白，做精确比对（不做任何归一化）。

    平台不改写链接（srcUrl 与提交原样一致），且 http/https、www./m.、尾斜杠
    在业务上是【不同的合法链接】（可分别投诉），故绝不能归一化合并——
    否则会把用户有意提交的多个变体误判为同一条，破坏「集合相等」单号匹配。
    """
    return (u or '').strip()


def _parse_utc(ts: str):
    """解析 createdAtUtc（如 2026-07-15T17:25:31.560156+08:00）为带时区 datetime。失败返回 None。"""
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts)
    except Exception:
        return None


def _fetch_complaint_urls(auth: dict, request_id) -> set:
    """拉某单的侵权链接集合（归一化）。URL 有索引延迟，可能返回空。"""
    try:
        resp = requests.get(f'{BASE_URL}/complaint/details/urls',
                            headers=make_headers(auth),
                            params={'id': request_id, 'limit': 50, 'offset': 0, 'total': 0},
                            timeout=20)
        data = resp.json()
    except Exception:
        return set()
    results = ((data.get('data') or {}).get('results')) or []
    urls = set()
    for r in results:
        u = r.get('srcUrl') or (r.get('resolution') or {}).get('url') or ''
        if u:
            urls.add(_normalize_url(u))
    return urls


def match_complaint_id(auth: dict, work_name: str, submitted_urls: list,
                       already_matched: set, submit_ts=None, retries: int = 4) -> str:
    """按 作品名 + 提交时间 + 链接集合 三重约束匹配 ticketNo 作为单号。

    单号=ticketNo（如 T71526001301124096）；作品名=originName；详情ID=requestId。
    为什么不能只按作品名或"链接有交集"：同一作品的基础链接（如 mkzhan.com/209673）
    会出现在几乎所有历史单里，交集非空会误命中多个历史单。故用三重约束：
      ① originName 服务端过滤，缩小到同作品候选；
      ② createdAtUtc > submit_ts（留 60s 容差），排除历史同名单；
      ③ 该单的 srcUrl 集合与本批提交链接集合【精确相等】，锁定这一批
         （拆多单时 851400.html 与 851456.html 能干净区分）。
    链接为精确比对（仅去首尾空白，不归一化）：http/https、www./m.、尾斜杠
    均视为不同的合法链接，与上传去重规则一致。
    链接有索引延迟，故多次重试等待。集合相等拿不到时，退化为"提交集 ⊆ 单链接集"
    的子集匹配（末次重试才用），仍要求 ①②，避免误认历史单。
    """
    want = {_normalize_url(u) for u in submitted_urls if u}
    if not want:
        return ''
    # 容差：提交时刻前推 60s，避免服务端/本地时钟微差把刚提交的单排除
    cutoff = None
    if submit_ts is not None:
        cutoff = submit_ts.timestamp() - 60

    subset_fallback = None  # (ticket_no) 子集候选，末次重试才用
    for attempt in range(retries):
        time.sleep(3)
        try:
            resp = requests.get(f'{BASE_URL}/complaint/list',
                                headers=make_headers(auth),
                                params={'limit': 20, 'offset': 0,
                                        'delegateSubjectType': '', 'originName': work_name},
                                timeout=20)
            data = resp.json()
        except Exception as e:
            log(f'  查询 complaint/list 异常(第{attempt+1}次): {e}')
            continue

        results = ((data.get('data') or {}).get('results')) or []
        for rec in results:
            ticket_no = rec.get('ticketNo', '')
            request_id = rec.get('requestId')
            if not ticket_no or ticket_no in already_matched:
                continue
            # 约束①：作品名（服务端已按 originName 过滤，这里再兜底一次）
            if rec.get('originName', '') != work_name:
                continue
            # 约束②：时间——只认提交时刻之后的新单
            if cutoff is not None:
                created = _parse_utc(rec.get('createdAtUtc', ''))
                if created is not None and created.timestamp() < cutoff:
                    continue
            # 约束③：链接集合相等
            rec_urls = _fetch_complaint_urls(auth, request_id)
            if not rec_urls:
                continue  # URL 还没索引出来，下轮重试
            if rec_urls == want:
                already_matched.add(ticket_no)
                log(f'  匹配到单号(ticketNo): {ticket_no} (作品: {work_name}, 链接集合精确相等)')
                return ticket_no
            # 子集兜底：提交集 ⊆ 单链接集（该单可能含更多链接），末次才用
            if want <= rec_urls and subset_fallback is None:
                subset_fallback = ticket_no

        # 末次重试仍无精确相等，用子集兜底
        if attempt == retries - 1 and subset_fallback:
            already_matched.add(subset_fallback)
            log(f'  匹配到单号(ticketNo): {subset_fallback} (作品: {work_name}, 子集兜底匹配)')
            return subset_fallback
    return ''


def save_partial_result(task_id, result):
    """增量落盘进度（与其它平台一致），供 app.py 超时回收。"""
    try:
        result_dir = os.path.join(PROJECT_ROOT, 'task_results')
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

    # 信号处理：确保超时时也能输出最终结果
    def handle_timeout(signum, frame):
        log('⚠️ 收到超时信号，准备输出当前结果...')
        result['status'] = 'partial_failed' if result['completed_batches'] > 0 else 'failed'
        result['error_message'] = '执行超时'
        result['completed_at'] = datetime.now().isoformat()
        try:
            save_partial_result(args.task_id, result)
        except Exception:
            pass
        print('JSON_RESULT_START')
        print(json.dumps(result, ensure_ascii=False))
        print('JSON_RESULT_END')
        sys.exit(1)

    signal.signal(signal.SIGTERM, handle_timeout)
    signal.signal(signal.SIGINT, handle_timeout)

    task_id = args.task_id
    total_batches = sum(
        max(1, math.ceil(len(w.get('links', [])) / MAX_LINKS_PER_BATCH))
        for w in works_config
    )
    batch_no = 0
    already_matched = set()
    matched_by_work = {}
    failed_works = set()

    try:
        log('开始执行腾讯文犀投诉任务...')
        log(f'总作品数: {len(works_config)}, 预计批次: {total_batches}')
        check_login(auth)

        # 委托方 group 对象：整个任务共用一个委托方，拉一次即可
        log(f'获取委托方详情: {delegate_code}')
        group, delegate_id, delegate_code_r = fetch_delegate_details(auth, delegate_code)
        log(f"委托方: {group.get('name', '')} (delegateId={delegate_id})")

        for work_idx, work in enumerate(works_config):
            work_name = work['work_name']
            api_work_name = work.get('api_work_name', work_name)
            origin_url = work.get('origin_url', '')
            links = work.get('links', [])
            proof_path = work.get('proof_path', '')
            chunks = [links[i:i + MAX_LINKS_PER_BATCH]
                      for i in range(0, max(len(links), 1), MAX_LINKS_PER_BATCH)]
            log(f"[{work_idx+1}/{len(works_config)}] 处理作品: {work_name} "
                f"({len(links)}条链接, {len(chunks)}批)")

            work_start_batch_no = batch_no
            work_matched = []
            work_has_completed_batch = False
            try:
                # 权属证明（每作品单独上传，得 attach 项）
                attach_item = upload_cos(auth, proof_path)
                attach = [attach_item]

                result['works_detail'].append({
                    'work_index': work_idx, 'work_name': work_name, 'status': 'processing',
                })

                for chunk in chunks:
                    batch_no += 1
                    log(f'  批次 {batch_no}: {len(chunk)}条链接')

                    # 提交前 URL 校验（失败不阻断，仅告警）
                    fc = format_check(auth, meta['appKey'], origin_url, chunk)
                    if fc.get('code') != 0:
                        log(f"  ⚠️ URL 校验未通过: {fc.get('message', fc)}（继续尝试提交）")

                    submit_ts = datetime.now(timezone.utc)
                    payload = build_payload(meta, group, delegate_id, delegate_code_r,
                                            subject_group, api_work_name, origin_url, chunk, attach)
                    resp_data = submit_complaint(auth, payload)

                    if resp_data.get('code') == 0:
                        log(f'  批次 {batch_no} 提交成功')
                        work_has_completed_batch = True
                        result['completed_batches'] += 1
                        batch_result = {
                            'batch_no': batch_no, 'work_name': work_name,
                            'status': 'completed', 'link_count': len(chunk),
                        }
                        result['batch_results'].append(batch_result)
                        rid = match_complaint_id(auth, api_work_name, chunk, already_matched, submit_ts)
                        if rid:
                            batch_result['feedback_number'] = rid
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
                    if work_idx < len(works_config) - 1 or chunk != chunks[-1]:
                        # 降低延迟：减少总执行时间，避免超时；最后一批无需等待
                        time.sleep(random.randint(30, 90))

                if work_matched:
                    matched_by_work[work_name] = work_matched
                if not work_matched and not work_has_completed_batch:
                    failed_works.add(work_name)

                for wd in result['works_detail']:
                    if wd.get('work_index') == work_idx:
                        wd['status'] = 'completed' if work_matched else ('partial_failed' if work_has_completed_batch else 'failed')
                        break
                _rebuild_numbers(result, works_config, failed_works, matched_by_work)
                save_partial_result(task_id, result)

            except Exception as e:
                import traceback
                log(f"  ❌ 作品「{work_name}」处理异常，跳过: {e}")
                log(f"  异常堆栈: {traceback.format_exc()}")
                if work_matched:
                    matched_by_work[work_name] = work_matched
                elif not work_has_completed_batch:
                    failed_works.add(work_name)
                for wd in result['works_detail']:
                    if wd.get('work_index') == work_idx:
                        wd['status'] = 'partial_failed' if work_has_completed_batch else 'failed'
                        wd['error'] = str(e)
                        break
                expected_batch_nos = set(range(work_start_batch_no + 1, work_start_batch_no + len(chunks) + 1))
                recorded_batch_nos = {
                    br.get('batch_no') for br in result['batch_results']
                    if br.get('batch_no') in expected_batch_nos
                }
                for failed_batch_no in sorted(expected_batch_nos - recorded_batch_nos):
                    result['failed_batches'] += 1
                    result['batch_results'].append({
                        'batch_no': failed_batch_no, 'work_name': work_name,
                        'status': 'failed', 'error': str(e),
                    })
                batch_no = max(batch_no, work_start_batch_no + len(chunks))
                _rebuild_numbers(result, works_config, failed_works, matched_by_work)
                save_partial_result(task_id, result)
                continue

            # 进度日志
            log(f"✓ 作品「{work_name}」处理完成 ({work_idx + 1}/{len(works_config)}), "
                f"已完成批次 {result['completed_batches']}/{total_batches}")

        _rebuild_numbers(result, works_config, failed_works, matched_by_work)
        if result['failed_batches'] == 0:
            result['status'] = 'completed'
        elif result['completed_batches'] > 0:
            result['status'] = 'partial_failed'
        else:
            result['status'] = 'failed'
        result['completed_at'] = datetime.now().isoformat()
        save_partial_result(task_id, result)
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

