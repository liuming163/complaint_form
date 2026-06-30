#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""百度版权投诉自动化脚本 - 纯API版本"""

import argparse
import json
import sys
import time
import requests
from datetime import datetime

BASE_URL = 'https://newcopyright.baidu.com'
MAX_LINKS_PER_SUBMISSION = 200


def make_headers(cookie):
    return {
        'Cookie': cookie,
        'Content-Type': 'application/json;charset=UTF-8',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Referer': 'https://newcopyright.baidu.com/',
        'Origin': 'https://newcopyright.baidu.com',
    }


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def check_login(cookie):
    resp = requests.get(f'{BASE_URL}/login/check', headers=make_headers(cookie), timeout=10)
    data = resp.json()
    if data.get('code') != 200 or not data.get('data', {}).get('uid'):
        raise RuntimeError('Cookie无效或已过期')
    log(f"登录验证通过: {data['data'].get('display_name', '')}")
    return data['data']


def get_user_info(cookie):
    resp = requests.get(f'{BASE_URL}/user/info', headers=make_headers(cookie), timeout=10)
    data = resp.json()
    if data.get('code') != 200:
        raise RuntimeError(f"获取用户信息失败: {data.get('message', '')}")
    return data['data']


def search_ownership(cookie, work_name):
    """返回 (record, status_hint)
    - 找到已通过: (record, None)
    - 找到但未通过: (None, 'rejected')
    - 未找到: (None, 'not_found')
    """
    resp = requests.post(
        f'{BASE_URL}/ownership/keyword',
        headers=make_headers(cookie),
        json={'page': 1, 'size': 50, 'lastPageNo': 0, 'key_word': work_name, 'owner_type': 0},
        timeout=15,
    )
    data = resp.json()
    if data.get('code') != 200:
        return None, 'not_found'
    records = data.get('data', {}).get('records', [])
    found_but_not_passed = False
    for record in records:
        if record.get('works_name', '') == work_name:
            if record.get('ownership_status') == 2:
                return record, None
            else:
                found_but_not_passed = True
    if found_but_not_passed:
        return None, 'rejected'
    return None, 'not_found'


def get_ownership_detail(cookie, cp_id):
    resp = requests.get(
        f'{BASE_URL}/ownership/{cp_id}',
        headers=make_headers(cookie),
        timeout=15,
    )
    data = resp.json()
    if data.get('code') == 200:
        return data.get('data')
    return None


def ts_to_date(ts):
    if ts is None:
        return None
    if isinstance(ts, str):
        return ts
    return datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d')


def build_user_form(user_info):
    return {
        'complaint_account': user_info.get('complaint_account', ''),
        'user_type': user_info.get('user_type', 2),
        'user_name': user_info.get('user_name', ''),
        'company_code': user_info.get('company_code', ''),
        'license_url': user_info.get('license_url', ''),
        'card_start_date': ts_to_date(user_info.get('card_start_date')),
        'card_end_date': ts_to_date(user_info.get('card_end_date')),
        'legal_person': user_info.get('legal_person', ''),
        'contact_name': user_info.get('contact_name', ''),
        'email': user_info.get('email', ''),
        'mobile_phone': user_info.get('mobile_phone', ''),
        'reputation_type': user_info.get('reputation_type', 2),
        'update': False,
        'id_card': user_info.get('id_card', ''),
        'pics_url': user_info.get('pics_url', []),
        'pass_display_name': '',
        'code_type': user_info.get('code_type', 1),
    }


def build_ownership_form(ownership_detail):
    return {
        'cp_id': ownership_detail.get('cp_id', ''),
        'owner_type': ownership_detail.get('owner_type', 2),
        'update': True,
        'works_name': ownership_detail.get('works_name', ''),
        'works_category': ownership_detail.get('works_category', 2),
        'works_certificate_type': ownership_detail.get('works_certificate_type', 0),
        'works_certificate_url': ownership_detail.get('works_certificate_url', '{}'),
        'works_start_date': ts_to_date(ownership_detail.get('works_start_date')),
        'works_end_date': ts_to_date(ownership_detail.get('works_end_date')),
        'pseudonym': ownership_detail.get('pseudonym', ''),
        'contact_name': ownership_detail.get('contact_name', ''),
        'mobile_phone': ownership_detail.get('mobile_phone', ''),
        'email': ownership_detail.get('email', ''),
        'owner_url': ownership_detail.get('owner_url', []),
        'authorization_url': ownership_detail.get('authorization_url', []),
    }


def build_complaint_form(complaint_type_code, description, url_list, actual_name, actual_url, infringe_type=None):
    return {
        'complaint_type': complaint_type_code,
        'description': description,
        'url_list': url_list,
        'actual_name': actual_name,
        'actual_url': actual_url,
        'infringe_type': infringe_type,
    }


def submit_complaint(cookie, user_form, ownership_form, complaint_form):
    payload = {
        'create_user_form': user_form,
        'create_owner_ship_form': ownership_form,
        'create_complaint_form': complaint_form,
    }
    resp = requests.post(
        f'{BASE_URL}/upload',
        headers=make_headers(cookie),
        json=payload,
        timeout=30,
    )
    # 保留 HTTP 状态码与原始响应体：百度失败时返回的 JSON 可能不含 message，
    # 只看解析后的 data 会丢失真实原因（如重复投诉、限流、字段校验等业务码）。
    try:
        data = resp.json()
    except Exception:
        data = {}
    data['_http_status'] = resp.status_code
    data['_raw_text'] = resp.text
    return data


def query_feedback(cookie, keyword='', page=1, size=10):
    resp = requests.get(
        f'{BASE_URL}/feedback/list',
        headers=make_headers(cookie),
        params={'page': page, 'size': size, 'status': 0, 'keyWord': keyword},
        timeout=15,
    )
    data = resp.json()
    if data.get('code') == 200:
        return data.get('data', {}).get('records', [])
    return []


def query_feedback_all(cookie, keyword='', max_pages=3, size=50):
    """翻多页拉取反馈列表，合并去重（按 id）。

    大批量投诉时单作品当天反馈可能超过一页，只查第一页会漏号。
    """
    seen_ids = set()
    records = []
    for page in range(1, max_pages + 1):
        try:
            page_records = query_feedback(cookie, keyword=keyword, page=page, size=size)
        except Exception as e:
            log(f"  ⚠️ 查询反馈列表第{page}页异常: {e}")
            break
        if not page_records:
            break
        for r in page_records:
            rid = r.get('id')
            if rid in seen_ids:
                continue
            seen_ids.add(rid)
            records.append(r)
        if len(page_records) < size:
            break  # 最后一页
    return records


def get_feedback_detail(cookie, feedback_id, retries=2):
    for attempt in range(retries):
        try:
            resp = requests.get(
                f'{BASE_URL}/feedback/detail/{feedback_id}',
                headers=make_headers(cookie),
                timeout=15,
            )
            data = resp.json()
            if data.get('code') == 200:
                return data.get('data')
        except Exception as e:
            log(f"  ⚠️ 获取反馈详情({feedback_id})异常(第{attempt+1}次): {e}")
        if attempt < retries - 1:
            time.sleep(1)
    return None


def match_feedback_for_work(cookie, work_name, submitted_urls, today_start_ts,
                            already_matched_global, attempts=3):
    """查询某一部作品的反馈单号（通过链接地址精确匹配）。

    - 翻多页拉反馈列表，按提交链接交集匹配；
    - already_matched_global：本任务已认领的单号集合，避免跨作品/重复认领；
    - attempts：单作品多次尝试（应对百度对最新提交的索引延迟），每次间隔等待。
    返回本作品匹配到的单号列表（可能多个=多批次）。
    """
    if not submitted_urls:
        return []
    submitted_urls = set(u.split('?')[0].split('#')[0] for u in submitted_urls if u)
    found = []
    for attempt in range(attempts):
        feedbacks = query_feedback_all(cookie, keyword=work_name, max_pages=3, size=50)
        for fb in feedbacks:
            fn = fb.get('feedback_number')
            fb_date = fb.get('feedback_date', 0)
            if not fn or fb_date < today_start_ts:
                continue
            if fn in already_matched_global or fn in found:
                continue
            detail = get_feedback_detail(cookie, fb.get('id'))
            if not detail:
                continue
            detail_urls = set()
            for u in detail.get('url_list', []):
                url = u.get('url_address', '').split('?')[0].split('#')[0]
                if url:
                    detail_urls.add(url)
            if detail_urls & submitted_urls:
                found.append(fn)
                already_matched_global.add(fn)
                log(f'  匹配到反馈单号: {fn} (作品: {work_name})')
            time.sleep(0.3)
        if found:
            break  # 这次拿到了就不再等待重试
        if attempt < attempts - 1:
            log(f'  作品「{work_name}」暂未匹配到单号，等待5秒后第{attempt+2}次尝试...')
            time.sleep(5)
    return found


def save_partial_result(task_id, result):
    """把当前进度写入 task_results/<task_id>.json（与 UC 一致），
    供 app.py 在超时/异常被杀时回收已成功作品的单号。"""
    try:
        import os
        result_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'task_results')
        os.makedirs(result_dir, exist_ok=True)
        with open(os.path.join(result_dir, f'{task_id}.json'), 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log(f"  ⚠️ 增量保存进度失败: {e}")


def _rebuild_feedback_numbers(result, works_config, failed_works, matched_by_work):
    """按 works_config 顺序重建 feedback_numbers(扁平) 和 feedback_numbers_by_work(分组)。
    每次进度变化都调用，保证落盘的进度文件随时可用、可被超时回收。"""
    ordered = []
    by_work = []
    for work in works_config:
        wn = work['work_name']
        if wn in matched_by_work and matched_by_work[wn]:
            nums = [str(n) for n in matched_by_work[wn]]
            ordered.extend(nums)
        elif wn in failed_works:
            nums = [f"投诉失败:{wn}"]
            ordered.append(nums[0])
        else:
            nums = [f"未获取到单号:{wn}"]
            ordered.append(nums[0])
        by_work.append({'work_name': wn, 'numbers': nums})
    result['feedback_numbers'] = ordered
    result['feedback_numbers_by_work'] = by_work


def main():
    parser = argparse.ArgumentParser(description='百度版权投诉自动化脚本')
    parser.add_argument('--task-id', required=True)
    parser.add_argument('--cookie', required=True)
    parser.add_argument('--complaint-type-code', required=True, type=int)
    parser.add_argument('--infringe-type', type=int, default=None)
    parser.add_argument('--works-config-file', required=True)
    args = parser.parse_args()

    with open(args.works_config_file, encoding='utf-8') as _f:
        works_config = json.load(_f)
    cookie = args.cookie
    complaint_type_code = args.complaint_type_code
    infringe_type = args.infringe_type

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
    # 逐作品查号用：今天0点时间戳、已认领的单号（跨作品去重）、各作品已匹配单号
    today_start_ts = int(datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
    already_matched_global = set()
    matched_by_work = {}
    failed_works = set()

    try:
        log('开始执行百度投诉任务...')
        check_login(cookie)

        log('获取用户信息...')
        user_info = get_user_info(cookie)
        user_form = build_user_form(user_info)
        log(f"用户: {user_info.get('user_name', '')}")

        for work_idx, work in enumerate(works_config):
            work_name = work['work_name']
            description = work['description']
            actual_name = work['actual_name']
            actual_url = work['actual_url']
            links = work.get('links', [])

            log(f"[{work_idx+1}/{len(works_config)}] 处理作品: {work_name} ({len(links)}条链接)")

            # 逐作品异常隔离：单部作品任意环节出错只记这部失败，不波及其它作品、
            # 也不影响已成功作品的单号（之前共用一个外层 try，一处异常全盘皆输）。
            try:
                # 搜索权属
                log(f"  搜索权属记录...")
                ownership, status_hint = search_ownership(cookie, work_name)
                if not ownership:
                    if status_hint == 'rejected':
                        error_msg = f"作品'{work_name}'权属状态未通过，请在百度投诉原平台进行投诉"
                    else:
                        error_msg = f"作品'{work_name}'未找到权属记录，请在百度投诉原平台进行投诉"
                    log(f"  错误: {error_msg}")
                    failed_works.add(work_name)
                    result['works_detail'].append({
                        'work_index': work_idx,
                        'work_name': work_name,
                        'cp_id': '',
                        'owner_type': None,
                        'works_category': None,
                        'contact_name': '',
                        'status': 'failed',
                        'error': error_msg,
                    })
                    for chunk_start in range(0, len(links), MAX_LINKS_PER_SUBMISSION):
                        batch_no += 1
                        result['batch_results'].append({
                            'batch_no': batch_no,
                            'work_name': work_name,
                            'status': 'failed',
                            'error': error_msg,
                        })
                        result['failed_batches'] += 1
                    save_partial_result(task_id, result)
                    continue

                log(f"  权属ID: {ownership.get('cp_id')}, 状态: 已通过")

                # 获取权属详情（包含授权信息）
                ownership_detail = get_ownership_detail(cookie, ownership['cp_id'])
                if not ownership_detail:
                    ownership_detail = ownership
                ownership_form = build_ownership_form(ownership_detail)

                # 记录作品权属详情
                result['works_detail'].append({
                    'work_index': work_idx,
                    'work_name': work_name,
                    'cp_id': ownership_detail.get('cp_id', ''),
                    'owner_type': ownership_detail.get('owner_type'),
                    'works_category': ownership_detail.get('works_category'),
                    'contact_name': ownership_detail.get('contact_name', ''),
                })

                # 收集本作品提交的链接地址（用于后续按链接精确匹配单号）
                submitted_urls = set()
                for lk in links:
                    u = lk.get('url_address', '').split('?')[0].split('#')[0]
                    if u:
                        submitted_urls.add(u)

                work_completed_batches = 0
                # 按200条分片提交
                for chunk_start in range(0, len(links), MAX_LINKS_PER_SUBMISSION):
                    batch_no += 1
                    chunk = links[chunk_start:chunk_start + MAX_LINKS_PER_SUBMISSION]
                    url_list = [{'link_name': lk.get('link_name', ''), 'url_address': lk.get('url_address', '')} for lk in chunk]

                    log(f"  提交批次 {batch_no}: {len(chunk)}条链接 (行{chunk_start+1}-{chunk_start+len(chunk)})")

                    complaint_form = build_complaint_form(
                        complaint_type_code, description, url_list, actual_name, actual_url, infringe_type
                    )

                    try:
                        resp_data = submit_complaint(cookie, user_form, ownership_form, complaint_form)
                        if resp_data.get('code') == 200:
                            log(f"  批次 {batch_no} 提交成功")
                            result['batch_results'].append({
                                'batch_no': batch_no,
                                'work_name': work_name,
                                'status': 'completed',
                                'link_count': len(chunk),
                            })
                            result['completed_batches'] += 1
                            work_completed_batches += 1
                        else:
                            biz_code = resp_data.get('code')
                            http_status = resp_data.get('_http_status')
                            raw_text = (resp_data.get('_raw_text') or '')[:500]
                            error_msg = resp_data.get('message') or resp_data.get('msg') or '提交失败'
                            log(f"  批次 {batch_no} 失败: {error_msg} "
                                f"(业务code={biz_code}, HTTP={http_status})")
                            log(f"  批次 {batch_no} 百度原始响应: {raw_text}")
                            result['batch_results'].append({
                                'batch_no': batch_no,
                                'work_name': work_name,
                                'status': 'failed',
                                'error': error_msg,
                            })
                            result['failed_batches'] += 1
                    except Exception as e:
                        log(f"  批次 {batch_no} 异常: {str(e)}")
                        result['batch_results'].append({
                            'batch_no': batch_no,
                            'work_name': work_name,
                            'status': 'failed',
                            'error': str(e),
                        })
                        result['failed_batches'] += 1

                    time.sleep(2)

                # 该作品所有批次提交完，立即查询它的反馈单号并增量落盘。
                # 这样即使后续作品异常/任务超时，已成功作品的单号也不丢。
                if work_completed_batches > 0:
                    log(f"  查询作品「{work_name}」反馈单号...")
                    time.sleep(2)
                    nums = match_feedback_for_work(
                        cookie, work_name, submitted_urls, today_start_ts,
                        already_matched_global, attempts=3
                    )
                    if nums:
                        matched_by_work[work_name] = nums
                    else:
                        log(f"  ⚠️ 作品「{work_name}」暂未查到单号，稍后统一补查")

                # 实时刷新 feedback_numbers，落盘进度
                _rebuild_feedback_numbers(result, works_config, failed_works, matched_by_work)
                save_partial_result(task_id, result)
                time.sleep(1)

            except Exception as e:
                # 单部作品异常：只标记这部失败，继续后面的作品
                log(f"  ❌ 作品「{work_name}」处理异常，跳过: {str(e)}")
                failed_works.add(work_name)
                if not any(wd.get('work_name') == work_name for wd in result['works_detail']):
                    result['works_detail'].append({
                        'work_index': work_idx, 'work_name': work_name,
                        'cp_id': '', 'owner_type': None, 'works_category': None,
                        'contact_name': '', 'status': 'failed', 'error': str(e),
                    })
                _rebuild_feedback_numbers(result, works_config, failed_works, matched_by_work)
                save_partial_result(task_id, result)
                continue

        # 收尾：对仍未查到单号的成功作品再补查一轮（给百度索引留时间）
        log('补查未匹配到单号的作品...')
        for work in works_config:
            wn = work['work_name']
            if wn in failed_works or wn in matched_by_work:
                continue
            submitted_urls = set()
            for lk in work.get('links', []):
                u = lk.get('url_address', '').split('?')[0].split('#')[0]
                if u:
                    submitted_urls.add(u)
            if not submitted_urls:
                continue
            nums = match_feedback_for_work(
                cookie, wn, submitted_urls, today_start_ts,
                already_matched_global, attempts=2
            )
            if nums:
                matched_by_work[wn] = nums
        _rebuild_feedback_numbers(result, works_config, failed_works, matched_by_work)
        save_partial_result(task_id, result)

        # 旧的统一查询阶段（已被逐作品查询取代）
        # feedback_numbers 已由逐作品查询 + 收尾补查实时组装并落盘。
        total_matched = sum(len(v) for v in matched_by_work.values())
        expected_count = result['completed_batches']
        if total_matched < expected_count:
            log(f'  警告: 预期{expected_count}个反馈单号，实际匹配到{total_matched}个')
        else:
            log(f'  成功匹配{total_matched}个反馈单号')

        if result['failed_batches'] == 0:
            result['status'] = 'completed'
        elif result['completed_batches'] > 0:
            result['status'] = 'partial_failed'
        else:
            result['status'] = 'failed'

        result['completed_at'] = datetime.now().isoformat()
        log(f"任务完成: 状态={result['status']}, 成功={result['completed_batches']}, 失败={result['failed_batches']}, 反馈单号={result['feedback_numbers']}")

    except Exception as e:
        # 走到这里多为前置致命错误（登录/用户信息）。但若已有成功作品，
        # 保留已查到的单号、标记 partial_failed，不要把已成功的也判为全失败。
        result['error_message'] = str(e)
        result['status'] = 'partial_failed' if result['completed_batches'] > 0 else 'failed'
        result['completed_at'] = datetime.now().isoformat()
        try:
            save_partial_result(result.get('task_id'), result)
        except Exception:
            pass
        log(f"任务异常终止: {str(e)}")

    print('JSON_RESULT_START')
    print(json.dumps(result, ensure_ascii=False))
    print('JSON_RESULT_END')

    return 0 if result['status'] in ('completed', 'partial_failed') else 1


if __name__ == '__main__':
    sys.exit(main())
