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


def build_complaint_form(complaint_type_code, description, url_list, actual_name, actual_url):
    return {
        'complaint_type': complaint_type_code,
        'description': description,
        'url_list': url_list,
        'actual_name': actual_name,
        'actual_url': actual_url,
        'infringe_type': None,
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
    data = resp.json()
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


def get_feedback_detail(cookie, feedback_id):
    resp = requests.get(
        f'{BASE_URL}/feedback/detail/{feedback_id}',
        headers=make_headers(cookie),
        timeout=15,
    )
    data = resp.json()
    if data.get('code') == 200:
        return data.get('data')
    return None


def main():
    parser = argparse.ArgumentParser(description='百度版权投诉自动化脚本')
    parser.add_argument('--task-id', required=True)
    parser.add_argument('--cookie', required=True)
    parser.add_argument('--complaint-type-code', required=True, type=int)
    parser.add_argument('--works-config', required=True)
    args = parser.parse_args()

    works_config = json.loads(args.works_config)
    cookie = args.cookie
    complaint_type_code = args.complaint_type_code

    result = {
        'task_id': args.task_id,
        'status': 'running',
        'started_at': datetime.now().isoformat(),
        'completed_batches': 0,
        'failed_batches': 0,
        'feedback_numbers': [],
        'batch_results': [],
        'works_detail': [],
        'error_message': '',
    }

    batch_no = 0

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

            # 搜索权属
            log(f"  搜索权属记录...")
            ownership, status_hint = search_ownership(cookie, work_name)
            if not ownership:
                if status_hint == 'rejected':
                    error_msg = f"作品'{work_name}'权属状态未通过，请在百度投诉原平台进行投诉"
                else:
                    error_msg = f"作品'{work_name}'未找到权属记录，请在百度投诉原平台进行投诉"
                log(f"  错误: {error_msg}")
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

            # 按200条分片提交
            for chunk_start in range(0, len(links), MAX_LINKS_PER_SUBMISSION):
                batch_no += 1
                chunk = links[chunk_start:chunk_start + MAX_LINKS_PER_SUBMISSION]
                url_list = [{'link_name': lk.get('link_name', ''), 'url_address': lk.get('url_address', '')} for lk in chunk]

                log(f"  提交批次 {batch_no}: {len(chunk)}条链接 (行{chunk_start+1}-{chunk_start+len(chunk)})")

                complaint_form = build_complaint_form(
                    complaint_type_code, description, url_list, actual_name, actual_url
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
                    else:
                        error_msg = resp_data.get('message', '提交失败')
                        log(f"  批次 {batch_no} 失败: {error_msg}")
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

            time.sleep(1)

        # 查询反馈单号（按作品顺序，通过链接地址精确匹配）
        expected_count = result['completed_batches']
        log(f'查询反馈单号（预期{expected_count}个）...')
        time.sleep(3)
        today_start_ts = int(datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)

        # 收集本次提交的所有链接地址（按作品分组）
        submitted_urls_by_work = {}
        for work in works_config:
            wn = work['work_name']
            urls = set()
            for lk in work.get('links', []):
                url = lk.get('url_address', '').split('?')[0].split('#')[0]
                if url:
                    urls.add(url)
            submitted_urls_by_work[wn] = urls

        # 记录哪些作品失败了
        failed_works = set()
        for wd in result['works_detail']:
            if wd.get('status') == 'failed':
                failed_works.add(wd['work_name'])

        def try_match_feedback_by_work(existing_matched=None):
            matched_by_work = dict(existing_matched) if existing_matched else {}
            already_found_works = set(matched_by_work.keys())

            for work in works_config:
                work_name = work['work_name']
                if work_name in failed_works or work_name in already_found_works:
                    continue
                submitted_urls = submitted_urls_by_work.get(work_name, set())
                if not submitted_urls:
                    continue

                feedbacks = query_feedback(cookie, keyword=work_name, page=1, size=20)
                for fb in feedbacks:
                    fn = fb.get('feedback_number')
                    fb_date = fb.get('feedback_date', 0)
                    if not fn or fb_date < today_start_ts:
                        continue
                    # 避免重复匹配
                    already_matched = set()
                    for nums in matched_by_work.values():
                        already_matched.update(nums)
                    if fn in already_matched:
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
                        if work_name not in matched_by_work:
                            matched_by_work[work_name] = []
                        matched_by_work[work_name].append(fn)
                        log(f'  匹配到反馈单号: {fn} (作品: {work_name})')

                    time.sleep(0.5)
            return matched_by_work

        matched_by_work = try_match_feedback_by_work()

        # 如果数量不足，等待后重试（最多重试2次，合并结果）
        total_matched = sum(len(v) for v in matched_by_work.values())
        for retry in range(2):
            if total_matched >= expected_count:
                break
            log(f'  当前匹配到{total_matched}个，不足{expected_count}个，等待5秒后第{retry+1}次重试...')
            time.sleep(5)
            matched_by_work = try_match_feedback_by_work(existing_matched=matched_by_work)
            total_matched = sum(len(v) for v in matched_by_work.values())

        # 按作品顺序组装反馈单号列表（失败的标记为"投诉失败"）
        ordered_numbers = []
        # 同时按作品分组保存，供导出时「一部作品多批次=多单号」正确配对。
        # 与 ordered_numbers 内容一致，只是按作品聚合（保持 works_config 顺序）。
        feedback_numbers_by_work = []
        for work in works_config:
            work_name = work['work_name']
            if work_name in failed_works:
                ordered_numbers.append(f"投诉失败:{work_name}")
                work_numbers = [f"投诉失败:{work_name}"]
            elif work_name in matched_by_work:
                work_numbers = [str(fn) for fn in matched_by_work[work_name]]
                ordered_numbers.extend(work_numbers)
            else:
                ordered_numbers.append(f"未获取到单号:{work_name}")
                work_numbers = [f"未获取到单号:{work_name}"]
            feedback_numbers_by_work.append({
                'work_name': work_name,
                'numbers': work_numbers,
            })

        result['feedback_numbers'] = ordered_numbers
        result['feedback_numbers_by_work'] = feedback_numbers_by_work
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
        result['status'] = 'failed'
        result['error_message'] = str(e)
        result['completed_at'] = datetime.now().isoformat()
        log(f"任务异常终止: {str(e)}")

    print('JSON_RESULT_START')
    print(json.dumps(result, ensure_ascii=False))
    print('JSON_RESULT_END')

    return 0 if result['status'] in ('completed', 'partial_failed') else 1


if __name__ == '__main__':
    sys.exit(main())
