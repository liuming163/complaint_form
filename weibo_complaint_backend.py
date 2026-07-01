#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""微博版权投诉自动化脚本 - 纯API版本

与前三平台最大差异：提交前须填图形验证码。策略：
  取验证码图(同时拿到配对cookie) → ddddocr识别 → 带captcha提交
  若返回 code==25001(验证码错误) → 重取图+重识别+重投，循环 N 次
验证码错误码(25001)与业务错误(重复/链接非法等)可干净区分，故重试安全。

一单=1部作品；单次侵权链接上限100条，超过自动拆多单(多rdid)。
微博无独立单号接口，成功后抓 /rights/my 按 作品名+链接 匹配 rdid 作为单号。
"""

import argparse
import json
import re
import sys
import time
import os
import requests
from datetime import datetime

BASE_URL = 'https://service.account.weibo.com'
MAX_LINKS_PER_SUBMISSION = 100
CAPTCHA_MAX_RETRY = 8          # 单批提交时验证码最多重试次数
PAGE_SIGN = 'rights_movie'

# 各类材料上传时的 ftype（缺失会报"请上传正确格式的材料"）
FTYPE_LICENSE = 10   # 营业执照 / 身份证正反 / 一般证件
FTYPE_EMPOWER = 14   # 授权委托书
FTYPE_PROOF = 13     # 权属证明(pro_pics)

_OCR = None


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def _get_ocr():
    """懒加载 ddddocr（onnx 模型首次加载较慢，只初始化一次）。"""
    global _OCR
    if _OCR is None:
        import ddddocr
        _OCR = ddddocr.DdddOcr(show_ad=False)
    return _OCR


def _cookie_str_to_dict(cookie: str) -> dict:
    jar = {}
    for part in cookie.split(';'):
        part = part.strip()
        if not part or '=' not in part:
            continue
        k, v = part.split('=', 1)
        jar[k.strip()] = v.strip()
    return jar


def make_headers(cookie: str) -> dict:
    return {
        'Cookie': cookie,
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 '
                      '(KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36',
        'Referer': f'{BASE_URL}/rights/movie',
        'Origin': BASE_URL,
        'x-requested-with': 'XMLHttpRequest',
    }


def check_login(cookie: str) -> None:
    """抓投诉主页面，靠 $CONFIG['islogin'] 判断登录态。"""
    resp = requests.get(f'{BASE_URL}/rights/movie', headers=make_headers(cookie), timeout=15)
    m = re.search(r"\$CONFIG\['islogin'\]\s*=\s*(\d)", resp.text)
    if not m or m.group(1) != '1':
        raise RuntimeError('Cookie无效或已过期（未登录）')
    nick = re.search(r"\$CONFIG\['nick'\]\s*=\s*'([^']*)'", resp.text)
    log(f"登录验证通过: {nick.group(1) if nick else ''}")


def fetch_captcha(cookie: str):
    """取一张验证码图，返回 (captcha_text, captcha_cookie)。

    该接口 set-cookie 一个 weibo_complaint_captcha_rights_movie=<hash>，是这张图的
    服务端句柄，提交时必须带上配对使用。用 requests.Session 自动接住这个 cookie。
    """
    sess = requests.Session()
    for k, v in _cookie_str_to_dict(cookie).items():
        sess.cookies.set(k, v)
    resp = sess.get(
        f'{BASE_URL}/image/getcaptcha',
        params={'page_sign': PAGE_SIGN, 'rand': str(time.time())},
        headers={
            'User-Agent': make_headers(cookie)['User-Agent'],
            'Referer': f'{BASE_URL}/rights/movie',
            'Accept': 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8',
        },
        timeout=15,
    )
    if resp.status_code != 200 or not resp.content:
        raise RuntimeError(f'获取验证码失败: HTTP {resp.status_code}')
    captcha_cookie = sess.cookies.get(f'weibo_complaint_captcha_{PAGE_SIGN}', '')
    text = _get_ocr().classification(resp.content)
    text = re.sub(r'[^0-9a-zA-Z]', '', text or '')
    return text, captcha_cookie


def _guess_mime(file_path: str) -> str:
    """按扩展名推断 MIME。微博上传接口靠 Content-Type 判断"格式"，
    requests 的两元组 files 不带 MIME 会被判"请上传正确格式的材料"。"""
    ext = os.path.splitext(file_path)[1].lower()
    return {
        '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg',
        '.png': 'image/png', '.gif': 'image/gif',
        '.bmp': 'image/bmp', '.webp': 'image/webp',
        '.pdf': 'application/pdf',
    }.get(ext, 'application/octet-stream')


def upload_image(cookie: str, file_path: str, ftype: int) -> str:
    """上传材料图，返回 picid。文件不存在返回空串。"""
    if not file_path or not os.path.exists(file_path):
        return ''
    headers = make_headers(cookie)
    headers.pop('x-requested-with', None)
    mime = _guess_mime(file_path)
    with open(file_path, 'rb') as f:
        resp = requests.post(
            f'{BASE_URL}/aj/upload/uploadfile2s3',
            headers=headers,
            data={'ftype': str(ftype)},
            files={'file': (os.path.basename(file_path), f, mime)},
            timeout=60,
        )
    data = resp.json()
    if str(data.get('code')) != '100000':
        raise RuntimeError(f"材料上传失败({os.path.basename(file_path)}): {data.get('msg', '')}")
    inner = data.get('data')
    if isinstance(inner, str):
        inner = json.loads(inner)
    picid = (inner or {}).get('picid', '')
    if not picid:
        raise RuntimeError(f"材料上传未返回picid: {os.path.basename(file_path)}")
    log(f"材料上传成功: {os.path.basename(file_path)} → picid={picid}")
    return str(picid)


def _build_films_json(work_name: str) -> str:
    """films 字段：《》包裹的作品名，JSON 数组字符串。"""
    name = work_name.strip()
    if not (name.startswith('《') and name.endswith('》')):
        name = f'《{name}》'
    return json.dumps([name], ensure_ascii=False)


def submit_once(cookie: str, form: dict, captcha_text: str, captcha_cookie: str) -> dict:
    """带验证码提交一次。返回解析后的响应 dict（含 code/msg）。"""
    # 把这张验证码配对的 cookie 覆盖进请求 cookie
    jar = _cookie_str_to_dict(cookie)
    if captcha_cookie:
        jar[f'weibo_complaint_captcha_{PAGE_SIGN}'] = captcha_cookie
    merged_cookie = '; '.join(f'{k}={v}' for k, v in jar.items())

    headers = make_headers(merged_cookie)
    headers['Content-Type'] = 'application/x-www-form-urlencoded; charset=UTF-8'

    payload = dict(form)
    payload['captcha'] = captcha_text
    resp = requests.post(
        f'{BASE_URL}/aj/rights/movie',
        headers=headers,
        data=payload,
        timeout=30,
    )
    try:
        data = resp.json()
    except Exception:
        # 成功时 response 可能为空体 + HTTP 200
        data = {'code': 100000, 'msg': ''} if resp.status_code == 200 and not resp.text.strip() else {}
    data['_http_status'] = resp.status_code
    data['_raw_text'] = resp.text[:500]
    return data


def submit_with_captcha_retry(cookie: str, form: dict) -> dict:
    """提交一批，验证码错误(25001)时自动重取图重投，直到成功或耗尽重试。

    返回 {'ok': bool, 'code', 'msg', 'attempts'}。
    """
    last = {}
    for attempt in range(1, CAPTCHA_MAX_RETRY + 1):
        try:
            captcha_text, captcha_cookie = fetch_captcha(cookie)
        except Exception as e:
            log(f"  验证码获取/识别异常(第{attempt}次): {e}")
            time.sleep(1)
            continue
        if not captcha_text:
            log(f"  验证码识别为空(第{attempt}次)，重取")
            continue
        data = submit_once(cookie, form, captcha_text, captcha_cookie)
        code = data.get('code')
        try:
            code = int(code)
        except (TypeError, ValueError):
            pass
        last = data
        if code == 100000:
            return {'ok': True, 'code': code, 'msg': data.get('msg', ''), 'attempts': attempt}
        if code == 25001:
            log(f"  验证码错误(识别值={captcha_text})，第{attempt}次重试...")
            time.sleep(0.5)
            continue
        # 其它业务错误：不是验证码问题，停止重试
        return {'ok': False, 'code': code, 'msg': data.get('msg') or data.get('_raw_text', '') or '提交失败',
                'attempts': attempt}
    return {'ok': False, 'code': last.get('code'),
            'msg': f"验证码重试{CAPTCHA_MAX_RETRY}次仍失败", 'attempts': CAPTCHA_MAX_RETRY}


def match_rdid(cookie: str, work_name: str, submitted_urls: list,
               already_matched: set, retries: int = 3) -> str:
    """提交成功后抓 /rights/my，按侵权链接匹配最新 rdid 作为单号。

    /rights/my 服务端直接渲染 HTML：每条投诉一行带 rdid 属性，并有隐藏字段
    rdid_url_<rdid>（该单链接）。按提交链接交集匹配，取未认领过的 rdid。
    """
    if not submitted_urls:
        return ''
    want = set(u.split('?')[0].split('#')[0] for u in submitted_urls if u)
    for attempt in range(retries):
        time.sleep(2)
        try:
            resp = requests.get(f'{BASE_URL}/rights/my', headers=make_headers(cookie), timeout=20)
            html = resp.text
        except Exception as e:
            log(f"  抓取 /rights/my 异常(第{attempt+1}次): {e}")
            continue
        # 收集所有 rdid（按出现顺序，最新在前）
        rdids = re.findall(r'rdid="(\d+)"', html)
        seen = []
        for rid in rdids:
            if rid not in seen:
                seen.append(rid)
        for rid in seen:
            if rid in already_matched:
                continue
            # 该单的链接藏在 rdid_url_<rid> 隐藏字段 / 行内文本，做包含匹配
            block = _extract_rdid_block(html, rid)
            if any(u in block for u in want):
                already_matched.add(rid)
                log(f"  匹配到单号(rdid): {rid} (作品: {work_name})")
                return rid
    return ''


def _extract_rdid_block(html: str, rdid: str) -> str:
    """截取某个 rdid 附近的 HTML 文本（用于链接包含匹配）。"""
    idx = html.find(f'rdid="{rdid}"')
    if idx < 0:
        return ''
    return html[max(0, idx - 4000): idx + 1000]


def save_partial_result(task_id, result):
    """增量落盘进度（与其它平台一致），供 app.py 超时回收。"""
    try:
        result_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'task_results')
        os.makedirs(result_dir, exist_ok=True)
        with open(os.path.join(result_dir, f'{task_id}.json'), 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log(f"  ⚠️ 增量保存进度失败: {e}")


# 表单固定字段（机构代理场景），从 config['form'] 覆盖
FORM_FIXED = {
    'mtype': 3, 'is_agree': 1, 'complaint_type': 2, 'agent_type': 3, 'empower_type': 1,
    # 律所代理相关字段留空（v1 仅机构代理）
    'med_tel': '', 'front_pic': '', 'reverse_pic': '',
    'law_name': '', 'law_idnum': '', 'law_pic': '', 'lawyer_name': '', 'lawyer_tel': '',
    'lawyer_idnum': '', 'lawyercard_pic': '', 'empower_pic': '',
}


def build_form(config_form: dict, shared_pics: dict, work: dict, proof_picid: str) -> dict:
    """组装一批提交的表单字段。config_form 为 Sheet1 文本字段，
    shared_pics 为共享证件 picid，work 携带作品链接/原片链接。"""
    urls = '\n'.join(work['_chunk_links'])
    original = work.get('original_urls', [])
    if isinstance(original, list):
        original_urls = '\n'.join([u for u in original if u]) or ''
    else:
        original_urls = original or ''
    form = dict(FORM_FIXED)
    # Sheet1 文本字段
    for k in ('complaint_type', 'agent_type', 'rights_type', 'class_id', 'c_content',
              'empower_type', 'mtype', 'is_agree',
              'med_name', 'med_legname', 'med_idnum',
              'org_name', 'org_legname', 'org_idnum',
              'org_agt_name', 'org_agt_tel', 'org_agt_idnum',
              'dpt_reason', 'deal_req'):
        if k in config_form and config_form[k] != '':
            form[k] = config_form[k]
    # 共享证件 picid
    form['obusiness_pic'] = shared_pics.get('obusiness_pic', '')
    form['org_pic'] = shared_pics.get('org_pic', '')
    form['org_agt_pic1'] = shared_pics.get('org_agt_pic1', '')
    form['org_agt_pic2'] = shared_pics.get('org_agt_pic2', '')
    form['org_empower_pic'] = shared_pics.get('org_empower_pic', '')
    # 投诉内容
    form['films'] = _build_films_json(work['work_name'])
    form['urls'] = urls
    form['original_urls'] = original_urls
    form['pro_pics'] = proof_picid
    return form


def main():
    parser = argparse.ArgumentParser(description='微博版权投诉自动化脚本')
    parser.add_argument('--task-id', required=True)
    parser.add_argument('--cookie', required=True)
    parser.add_argument('--config-file', required=True,
                        help='JSON: {"form": {...共享字段...}, "works": [...]}')
    args = parser.parse_args()

    with open(args.config_file, encoding='utf-8') as f:
        config = json.load(f)
    cookie = args.cookie
    config_form = config.get('form', {})
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

    def rebuild_numbers():
        ordered, by_work = [], []
        for w in works_config:
            wn = w['work_name']
            if matched_by_work.get(wn):
                nums = [str(n) for n in matched_by_work[wn]]
                ordered.extend(nums)
            elif wn in failed_works:
                nums = [f"投诉失败:{wn}"]
                ordered.append(nums[0])
            else:
                nums = [f"未获取到单号:{wn}"]
                ordered.append(nums[0])
            by_work.append({'work_name': wn, 'numbers': nums,
                            'status': 'failed' if wn in failed_works
                            else ('completed' if matched_by_work.get(wn) else 'partial_failed')})
        result['feedback_numbers'] = ordered
        result['feedback_numbers_by_work'] = by_work

    try:
        log('开始执行微博投诉任务...')
        check_login(cookie)

        # 1) 上传共享证件（营业执照/机构执照/联系人身份证正反/授权委托书），一次复用
        log('上传共享证件材料...')
        shared_pics = {}
        try:
            shared_pics['obusiness_pic'] = upload_image(cookie, config_form.get('obusiness_path', ''), FTYPE_LICENSE)
            shared_pics['org_pic'] = upload_image(cookie, config_form.get('org_pic_path', ''), FTYPE_LICENSE)
            shared_pics['org_agt_pic1'] = upload_image(cookie, config_form.get('org_agt_pic1_path', ''), FTYPE_LICENSE)
            shared_pics['org_agt_pic2'] = upload_image(cookie, config_form.get('org_agt_pic2_path', ''), FTYPE_LICENSE)
            shared_pics['org_empower_pic'] = upload_image(cookie, config_form.get('org_empower_path', ''), FTYPE_EMPOWER)
        except Exception as e:
            raise RuntimeError(f'共享证件上传失败: {e}')

        # 2) 按作品循环
        for work_idx, work in enumerate(works_config):
            work_name = work['work_name']
            links = work.get('links', [])
            log(f"[{work_idx+1}/{len(works_config)}] 处理作品: {work_name} ({len(links)}条链接)")

            try:
                # 权属证明（每作品单独）
                proof_picid = upload_image(cookie, work.get('proof_path', ''), FTYPE_PROOF)
                if not proof_picid:
                    raise RuntimeError('缺少或上传失败：权属证明文件')

                result['works_detail'].append({
                    'work_index': work_idx, 'work_name': work_name, 'status': 'processing',
                })

                work_matched = []
                submitted_urls_all = []
                for chunk_start in range(0, len(links), MAX_LINKS_PER_SUBMISSION):
                    batch_no += 1
                    chunk = links[chunk_start:chunk_start + MAX_LINKS_PER_SUBMISSION]
                    submitted_urls_all.extend(chunk)
                    work['_chunk_links'] = chunk
                    log(f"  提交批次 {batch_no}: {len(chunk)}条链接 (行{chunk_start+1}-{chunk_start+len(chunk)})")

                    form = build_form(config_form, shared_pics, work, proof_picid)
                    outcome = submit_with_captcha_retry(cookie, form)

                    if outcome['ok']:
                        log(f"  批次 {batch_no} 提交成功(验证码尝试{outcome['attempts']}次)")
                        result['completed_batches'] += 1
                        result['batch_results'].append({
                            'batch_no': batch_no, 'work_name': work_name,
                            'status': 'completed', 'link_count': len(chunk),
                        })
                        # 回查该批 rdid
                        rid = match_rdid(cookie, work_name, chunk, already_matched)
                        if rid:
                            work_matched.append(rid)
                    else:
                        log(f"  批次 {batch_no} 失败: {outcome['msg']} (code={outcome['code']})")
                        result['failed_batches'] += 1
                        result['batch_results'].append({
                            'batch_no': batch_no, 'work_name': work_name,
                            'status': 'failed', 'error': outcome['msg'],
                        })
                    time.sleep(2)

                if work_matched:
                    matched_by_work[work_name] = work_matched
                if not work_matched and not any(
                        b['work_name'] == work_name and b['status'] == 'completed'
                        for b in result['batch_results']):
                    failed_works.add(work_name)

                rebuild_numbers()
                save_partial_result(task_id, result)

            except Exception as e:
                log(f"  ❌ 作品「{work_name}」处理异常，跳过: {e}")
                failed_works.add(work_name)
                # 该作品的批次全记失败
                for chunk_start in range(0, max(len(links), 1), MAX_LINKS_PER_SUBMISSION):
                    batch_no += 1
                    result['failed_batches'] += 1
                    result['batch_results'].append({
                        'batch_no': batch_no, 'work_name': work_name,
                        'status': 'failed', 'error': str(e),
                    })
                rebuild_numbers()
                save_partial_result(task_id, result)
                continue

        rebuild_numbers()
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
        log(f"任务异常终止: {e}")

    print('JSON_RESULT_START')
    print(json.dumps(result, ensure_ascii=False))
    print('JSON_RESULT_END')
    return 0 if result['status'] in ('completed', 'partial_failed') else 1


if __name__ == '__main__':
    sys.exit(main())
