#!/usr/bin/env python3
import os
import re
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

COOKIE = "cna=cYbqIAD9IRICAd9IJtkd2tCa; isg=BPr6EVNHR0NI7cp6rBImcihOSyYcq36Ftrr46wTzFw1Y95ox7D3-lcFEQ4Mr5_Yd; __itrace_wid=bdf05497-d6ab-45bd-985b-905cca560d42; _UP_A4A_11_=wba2b18a28c64601aa3cb1a24b5658ae; tfstk=gx9IEbspjUpNeTmKyyoafOpHYyXS0ck2pusJm3eU29BdF3LF7w7rx3R_eUQwL_WK-T_RWUVULaWeFFTMSe-e4LOgFnxkLDXrx9xhET3quxu2xHXl6026Up6OXitOvJB-_OFn3Eqnuxk2vq5Iab32TdR1ENsReMC89On1DNIR2UQd6PIfmTQJyUET6gI0JJQdy5n1qNQReTLJXcsPWaBReUKtwcVcVv_BA0LMzCN7mj-F58eJCG3lLH9NjMv1A6_A6NwRAdsCOZKpHHJvKgOvh6Yx7W7pGLLf0ekTdEtJmQC6FP31EhRXPi9KDS_W-hpF6pnziGXkwpW9pVHAs_dWyGAiP7_XwHRDQ1r866dD8IC6Q2wGkCK641OiqW7yigLliLu0KwKJmKAVhvacACKfCgS33Z_EV7Z1i8I1uci_Z7cs7yY54J0nv6IGAGnsfyTl9Gj6Vci_Z7fdjGzjfcaBr; _c_WBKFRo=pe4tVbAuU5rmCPRZhi1zdnOJUV7lAxBimTfT2jN3; _nb_ioWEgULi=; _UP_28A_52_=594; _UP_D_=pc; cmptstk=Suz_jwKMgNIWRdv_CLWNUUsj; EGG_SESS=oXseTBTtPwv75To_tpZhqcrhhow6L20q73JYen7GyrkmUZ2dLKraNp0JJS4Z_1-ZD1oOZc8td-trldfKIKoeCnwof7KKWCcDJ7y1uctnAeinK-eixQms9BRnyg_4UeMw3fKmjoTefcBZ3zbMvx1ubOAjonDBKGxC9eMqIgNt9To89Eb_Xqm56wkHpHiI_TevfVLxGhoVlXmiGvY-XJxoWuVdZFIi8vMauv_SSJYLjDGq8mqsPUVKjdJ0ubmFuGNU62LNBHj23f0sVsIKPwvRTuZbtLhEKsc-pF02LTl_9NbXQ_2Eovb_DHJw88nrajP758MqFFTf0Jd-tOVbrfFNjnQ3OEiUP0aGEl108PlXj_4pOLmGn8lSN3VST-oUhR7QO6Mq5ru3EcOVA0jidfiT0IrgIsRYhs86W-N4NenLS0ZrIo3GwPVgD67S4O3QCWWqj0JCVe986XJGCWiMMpsNg5JF6k0Kx8QT6vzvFlqupV8="
TARGET_FILE = "/opt/complaint_form/static/imgs/剧名/大周仙吏（怎么都要和我双修）_和晞科技_漫画_著作权/证明文件_大周仙吏（怎么都要和我双修）_-.jpg"
LABEL = "server"  # local / server
MODE = "proof"  # proof / other


def launch_browser(p):
    chromium_path = os.getenv('PLAYWRIGHT_CHROMIUM_PATH', '').strip()
    headless = os.getenv('PLAYWRIGHT_HEADLESS', '1').strip() != '0'
    launch_kwargs = {
        'headless': headless,
        'args': [
            '--disable-blink-features=AutomationControlled',
            '--no-sandbox',
            '--disable-setuid-sandbox',
            '--lang=zh-CN,en-US',
        ],
    }
    if chromium_path:
        launch_kwargs['executable_path'] = chromium_path
    return p.chromium.launch(**launch_kwargs)


def cookie_to_context(context, cookie_value):
    if cookie_value.startswith('[') or cookie_value.startswith('{'):
        import json
        cookies = json.loads(cookie_value) if isinstance(cookie_value, str) else cookie_value
        context.add_cookies(cookies)
        return

    for pair in cookie_value.split(';'):
        pair = pair.strip()
        if '=' in pair:
            key, value = pair.split('=', 1)
            context.add_cookies([{
                'name': key,
                'value': value,
                'domain': '.uc.cn',
                'path': '/'
            }])


def find_upload_wrapper(page, mode):
    evidences = page.locator('#evidences')
    if mode == 'proof':
        proof_section = evidences.locator("h1:has-text('证明文件：')").first
        if proof_section.count() == 0:
            return evidences.locator('.upload-wrapper').first
        return proof_section.locator('..').locator('.upload-wrapper').first

    wrappers = evidences.locator('.upload-wrapper')
    return wrappers.nth(1) if wrappers.count() > 1 else wrappers.first


def main():
    out_dir = Path(__file__).resolve().parent / 'task_results'
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = time.strftime('%Y%m%d_%H%M%S')
    log_path = out_dir / f'uc_compare_{MODE}_{LABEL}_{ts}.txt'

    lines = []
    def log(msg):
        print(msg)
        lines.append(msg)

    with sync_playwright() as p:
        browser = launch_browser(p)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport={'width': 1920, 'height': 1080},
        )
        cookie_to_context(context, COOKIE)
        page = context.new_page()
        page.goto('https://ipp.uc.cn/#/home', wait_until='load', timeout=30000)
        page.wait_for_timeout(2000)
        log(f'URL: {page.url}')
        log(f'TITLE: {page.title()}')
        log(f'LOGIN_DIALOG_VISIBLE: {page.locator("text=UC账号登录").first.is_visible() if page.locator("text=UC账号登录").count() else False}')

        page.goto('https://ipp.uc.cn/#/home', wait_until='load', timeout=30000)
        page.wait_for_timeout(2000)
        page.locator('text=发起侵权投诉').first.scroll_into_view_if_needed()
        page.get_by_text('发起侵权投诉', exact=True).first.click()
        page.wait_for_timeout(2000)

        wrapper = find_upload_wrapper(page, MODE)
        file_input = wrapper.locator("input[type='file']")
        log(f'TARGET_MODE: {MODE}')
        log(f'UPLOAD_WRAPPER_COUNT: {wrapper.count()}')
        log(f'FILE_INPUT_COUNT: {file_input.count()}')
        file_input.set_input_files(TARGET_FILE)
        page.wait_for_timeout(2000)
        wrapper_html = wrapper.evaluate('el => el.outerHTML')
        wrapper_html = re.sub(r'\s+', ' ', wrapper_html)
        log(f'WRAPPER_HTML: {wrapper_html}')
        log(f'WRAPPER_TEXT: {wrapper.inner_text()}')
        shot = out_dir / f"uc_compare_{MODE}_{LABEL}_{ts}.png"
        wrapper.screenshot(path=str(shot))
        log(f'SHOT: {shot}')
        browser.close()

    log_path.write_text('\n'.join(lines) + '\n', encoding='utf-8')
    print(f'LOG_PATH: {log_path}')


if __name__ == '__main__':
    main()
