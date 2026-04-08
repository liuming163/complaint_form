#!/usr/bin/env python3
"""
UC侵权投诉平台自动化脚本 - 接收后端数据版本
"""

import argparse
import json
import os
import random
import re
import time
from pathlib import Path

from playwright.sync_api import sync_playwright


# ========== 模拟人类行为函数 ==========
def human_delay(min_ms=200, max_ms=800):
    time.sleep(random.uniform(min_ms / 1000, max_ms / 1000))


def natural_scroll(page, direction="down", distance=None):
    if distance is None:
        distance = random.randint(300, 800)
    if direction == "down":
        scrolled = 0
        while scrolled < distance:
            step = random.randint(40, 100)
            page.evaluate(f"window.scrollBy(0, {step})")
            scrolled += step
            time.sleep(random.uniform(0.1, 0.3))
    else:
        current_scroll = page.evaluate("window.pageYOffset")
        while current_scroll > 0:
            step = min(random.randint(50, 100), current_scroll)
            page.evaluate(f"window.scrollBy(0, -{step})")
            time.sleep(random.uniform(0.1, 0.25))
            current_scroll = page.evaluate("window.pageYOffset")


def scroll_to_bottom(page):
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    time.sleep(random.uniform(0.5, 1))


def human_click(page, element):
    try:
        box = element.bounding_box()
        if box:
            x = box['x'] + random.uniform(box['width'] * 0.2, box['width'] * 0.8)
            y = box['y'] + random.uniform(box['height'] * 0.3, box['height'] * 0.7)
            page.mouse.move(x, y)
            human_delay(100, 300)
            page.mouse.click(x, y)
        else:
            element.click()
        return True
    except Exception:
        element.click()
        return False


def human_type(page, element, text):
    element.click()
    human_delay(300, 600)
    for char in text:
        element.type(char, delay=random.uniform(50, 150))
    human_delay(200, 400)


# ========== 保存任务结果 ==========
def save_task_result(task_id, result):
    result_dir = Path("/Users/jan/Desktop/pj/complaint_form/task_results")
    result_dir.mkdir(parents=True, exist_ok=True)
    result_file = result_dir / f"{task_id}.json"

    with open(result_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"📁 任务结果已保存到: {result_file}")


def update_batch_result(result, batch_no, status, error=None):
    for batch in result["batches"]:
        if batch["batch_no"] == batch_no:
            batch["status"] = status
            batch["error"] = error
            break
    result["completed_batches"] = sum(1 for batch in result["batches"] if batch["status"] == "completed")
    result["failed_batches"] = sum(1 for batch in result["batches"] if batch["status"] == "failed")


def upload_batch_excel(page, excel_file):
    print(f"📎 导入批次文件: {excel_file}")
    batch_btn = page.get_by_text("批量导入", exact=True)
    if batch_btn.count() == 0:
        batch_btn = page.get_by_role("button", name="批量导入")
    if batch_btn.count() == 0:
        raise RuntimeError("未找到批量导入按钮")

    human_click(page, batch_btn.first)
    print("✅ 已点击批量导入，等待弹窗...")
    human_delay(1500, 2500)

    dialog = page.locator(".el-dialog, .ant-modal, [role='dialog']").first
    dialog.wait_for(state="visible", timeout=10000)
    print("✅ 弹窗已打开")

    upload_btn = dialog.get_by_role("button", name="上传文件")
    if upload_btn.count() == 0:
        upload_btn = dialog.get_by_text("上传文件", exact=True)
    if upload_btn.count() > 0:
        human_click(page, upload_btn.first)
        human_delay(500, 1000)

    file_input = dialog.locator("input[type='file'][accept*='.xlsx']")
    if file_input.count() == 0:
        file_input = dialog.locator("input[type='file']").last
    if file_input.count() == 0:
        raise RuntimeError("在弹窗中未找到文件上传框")

    file_input.set_input_files(excel_file)
    print(f"✅ 已上传文件: {excel_file}")
    human_delay(2000, 3000)

    parse_btn = dialog.get_by_role("button", name="解析数据")
    if parse_btn.count() == 0:
        parse_btn = dialog.get_by_text("解析数据", exact=True)
    if parse_btn.count() == 0:
        raise RuntimeError("未找到解析数据按钮")

    human_click(page, parse_btn.first)
    print("✅ 已点击解析数据")
    human_delay(3000, 5000)


def submit_form(page):
    print("📨 提交投诉...")
    scroll_to_bottom(page)
    human_delay(1000, 1500)

    submit_btn = page.get_by_role("button", name="提 交")
    if submit_btn.count() == 0:
        submit_btn = page.get_by_role("button", name="提交")
    if submit_btn.count() == 0:
        raise RuntimeError("未找到提交按钮")

    human_click(page, submit_btn.first)
    print("✅ 已点击提交")
    human_delay(3000, 5000)


def get_success_dialog(page):
    dialogs = page.locator(".el-message-box:visible, .ant-modal-wrap:visible, .ant-modal:visible, [role='dialog']:visible")
    dialogs.first.wait_for(state="visible", timeout=15000)
    return dialogs.first


def click_continue_in_success_dialog(page):
    dialog = get_success_dialog(page)
    continue_btn = dialog.get_by_role("button", name=re.compile(r"继\s*续"))
    if continue_btn.count() == 0:
        continue_btn = dialog.get_by_text(re.compile(r"继\s*续"))
    if continue_btn.count() == 0:
        raise RuntimeError("未找到继续按钮")

    human_click(page, continue_btn.first)
    print("✅ 已点击继续")
    human_delay(2000, 3000)


def click_list_in_success_dialog(page):
    dialog = get_success_dialog(page)
    list_btn = dialog.get_by_role("button", name="投诉列表")
    if list_btn.count() == 0:
        list_btn = dialog.get_by_text("投诉列表", exact=True)
    if list_btn.count() == 0:
        raise RuntimeError("未找到投诉列表按钮")

    human_click(page, list_btn.first)
    print("✅ 已点击投诉列表")
    human_delay(2000, 3000)


def read_latest_complaint_numbers(page, count):
    print(f"🔢 读取最新 {count} 个投诉单号...")
    human_delay(1500, 2500)
    rows = page.locator("table tbody tr")
    total_rows = rows.count()
    complaint_numbers = []

    for index in range(min(count, total_rows)):
        row = rows.nth(index)
        cells = row.locator("td")
        if cells.count() > 1:
            complaint_number = cells.nth(1).text_content().strip()
            if complaint_number:
                complaint_numbers.append(complaint_number)

    print(f"✅ 已读取投诉单号: {complaint_numbers}")
    return complaint_numbers


def fill_initial_form(page, identity, rights_holder, complaint_type, copyright_type,
                      module, content_type, description, proof_file, proxy_file, other_proof_files):
    print("📝 开始填写投诉表单...")

    print("👤 选择身份信息...")
    if identity == "权利人":
        identity_radio = page.get_by_role("radio", name="权利人")
    else:
        identity_radio = page.get_by_role("radio", name="代理人")
    if identity_radio.count() == 0:
        raise RuntimeError("未找到身份选项")
    human_click(page, identity_radio.first)
    human_delay(1000, 1500)

    print("👤 选择权利人...")
    combobox = page.get_by_role("combobox").first
    combobox.wait_for(state="visible", timeout=10000)
    human_click(page, combobox)
    human_delay(500, 800)
    option = page.get_by_role("option", name=rights_holder)
    if option.count() == 0:
        raise RuntimeError(f"未找到权利人选项: {rights_holder}")
    human_click(page, option.first)
    human_delay(1000, 1500)

    print("📌 选择投诉类型...")
    ip_radio = page.get_by_role("radio", name=complaint_type)
    if ip_radio.count() > 0:
        human_click(page, ip_radio.first)
        human_delay(500, 800)
    if copyright_type:
        copyright_cb = page.get_by_role("checkbox", name=copyright_type)
        if copyright_cb.count() > 0:
            human_click(page, copyright_cb.first)
    human_delay(1000, 1500)

    print("📦 选择功能模块...")
    module_radio = page.get_by_role("radio", name=module)
    if module_radio.count() == 0:
        raise RuntimeError(f"未找到功能模块选项: {module}")
    human_click(page, module_radio.first)
    human_delay(1000, 1500)

    print("🎬 选择内容类型...")
    content_radio = page.get_by_role("radio", name=content_type)
    if content_radio.count() == 0:
        raise RuntimeError(f"未找到内容类型选项: {content_type}")
    human_click(page, content_radio.first)
    human_delay(1000, 1500)

    print("📝 填写投诉描述...")
    desc_textarea = page.get_by_role("textbox", name="请客观公正描述具体侵权所在，最多填写1000字")
    if desc_textarea.count() == 0:
        desc_textarea = page.locator("textarea").first
    if desc_textarea.count() == 0:
        raise RuntimeError("未找到投诉描述输入框")
    human_type(page, desc_textarea.first, description)
    human_delay(1000, 1500)

    print("📤 上传证明文件...")
    if proof_file and os.path.exists(proof_file):
        proof_upload = None
        proof_label = page.get_by_text("证明文件", exact=True).first
        if proof_label.count() > 0:
            proof_block = proof_label.locator("xpath=ancestor::div[contains(@class, 'mb-3')][1]").first
            scoped_upload = proof_block.locator("input[type='file']")
            if scoped_upload.count() > 0:
                proof_upload = scoped_upload.first
        if proof_upload is None:
            proof_upload = page.locator("input[type='file']:not([multiple])").first
        if proof_upload.count() == 0:
            raise RuntimeError("未找到证明文件上传框")
        proof_upload.set_input_files(proof_file)
        human_delay(2000, 3000)

    print("📤 上传委托代理文件...")
    if identity == "代理人":
        if not proxy_file or not os.path.exists(proxy_file):
            raise RuntimeError("缺少委托代理文件")
        proxy_upload = None
        proxy_label = page.get_by_text("委托代理文件", exact=True).first
        if proxy_label.count() == 0:
            proxy_label = page.get_by_text("委托代理", exact=False).first
        if proxy_label.count() > 0:
            proxy_block = proxy_label.locator("xpath=ancestor::div[contains(@class, 'mb-3')][1]").first
            scoped_upload = proxy_block.locator("input[type='file']")
            if scoped_upload.count() > 0:
                proxy_upload = scoped_upload.first
        if proxy_upload is None:
            file_inputs = page.locator("input[type='file']:not([multiple])")
            if file_inputs.count() > 1:
                proxy_upload = file_inputs.nth(1)
        if proxy_upload is None or proxy_upload.count() == 0:
            raise RuntimeError("未找到委托代理文件上传框")
        proxy_upload.set_input_files(proxy_file)
        human_delay(2000, 3000)

    print("📤 上传其他证明文件...")
    if other_proof_files:
        scroll_to_bottom(page)
        human_delay(1000, 1500)
        other_proof_title = page.locator("text=其他证明：").first
        if other_proof_title.count() == 0:
            other_proof_title = page.get_by_text("其他证明", exact=True).first
        if other_proof_title.count() == 0:
            raise RuntimeError("未找到其他证明区域")

        add_button = page.get_by_text("添 加", exact=True).first
        if add_button.count() == 0:
            add_button = page.get_by_text("添加", exact=True).first
        if add_button.count() == 0:
            add_button = page.locator("button.add.ant-btn-default").first

        add_clicks = max(len(other_proof_files) - 1, 0)
        for _ in range(add_clicks):
            add_button.scroll_into_view_if_needed()
            human_delay(300, 500)
            add_button.click()
            human_delay(2000, 2500)

        other_proof_container = other_proof_title.locator("xpath=..").first
        upload_wrappers = other_proof_container.locator(".upload-wrapper").all()
        for idx, proof_path in enumerate(other_proof_files):
            if idx >= len(upload_wrappers):
                raise RuntimeError(f"其他证明上传框不足，第 {idx + 1} 个文件无法上传")
            wrapper = upload_wrappers[idx]
            file_input = wrapper.locator("input[type='file']")
            if file_input.count() == 0:
                raise RuntimeError(f"第 {idx + 1} 个其他证明未找到文件输入框")
            file_input.set_input_files(proof_path)
            human_delay(2000, 2500)


def open_complaint_form(page):
    print("📂 打开UC侵权投诉平台...")
    page.goto("https://ipp.uc.cn/#/home", wait_until="load")
    human_delay(2000, 3000)

    print("🔐 检查登录状态...")
    login_dialog = page.locator("text=UC账号登录").first
    if login_dialog.count() > 0 and login_dialog.is_visible():
        raise RuntimeError("Cookie无效，请重新登录")

    natural_scroll(page, "down", 300)
    human_delay(500, 800)
    natural_scroll(page, "up", 200)
    scroll_to_bottom(page)
    human_delay(1000, 1500)

    btn = page.get_by_text("发起侵权投诉", exact=True)
    if btn.count() == 0:
        btn = page.locator("button:has-text('发起侵权投诉')")
    if btn.count() == 0:
        btn = page.get_by_role("button", name="发起侵权投诉")
    if btn.count() == 0:
        raise RuntimeError("未找到发起侵权投诉按钮")

    btn.first.scroll_into_view_if_needed()
    human_delay(300, 600)
    human_click(page, btn.first)
    human_delay(2000, 3000)


# ========== 主流程 ==========
def main(args):
    task_id = args.task_id or f"uc_{int(time.time())}"
    cookie = args.cookie
    proof_file = args.proof_file
    proxy_file = args.proxy_file
    other_proof_files = args.other_proof_files.split(',') if args.other_proof_files else []
    description = args.description
    identity = args.identity
    rights_holder = args.rights_holder
    complaint_type = args.complaint_type
    copyright_type = args.copyright_type
    module = args.module
    content_type = args.content_type
    excel_files = json.loads(args.excel_files) if args.excel_files else []
    batch_metadata = json.loads(args.batch_metadata) if args.batch_metadata else []

    result = {
        "task_id": task_id,
        "status": "running",
        "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "completed_at": None,
        "complaint_number": None,
        "complaint_numbers": [],
        "total_batches": len(excel_files),
        "completed_batches": 0,
        "failed_batches": 0,
        "current_batch": 0,
        "batches": [
            {
                "batch_no": batch.get("batch_no", index + 1),
                "rows": batch.get("rows"),
                "start_row": batch.get("start_row"),
                "end_row": batch.get("end_row"),
                "excel_file": excel_files[index] if index < len(excel_files) else batch.get("filename"),
                "status": "pending",
                "error": None,
            }
            for index, batch in enumerate(batch_metadata)
        ],
        "error": None,
    }

    if not result["batches"]:
        result["batches"] = [
            {
                "batch_no": index + 1,
                "rows": None,
                "start_row": None,
                "end_row": None,
                "excel_file": excel_file,
                "status": "pending",
                "error": None,
            }
            for index, excel_file in enumerate(excel_files)
        ]

    if not cookie:
        result["status"] = "failed"
        result["error"] = "缺少Cookie"
        return result
    if not description:
        result["status"] = "failed"
        result["error"] = "缺少投诉描述"
        return result
    if not identity:
        result["status"] = "failed"
        result["error"] = "缺少身份类型"
        return result
    if not rights_holder:
        result["status"] = "failed"
        result["error"] = "缺少权利人信息"
        return result
    if not module:
        result["status"] = "failed"
        result["error"] = "缺少功能模块"
        return result
    if not content_type:
        result["status"] = "failed"
        result["error"] = "缺少内容类型"
        return result
    if not excel_files:
        result["status"] = "failed"
        result["error"] = "缺少批次Excel文件"
        return result

    print(f"🚀 开始执行UC投诉任务: {task_id}")
    print(f"📦 批次数量: {len(excel_files)}")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--lang=zh-CN,en-US",
            ],
        )
        context = browser.new_context(
            user_agent=random.choice([
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            ]),
            viewport={"width": 1920, "height": 1080},
        )
        context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            window.chrome = { runtime: {} };
            delete navigator.__proto__.webdriver;
        """)

        page = context.new_page()

        try:
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

            open_complaint_form(page)
            fill_initial_form(
                page, identity, rights_holder, complaint_type, copyright_type,
                module, content_type, description, proof_file, proxy_file, other_proof_files
            )

            for index, excel_file in enumerate(excel_files, start=1):
                result["current_batch"] = index
                print(f"\n===== 开始第 {index}/{len(excel_files)} 批 =====")
                upload_batch_excel(page, excel_file)
                submit_form(page)

                if index < len(excel_files):
                    click_continue_in_success_dialog(page)
                    update_batch_result(result, index, "completed")
                    human_delay(1500, 2500)
                else:
                    click_list_in_success_dialog(page)
                    update_batch_result(result, index, "completed")

            complaint_numbers = read_latest_complaint_numbers(page, len(excel_files))
            result["complaint_numbers"] = complaint_numbers
            result["complaint_number"] = complaint_numbers[0] if complaint_numbers else None
            result["status"] = "completed"
            if len(complaint_numbers) != len(excel_files):
                result["error"] = f"投诉已提交，但仅获取到 {len(complaint_numbers)} 个投诉单号"

        except Exception as e:
            batch_no = result.get("current_batch") or 1
            update_batch_result(result, batch_no, "failed", str(e))
            result["status"] = "partial_failed" if result["completed_batches"] > 0 else "failed"
            result["error"] = str(e)
            print(f"❌ 执行失败: {e}")
        finally:
            result["completed_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
            save_task_result(task_id, result)
            context.close()

    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="UC投诉自动化脚本（后端数据版）")
    parser.add_argument("--task-id", type=str, help="任务ID")
    parser.add_argument("--cookie", type=str, required=True, help="Cookie字符串")
    parser.add_argument("--excel-files", type=str, help="Excel文件路径列表JSON")
    parser.add_argument("--batch-metadata", type=str, help="批次元数据JSON")
    parser.add_argument("--proof-file", type=str, help="证明文件路径")
    parser.add_argument("--proxy-file", type=str, help="委托代理文件路径")
    parser.add_argument("--other-proof-files", type=str, help="其他证明文件，逗号分隔")
    parser.add_argument("--description", type=str, required=True, help="投诉描述")
    parser.add_argument("--identity", type=str, required=True, help="身份类型")
    parser.add_argument("--rights-holder", type=str, required=True, help="权利人名称")
    parser.add_argument("--complaint-type", type=str, help="投诉类型")
    parser.add_argument("--copyright-type", type=str, help="著作权类型")
    parser.add_argument("--module", type=str, required=True, help="功能模块")
    parser.add_argument("--content-type", type=str, required=True, help="内容类型")

    args = parser.parse_args()
    result = main(args)

    print("\n" + "=" * 50)
    print("JSON_RESULT_START")
    print(json.dumps(result, ensure_ascii=False))
    print("JSON_RESULT_END")
