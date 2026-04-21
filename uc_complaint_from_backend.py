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
    dialogs = page.locator(
        ".el-message-box:visible, .ant-modal-wrap:visible, .ant-modal:visible, [role='dialog']:visible")
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


def fill_initial_form(page, identity, agent, rights_holder, complaint_type, copyright_type,
                      module, content_type, description, proof_file, other_proof_files):
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

    # 选择代理人/权利人下拉框（id=obligee_id）
    print("👤 选择代理人/权利人...")
    agent_select = page.locator("#obligee_id .ant-select-selection")
    agent_select.wait_for(state="visible", timeout=10000)
    human_click(page, agent_select)
    human_delay(500, 800)
    # 等待下拉菜单出现，在dropdown内找选项
    page.wait_for_selector(".ant-select-dropdown", timeout=5000)
    agent_option = page.locator(".ant-select-dropdown").get_by_role("option", name=agent)
    if agent_option.count() == 0:
        raise RuntimeError(f"未找到代理人/权利人选项: {agent}")
    human_click(page, agent_option.first)
    human_delay(1000, 1500)

    # 选择被代理人（权利人）下拉框（id=proxy_id，仅代理人身份时显示）
    if identity == "代理人":
        print("👤 选择被代理人（权利人）信息...")
        principal_select = page.locator("#proxy_id .ant-select-selection")
        principal_select.wait_for(state="visible", timeout=10000)
        human_click(page, principal_select)
        human_delay(800, 1200)

        # 等待被代理人下拉菜单出现
        print("⏳ 等待被代理人下拉菜单出现...")
        dropdown = None
        for attempt in range(15):  # 最多等待15秒
            # 查找可见的下拉菜单
            dropdowns = page.locator(".ant-select-dropdown:visible")
            count = dropdowns.count()

            if count > 0:
                # 获取最后一个下拉菜单（新打开的一般在最后）
                dropdown = dropdowns.last
                # 检查下拉菜单是否包含选项
                options = dropdown.locator(".ant-select-dropdown-menu-item, [role='option']")
                if options.count() > 0:
                    print(f"✅ 下拉菜单已打开，包含 {options.count()} 个选项")
                    break

            human_delay(500, 800)
            if attempt == 14:
                # 打印调试信息
                print("⚠️ 未找到下拉菜单，当前页面上的下拉菜单:")
                all_dropdowns = page.locator(".ant-select-dropdown")
                for i in range(all_dropdowns.count()):
                    is_visible = all_dropdowns.nth(i).is_visible()
                    print(f"  下拉菜单 {i + 1}: visible={is_visible}")
                raise RuntimeError("等待被代理人下拉菜单超时")
        else:
            raise RuntimeError("等待被代理人下拉菜单超时")

        print("✅ 下拉菜单已打开")

        # 策略1：先尝试在搜索框输入关键词（如果有的话）
        search_input = dropdown.locator("input").first
        if search_input.count() > 0 and search_input.is_visible():
            print(f"🔍 在搜索框输入关键词: {rights_holder}")
            human_type(page, search_input, rights_holder)
            human_delay(1000, 1500)

        # 策略2：查找选项（支持分组结构）
        principal_option = None

        # 获取所有选项
        all_options = dropdown.locator(".ant-select-dropdown-menu-item, [role='option']")
        total_options = all_options.count()
        print(f"📋 下拉列表中共有 {total_options} 个选项")

        if total_options == 0:
            print("\n🔍 下拉菜单HTML结构（前500字符）:")
            html_preview = dropdown.inner_html()[:500]
            print(html_preview)
            raise RuntimeError(f"下拉列表为空，无法选择被代理人: {rights_holder}")

        # 打印所有选项并查找目标
        print("\n📋 下拉列表所有选项:")
        found = False
        for idx in range(total_options):
            option_text = all_options.nth(idx).text_content().strip()
            print(f"  {idx + 1}. {option_text}")
            if rights_holder in option_text or option_text == rights_holder:
                principal_option = all_options.nth(idx)
                found = True
                print(f"✅ 在第 {idx + 1} 个位置找到匹配: {option_text}")
                break

        # 如果没找到，尝试滚动查找（因为选项可能在不可见区域）
        if not found and total_options > 0:
            print(f"🔄 目标 '{rights_holder}' 不在当前可见选项中，尝试滚动查找...")

            # 获取下拉菜单内容容器
            dropdown_content = dropdown.locator(".ant-select-dropdown-content")
            if dropdown_content.count() == 0:
                dropdown_content = dropdown.locator(".rc-virtual-list")
            if dropdown_content.count() == 0:
                dropdown_content = dropdown

            # 滚动查找
            scroll_attempts = 0
            max_scrolls = 20

            for i in range(max_scrolls):
                # 滚动
                if dropdown_content.count() > 0:
                    current_scroll = dropdown_content.evaluate("el => el.scrollTop")
                    dropdown_content.evaluate(f"el => el.scrollTop = {current_scroll + 150}")
                else:
                    page.keyboard.press("ArrowDown")

                human_delay(300, 500)

                # 重新获取选项并检查
                current_options = dropdown.locator(".ant-select-dropdown-menu-item, [role='option']")
                for idx in range(current_options.count()):
                    option_text = current_options.nth(idx).text_content().strip()
                    if rights_holder in option_text or option_text == rights_holder:
                        principal_option = current_options.nth(idx)
                        found = True
                        print(f"✅ 滚动后在第 {idx + 1} 个位置找到: {option_text}")
                        break

                if found:
                    break

                scroll_attempts += 1

            if not found:
                # 最后尝试：归一化匹配
                normalized_target = rights_holder.replace(" ", "").strip()
                for idx in range(total_options):
                    option_text = all_options.nth(idx).text_content().strip()
                    normalized_option = option_text.replace(" ", "").strip()
                    if normalized_target == normalized_option or normalized_target in normalized_option:
                        principal_option = all_options.nth(idx)
                        found = True
                        print(f"✅ 通过归一化匹配找到: {option_text}")
                        break

        if not found or principal_option is None:
            raise RuntimeError(f"查找后仍未找到被代理人（权利人）选项: {rights_holder}")

        # 确保选项可见并点击
        principal_option.first.scroll_into_view_if_needed()
        human_delay(300, 500)
        print(f"✅ 准备点击被代理人: {rights_holder}")

        # 使用JavaScript点击以确保成功
        try:
            principal_option.first.evaluate("el => el.click()")
        except Exception:
            human_click(page, principal_option.first)

        human_delay(1500, 2000)
        print("✅ 被代理人选择完成")

    print("📌 选择投诉类型...")
    try:
        ip_radio = page.get_by_role("radio", name=complaint_type)
        if ip_radio.count() == 0:
            print(f"⚠️ 未找到投诉类型radio: {complaint_type}，尝试其他方式")
            ip_radio = page.locator(f"input[type='radio']").filter(has_text=complaint_type)
        if ip_radio.count() > 0:
            human_click(page, ip_radio.first)
        human_delay(500, 800)
        if copyright_type:
            copyright_cb = page.get_by_role("checkbox", name=copyright_type)
            if copyright_cb.count() > 0:
                human_click(page, copyright_cb.first)
        human_delay(1000, 1500)
        print("✅ 投诉类型选择完成")
    except Exception as e:
        print(f"❌ 选择投诉类型失败: {e}")
        raise

    print("📦 选择功能模块...")
    try:
        module_radio = page.get_by_role("radio", name=module)
        if module_radio.count() == 0:
            raise RuntimeError(f"未找到功能模块选项: {module}")
        human_click(page, module_radio.first)
        human_delay(1000, 1500)
        print("✅ 功能模块选择完成")
    except Exception as e:
        print(f"❌ 选择功能模块失败: {e}")
        raise

    print("🎬 选择内容类型...")
    try:
        content_radio = page.get_by_role("radio", name=content_type)
        if content_radio.count() == 0:
            raise RuntimeError(f"未找到内容类型选项: {content_type}")
        human_click(page, content_radio.first)
        human_delay(1000, 1500)
        print("✅ 内容类型选择完成")
    except Exception as e:
        print(f"❌ 选择内容类型失败: {e}")
        raise

    print("📝 填写投诉描述...")
    try:
        desc_textarea = page.get_by_role("textbox", name="请客观公正描述具体侵权所在，最多填写1000字")
        if desc_textarea.count() == 0:
            desc_textarea = page.locator("textarea").first
        if desc_textarea.count() == 0:
            raise RuntimeError("未找到投诉描述输入框")
        human_type(page, desc_textarea.first, description)
        human_delay(1000, 1500)
        print("✅ 投诉描述填写完成")
    except Exception as e:
        print(f"❌ 填写投诉描述失败: {e}")
        raise

    print("📤 上传证明文件...")
    if proof_file and os.path.exists(proof_file):
        # 定位到"证明文件："区域的上传框（第一个 upload-wrapper）
        proof_section = page.locator("#evidences").locator("h1:has-text('证明文件：')").first
        if proof_section.count() == 0:
            # 备用方案：直接找第一个 upload-wrapper
            proof_upload_wrapper = page.locator("#evidences .upload-wrapper").first
        else:
            # 找到证明文件区域下的 upload-wrapper
            proof_upload_wrapper = proof_section.locator("..").locator(".upload-wrapper").first

        if proof_upload_wrapper.count() == 0:
            proof_upload_wrapper = page.locator("#evidences .upload-wrapper").first

        file_input = proof_upload_wrapper.locator("input[type='file']")
        if file_input.count() == 0:
            raise RuntimeError("未找到证明文件上传框")

        file_input.set_input_files(proof_file)
        print(f"✅ 已上传证明文件: {os.path.basename(proof_file)}")
        human_delay(2000, 3000)

    print("📤 上传其他证明文件...")
    if other_proof_files:
        scroll_to_bottom(page)
        human_delay(1000, 1500)

        # 获取所有上传框
        all_upload_wrappers = page.locator("#evidences .upload-wrapper").all()

        # 第一个上传框是"证明文件"区域的，需要跳过
        # 从第二个开始才是"其他证明"区域的
        other_upload_wrappers = all_upload_wrappers[1:] if len(all_upload_wrappers) > 1 else []

        print(f"📊 找到 {len(other_upload_wrappers)} 个其他证明上传框，需要上传 {len(other_proof_files)} 个文件")

        # 如果上传框不够，需要点击添加按钮
        if len(other_upload_wrappers) < len(other_proof_files):
            add_button = page.locator("#evidences").get_by_text("添 加", exact=True)
            if add_button.count() == 0:
                add_button = page.locator("#evidences").get_by_text("添加", exact=True)

            needed_clicks = len(other_proof_files) - len(other_upload_wrappers)
            print(f"🖱️ 需要点击添加按钮 {needed_clicks} 次")

            for i in range(needed_clicks):
                try:
                    # 每次点击前重新获取按钮，避免 stale element
                    add_button = page.locator("#evidences").get_by_text("添 加", exact=True)
                    if add_button.count() == 0:
                        add_button = page.locator("#evidences").get_by_text("添加", exact=True)

                    if add_button.count() > 0:
                        add_button.first.click()
                        print(f"✅ 已点击添加按钮 ({i + 1}/{needed_clicks})")
                        human_delay(1500, 2000)
                    else:
                        print(f"⚠️ 添加按钮不存在，停止添加")
                        break
                except Exception as e:
                    print(f"⚠️ 点击添加按钮失败: {e}")
                    break

            # 重新获取上传框
            all_upload_wrappers = page.locator("#evidences .upload-wrapper").all()
            other_upload_wrappers = all_upload_wrappers[1:] if len(all_upload_wrappers) > 1 else []

        # 上传其他证明文件
        for idx, proof_path in enumerate(other_proof_files):
            if idx >= len(other_upload_wrappers):
                print(f"⚠️ 第 {idx + 1} 个文件没有对应的上传框，跳过")
                continue

            # 找到上传框内的 file input
            file_input = other_upload_wrappers[idx].locator("input[type='file']")
            if file_input.count() == 0:
                print(f"⚠️ 第 {idx + 1} 个上传框未找到文件输入框，跳过")
                continue

            if os.path.exists(proof_path):
                file_input.set_input_files(proof_path)
                print(f"✅ 已上传第 {idx + 1} 个文件: {os.path.basename(proof_path)}")
            else:
                print(f"⚠️ 文件不存在: {proof_path}")

            human_delay(1500, 2000)

        print("✅ 其他证明文件上传完成")


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
    other_proof_files = args.other_proof_files.split(',') if args.other_proof_files else []
    description = args.description
    identity = args.identity
    agent = args.agent
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
                page, identity, agent, rights_holder, complaint_type, copyright_type,
                module, content_type, description, proof_file, other_proof_files
            )

            for index, excel_file in enumerate(excel_files, start=1):
                result["current_batch"] = index
                print(f"\n===== 开始第 {index}/{len(excel_files)} 批 =====")
                upload_batch_excel(page, excel_file)
                # === 以下步骤暂时注释，待测试后再放开 ===
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
    parser.add_argument("--other-proof-files", type=str, help="其他证明文件，逗号分隔")
    parser.add_argument("--description", type=str, required=True, help="投诉描述")
    parser.add_argument("--identity", type=str, required=True, help="身份类型")
    parser.add_argument("--agent", type=str, required=True, help="代理人/权利人")
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