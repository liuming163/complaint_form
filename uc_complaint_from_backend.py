#!/usr/bin/env python3
"""
UC侵权投诉平台自动化脚本 - 接收后端数据版本
用法: python3 uc_complaint_from_backend.py --cookie "xxx" --links "url1,url2" --proof-file "path" [--output-task-id "uc_xxxxx"]
"""

import argparse
import json
import os
import random
import re
import sys
import time
from pathlib import Path

import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


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
    except:
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
    """保存任务结果到 JSON 文件"""
    result_dir = Path("/Users/jan/Desktop/pj/complaint_form/task_results")
    result_dir.mkdir(parents=True, exist_ok=True)
    result_file = result_dir / f"{task_id}.json"

    with open(result_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"📁 任务结果已保存到: {result_file}")


# ========== 创建临时Excel文件 ==========
def create_temp_excel(links, output_path):
    """根据链接列表创建临时Excel文件"""
    df = pd.DataFrame({'链接': links})
    df.to_excel(output_path, index=False)
    print(f"📄 已创建临时Excel文件: {output_path}")
    return output_path


# ========== 主流程 ==========
def main(args):
    """
    执行投诉流程
    """
    task_id = args.task_id or f"uc_{int(time.time())}"
    cookie = args.cookie
    links = args.links.split(',') if args.links else []
    proof_file = args.proof_file
    other_proof_files = args.other_proof_files.split(',') if args.other_proof_files else []
    description = args.description
    identity = args.identity
    rights_holder = args.rights_holder
    complaint_type = args.complaint_type
    copyright_type = args.copyright_type
    module = args.module
    content_type = args.content_type

    # 初始化结果
    result = {
        "task_id": task_id,
        "status": "running",
        "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "complaint_number": None,
        "error": None
    }

    # 必填参数校验
    if not cookie:
        result["status"] = "failed"
        result["error"] = "缺少Cookie"
        print("❌ 缺少Cookie")
        print("\n" + "=" * 50)
        print("JSON_RESULT_START")
        print(json.dumps(result, ensure_ascii=False))
        print("JSON_RESULT_END")
        return result

    if not description:
        result["status"] = "failed"
        result["error"] = "缺少投诉描述"
        print("❌ 缺少投诉描述")
        print("\n" + "=" * 50)
        print("JSON_RESULT_START")
        print(json.dumps(result, ensure_ascii=False))
        print("JSON_RESULT_END")
        return result

    if not identity:
        result["status"] = "failed"
        result["error"] = "缺少身份类型"
        print("❌ 缺少身份类型")
        print("\n" + "=" * 50)
        print("JSON_RESULT_START")
        print(json.dumps(result, ensure_ascii=False))
        print("JSON_RESULT_END")
        return result

    if not rights_holder:
        result["status"] = "failed"
        result["error"] = "缺少权利人信息"
        print("❌ 缺少权利人信息")
        print("\n" + "=" * 50)
        print("JSON_RESULT_START")
        print(json.dumps(result, ensure_ascii=False))
        print("JSON_RESULT_END")
        return result

    if not module:
        result["status"] = "failed"
        result["error"] = "缺少功能模块"
        print("❌ 缺少功能模块")
        print("\n" + "=" * 50)
        print("JSON_RESULT_START")
        print(json.dumps(result, ensure_ascii=False))
        print("JSON_RESULT_END")
        return result

    if not content_type:
        result["status"] = "failed"
        result["error"] = "缺少内容类型"
        print("❌ 缺少内容类型")
        print("\n" + "=" * 50)
        print("JSON_RESULT_START")
        print(json.dumps(result, ensure_ascii=False))
        print("JSON_RESULT_END")
        return result

    print(f"🚀 开始执行UC投诉任务: {task_id}")
    print(f"📋 链接数量: {len(links)}")

    # 创建临时Excel文件
    temp_excel = None
    if links:
        temp_excel = f"/tmp/{task_id}_links.xlsx"
        create_temp_excel(links, temp_excel)
    else:
        print("⚠️ 未提供链接，将跳过批量导入")
        temp_excel = args.excel_file  # 备用：使用提供的Excel文件

    with sync_playwright() as p:
        user_agents = [
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        ]

        # 使用临时上下文（不保存浏览器数据）
        browser = p.chromium.launch(
            headless=False,  # 本地调试时可设为 True
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--lang=zh-CN,en-US",
            ],
        )
        context = browser.new_context(
            user_agent=random.choice(user_agents),
            viewport={"width": 1920, "height": 1080},
        )

        # 添加反检测脚本
        context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            window.chrome = { runtime: {} };
            delete navigator.__proto__.webdriver;
        """)

        page = context.new_page()

        # 设置 Cookie
        print("🔐 设置Cookie...")
        try:
            # 解析 cookie 字符串
            if cookie:
                # cookie 格式可能是 "key1=value1; key2=value2" 或 JSON 格式
                if cookie.startswith('[') or cookie.startswith('{'):
                    # JSON 格式
                    cookies = json.loads(cookie) if isinstance(cookie, str) else cookie
                    context.add_cookies(cookies)
                else:
                    # key=value 格式
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
                print("✅ Cookie已设置")
            else:
                print("⚠️ 未提供Cookie，请确保浏览器已手动登录")
        except Exception as e:
            print(f"⚠️ 设置Cookie失败: {e}")
            result["error"] = f"设置Cookie失败: {str(e)}"
            save_task_result(task_id, result)
            context.close()
            return result

        print("📂 打开UC侵权投诉平台...")
        page.goto("https://ipp.uc.cn/#/home", wait_until="load")
        human_delay(2000, 3000)

        # 检查登录状态
        print("\n🔐 检查登录状态...")
        login_dialog = page.locator("text=UC账号登录").first
        if login_dialog.count() > 0 and login_dialog.is_visible():
            print("⚠️ Cookie无效或已过期，仍显示登录弹窗")
            result["status"] = "failed"
            result["error"] = "Cookie无效，请重新登录"
            save_task_result(task_id, result)
            context.close()
            return result
        else:
            print("✅ 登录状态正常")

        human_delay(1000, 2000)

        natural_scroll(page, "down", 300)
        human_delay(500, 800)
        natural_scroll(page, "up", 200)

        print("📜 滚动到页面底部...")
        scroll_to_bottom(page)
        human_delay(1000, 1500)

        print("🔍 查找并点击'发起侵权投诉'...")
        try:
            btn = page.get_by_text("发起侵权投诉", exact=True)
            if btn.count() == 0:
                btn = page.locator("button:has-text('发起侵权投诉')")
            if btn.count() == 0:
                btn = page.get_by_role("button", name="发起侵权投诉")

            btn.first.scroll_into_view_if_needed()
            human_delay(300, 600)
            human_click(page, btn.first)
            print("✅ 已点击发起侵权投诉")
        except Exception as e:
            print(f"⚠️ 自动点击失败: {e}")
            result["status"] = "failed"
            result["error"] = f"点击发起投诉失败: {str(e)}"
            save_task_result(task_id, result)
            context.close()
            return result

        human_delay(2000, 3000)

        print("\n📝 开始填写投诉表单...")

        # 1. 选择权利人身份
        print("👤 选择身份信息...")
        try:
            if identity == "权利人":
                identity_radio = page.get_by_role("radio", name="权利人")
            else:
                identity_radio = page.get_by_role("radio", name="代理人")

            if identity_radio.count() > 0:
                human_click(page, identity_radio.first)
                print(f"✅ 已选择：{identity}")
            else:
                print("⚠️ 未找到权利人选项")
        except Exception as e:
            print(f"⚠️ 选择身份失败: {e}")

        human_delay(1000, 1500)

        # 2. 选择权利人
        print("👤 选择权利人...")
        try:
            combobox = page.get_by_role("combobox").first
            combobox.wait_for(state="visible", timeout=10000)
            human_click(page, combobox)
            human_delay(500, 800)
            option = page.get_by_role("option", name=rights_holder)
            if option.count() > 0:
                human_click(page, option.first)
                print(f"✅ 已选择权利人：{rights_holder}")
            else:
                print(f"⚠️ 未找到 {rights_holder} 选项")
        except Exception as e:
            print(f"⚠️ 选择失败: {e}")

        human_delay(1000, 1500)

        # 3. 投诉类型
        print("📌 选择投诉类型...")
        try:
            ip_radio = page.get_by_role("radio", name=complaint_type)
            if ip_radio.count() > 0:
                human_click(page, ip_radio.first)
                print(f"✅ 已选择：{complaint_type}")
                human_delay(500, 800)

            copyright_cb = page.get_by_role("checkbox", name=copyright_type)
            if copyright_cb.count() > 0:
                human_click(page, copyright_cb.first)
                print(f"✅ 已选择：{copyright_type}")
        except Exception as e:
            print(f"⚠️ 选择投诉类型失败: {e}")

        human_delay(1000, 1500)

        # 4. 功能模块
        print("📦 选择功能模块...")
        try:
            module_radio = page.get_by_role("radio", name=module)
            if module_radio.count() > 0:
                human_click(page, module_radio.first)
                print(f"✅ 已选择：{module}")
            else:
                print(f"⚠️ 未找到 {module} 选项")
        except Exception as e:
            print(f"⚠️ 选择功能模块失败: {e}")

        human_delay(1000, 1500)

        # 5. 内容类型
        print("🎬 选择内容类型...")
        try:
            content_radio = page.get_by_role("radio", name=content_type)
            if content_radio.count() > 0:
                human_click(page, content_radio.first)
                print(f"✅ 已选择：{content_type}")
            else:
                print(f"⚠️ 未找到 {content_type} 选项")
        except Exception as e:
            print(f"⚠️ 选择内容类型失败: {e}")

        human_delay(1000, 1500)

        # 6. 批量导入（弹窗操作）
        print("📎 批量导入链接...")
        if temp_excel and os.path.exists(temp_excel):
            try:
                batch_btn = page.get_by_text("批量导入", exact=True)
                if batch_btn.count() == 0:
                    batch_btn = page.get_by_role("button", name="批量导入")
                if batch_btn.count() > 0:
                    human_click(page, batch_btn.first)
                    print("✅ 已点击批量导入，等待弹窗...")
                    human_delay(1500, 2500)

                    # 等待弹窗出现
                    dialog = page.locator(".el-dialog, .ant-modal, [role='dialog']").first
                    dialog.wait_for(state="visible", timeout=10000)
                    print("✅ 弹窗已打开")

                    # 点击上传文件按钮
                    print("📂 点击上传文件按钮...")
                    upload_btn = dialog.get_by_role("button", name="上传文件")
                    if upload_btn.count() == 0:
                        upload_btn = dialog.get_by_text("上传文件", exact=True)

                    if upload_btn.count() > 0:
                        human_click(page, upload_btn.first)
                        print("✅ 已点击上传文件按钮")
                        human_delay(500, 1000)

                    # 查找文件输入框
                    file_input_in_dialog = dialog.locator("input[type='file'][accept*='.xlsx']")
                    if file_input_in_dialog.count() == 0:
                        file_input_in_dialog = dialog.locator("input[type='file']").last

                    if file_input_in_dialog.count() > 0:
                        file_input_in_dialog.set_input_files(temp_excel)
                        print(f"✅ 已上传文件: {temp_excel}")
                        human_delay(2000, 3000)
                    else:
                        print("⚠️ 在弹窗中未找到文件上传框")

                    # 点击"解析数据"
                    parse_btn = dialog.get_by_role("button", name="解析数据")
                    if parse_btn.count() > 0:
                        human_click(page, parse_btn.first)
                        print("✅ 已点击解析数据")
                        human_delay(3000, 5000)

                    # 关闭弹窗
                    close_btn = dialog.get_by_role("button", name="关闭")
                    if close_btn.count() > 0:
                        human_click(page, close_btn.first)
                        print("✅ 已关闭弹窗")
                    human_delay(1000, 1500)
            except Exception as e:
                print(f"⚠️ 批量导入失败: {e}")
        else:
            print("⚠️ 跳过批量导入（无有效Excel文件）")

        human_delay(1000, 1500)

        # 7. 投诉描述
        print("📝 填写投诉描述...")
        try:
            desc_textarea = page.get_by_role("textbox", name="请客观公正描述具体侵权所在，最多填写1000字")
            if desc_textarea.count() == 0:
                desc_textarea = page.locator("textarea").first
            if desc_textarea.count() > 0:
                human_type(page, desc_textarea.first, description)
                print("✅ 已填写投诉描述")
        except Exception as e:
            print(f"⚠️ 填写描述失败: {e}")

        human_delay(1000, 1500)

        # 8. 上传证明文件
        print("📤 上传证明文件...")
        if proof_file and os.path.exists(proof_file):
            try:
                proof_upload = None
                proof_label = page.get_by_text("证明文件", exact=True).first

                if proof_label.count() > 0:
                    proof_block = proof_label.locator("xpath=ancestor::div[contains(@class, 'mb-3')][1]").first
                    scoped_upload = proof_block.locator("input[type='file']")
                    if scoped_upload.count() > 0:
                        proof_upload = scoped_upload.first
                        print(f"[DEBUG] 在证明文件区域找到 {scoped_upload.count()} 个文件输入框")

                if proof_upload is None:
                    accept_upload = page.locator(
                        "input[type='file']:not([multiple])[accept*='.jpg'], "
                        "input[type='file']:not([multiple])[accept*='.jpeg'], "
                        "input[type='file']:not([multiple])[accept*='.png'], "
                        "input[type='file']:not([multiple])[accept*='.pdf']"
                    )
                    if accept_upload.count() > 0:
                        proof_upload = accept_upload.first
                        print(f"[DEBUG] 按 accept 属性找到 {accept_upload.count()} 个候选输入框")

                if proof_upload is None:
                    fallback_upload = page.locator("input[type='file']:not([multiple])")
                    if fallback_upload.count() > 0:
                        proof_upload = fallback_upload.first
                        print(f"[DEBUG] 使用兜底选择器，找到 {fallback_upload.count()} 个非 multiple 输入框")

                if proof_upload is not None:
                    proof_upload.set_input_files(proof_file)
                    print(f"✅ 已上传证明文件: {proof_file}")
                    human_delay(2000, 3000)
                else:
                    print("⚠️ 未找到证明文件上传框")
            except Exception as e:
                print(f"⚠️ 上传失败: {e}")

        human_delay(1000, 1500)

        # 8.5. 上传其他证明文件
        print("\n📤 上传其他证明文件...")
        if other_proof_files:
            try:
                scroll_to_bottom(page)
                human_delay(1000, 1500)

                other_proof_title = page.locator("text=其他证明：").first
                if other_proof_title.count() == 0:
                    other_proof_title = page.get_by_text("其他证明：", exact=True).first
                if other_proof_title.count() == 0:
                    other_proof_title = page.get_by_text("其他证明", exact=True).first

                if other_proof_title.count() > 0:
                    other_proof_title.scroll_into_view_if_needed()
                    human_delay(500, 800)

                    add_button = page.get_by_text("添 加", exact=True).first
                    if add_button.count() == 0:
                        add_button = page.get_by_text("添加", exact=True).first
                    if add_button.count() == 0:
                        add_button = page.locator("button.add.ant-btn-default").first

                    add_clicks = max(len(other_proof_files) - 1, 0)
                    if add_button.count() > 0:
                        for click_count in range(add_clicks):
                            add_button.scroll_into_view_if_needed()
                            human_delay(300, 500)
                            add_button.click()
                            print(f"✅ 第{click_count + 1}次点击添加按钮")
                            human_delay(2000, 2500)

                    other_proof_container = other_proof_title.locator("xpath=..").first
                    upload_wrappers = other_proof_container.locator(".upload-wrapper").all()
                    print(f"[DEBUG] 找到 {len(upload_wrappers)} 个 upload-wrapper 容器")

                    for idx, proof_path in enumerate(other_proof_files):
                        if idx >= len(upload_wrappers):
                            print(f"⚠️ upload-wrapper 数量不足，无法上传第{idx + 1}张")
                            break
                        if os.path.exists(proof_path):
                            wrapper = upload_wrappers[idx]
                            upload_text = wrapper.locator("p:has-text('点击上传')").first
                            if upload_text.count() == 0:
                                upload_text = wrapper.get_by_text("点击上传", exact=True).first
                            if upload_text.count() > 0:
                                upload_text.scroll_into_view_if_needed()
                                human_delay(300, 500)
                                upload_text.click()
                                human_delay(800, 1200)
                                file_input = wrapper.locator("input[type='file']")
                                if file_input.count() > 0:
                                    file_input.set_input_files(proof_path)
                                    print(f"✅ 上传第{idx + 1}张: {proof_path}")
                                    human_delay(2000, 2500)
                                else:
                                    print(f"⚠️ 第{idx + 1}张未找到文件输入框")
                            else:
                                print(f"⚠️ 第{idx + 1}张未找到点击上传区域")
                else:
                    print("⚠️ 未找到其他证明区域")
            except Exception as e:
                print(f"⚠️ 上传其他证明文件失败: {e}")

        # 9. 提交
        print("📨 提交投诉...")
        scroll_to_bottom(page)
        human_delay(1000, 1500)

        try:
            submit_btn = page.get_by_role("button", name="提 交")
            if submit_btn.count() == 0:
                submit_btn = page.get_by_role("button", name="提交")
            if submit_btn.count() > 0:
                human_click(page, submit_btn.first)
                print("✅ 已点击提交")
                human_delay(3000, 5000)
        except Exception as e:
            print(f"⚠️ 提交失败: {e}")

        # 10. 获取投诉单号
        print("\n🔢 获取投诉单号...")
        complaint_number = None

        try:
            human_delay(2000, 3000)

            success_dialog = page.locator(".el-message-box, .ant-modal, [role='dialog']").first
            if success_dialog.count() > 0 and success_dialog.is_visible():
                dialog_text = success_dialog.text_content()
                print(f"弹窗内容: {dialog_text[:200]}")

                match = re.search(r'(投诉单号|编号|单号)[：:]\s*([A-Z0-9]+)', dialog_text, re.IGNORECASE)
                if match:
                    complaint_number = match.group(2)
                else:
                    match = re.search(r'([A-Z0-9]{10,})', dialog_text)
                    if match:
                        complaint_number = match.group(1)

                if complaint_number:
                    print(f"✅ 从弹窗获取投诉单号: {complaint_number}")

                list_btn = success_dialog.get_by_role("button", name="投诉列表")
                if list_btn.count() == 0:
                    list_btn = success_dialog.get_by_text("投诉列表", exact=True)
                if list_btn.count() > 0:
                    human_click(page, list_btn.first)
                    print("✅ 已点击投诉列表")
                    human_delay(2000, 3000)
            else:
                list_btn = page.get_by_role("button", name="投诉列表")
                if list_btn.count() == 0:
                    list_btn = page.get_by_text("投诉列表", exact=True)
                if list_btn.count() > 0:
                    human_click(page, list_btn.first)
                    print("✅ 已点击投诉列表")
                    human_delay(2000, 3000)
        except Exception as e:
            print(f"⚠️ 获取投诉单号失败: {e}")

        if not complaint_number:
            print("🔍 在投诉列表中查找最新单号...")
            try:
                human_delay(2000, 3000)
                first_row = page.locator("table tbody tr").first
                if first_row.count() > 0:
                    second_cell = first_row.locator("td").nth(1)
                    if second_cell.count() > 0:
                        complaint_number = second_cell.text_content().strip()
                        print(f"✅ 从列表获取投诉单号: {complaint_number}")
            except Exception as e:
                print(f"⚠️ 从列表获取失败: {e}")

        # 保存结果
        result["completed_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        if complaint_number:
            result["status"] = "completed"
            result["complaint_number"] = complaint_number
            print(f"\n🎉 投诉提交成功！")
            print(f"📋 投诉单号: {complaint_number}")
        else:
            result["status"] = "completed"
            result["error"] = "未能获取投诉单号，请手动查看"
            print("\n⚠️ 未能获取投诉单号")

        save_task_result(task_id, result)

        # 关闭浏览器
        context.close()

        # 清理临时文件
        if temp_excel and os.path.exists(temp_excel) and temp_excel.startswith('/tmp/'):
            os.remove(temp_excel)
            print(f"🗑️ 已清理临时文件: {temp_excel}")

        return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="UC投诉自动化脚本（后端数据版）")
    parser.add_argument("--task-id", type=str, help="任务ID")
    parser.add_argument("--cookie", type=str, required=True, help="Cookie字符串")
    parser.add_argument("--links", type=str, help="链接列表，逗号分隔")
    parser.add_argument("--excel-file", type=str, help="Excel文件路径")
    parser.add_argument("--proof-file", type=str, help="证明文件路径")
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

    # 输出JSON结果供后端解析
    print("\n" + "=" * 50)
    print("JSON_RESULT_START")
    print(json.dumps(result, ensure_ascii=False))
    print("JSON_RESULT_END")
