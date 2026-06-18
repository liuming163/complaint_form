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
import traceback
from datetime import datetime, timezone
from pathlib import Path

import requests
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


def normalize_company_name(value):
    return (value or "").replace("（", "(").replace("）", ")").replace(" ", "").strip()


# ========== 保存任务结果 ==========
def save_task_result(task_id, result):
    result_dir = Path(__file__).resolve().parent / "task_results"
    result_dir.mkdir(parents=True, exist_ok=True)
    result_file = result_dir / f"{task_id}.json"

    with open(result_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)

    print(f"📁 任务结果已保存到: {result_file}")


def update_batch_result(result, batch_no, status, error=None, complaint_number=None):
    for batch in result["batches"]:
        if batch["batch_no"] == batch_no:
            batch["status"] = status
            batch["error"] = error
            if complaint_number is not None:
                batch["complaint_number"] = complaint_number
            break
    result["completed_batches"] = sum(1 for batch in result["batches"] if batch["status"] == "completed")
    result["failed_batches"] = sum(1 for batch in result["batches"] if batch["status"] == "failed")


def upload_batch_excel(page, excel_file):
    print(f"📎 导入批次文件: {excel_file}")
    # 注意：UC 平台关闭批量导入弹窗后，弹窗 DOM 节点并不会被移除，只是隐藏。
    # 该隐藏弹窗里带有标题 <div class="ant-modal-title">批量导入</div>，
    # 会与页面上真正的「批量导入」触发按钮文本重复。分批时必须只匹配“可见”元素，
    # 否则 .first 可能命中上一批遗留的隐藏标题。
    #
    # 更关键：上一批提交+点「继续」后，表单要重新挂载/清空表格才会重新渲染出
    # 「批量导入」触发按钮。当上一批有 200 行时，清表+重渲染较慢，固定等待不够，
    # 此刻页面上唯一的「批量导入」文本只剩隐藏弹窗标题 —— 因此小批次（如 2 行）
    # 必成功、大批次（200 行）第 2 批起必失败。这里显式等待“可见的触发按钮”出现。
    batch_btn = page.locator(
        "button:visible:has-text('批量导入'), "
        ":text-is('批量导入'):visible"
    )
    try:
        batch_btn.first.wait_for(state="visible", timeout=30000)
    except Exception:
        raise RuntimeError(
            "等待『批量导入』触发按钮可见超时——上一批表单可能尚未重新渲染完成"
        )

    human_click(page, batch_btn.first)
    print("✅ 已点击批量导入，等待弹窗...")
    human_delay(1500, 2500)

    # 同理，弹窗也只取“可见”的那个，避免抓到上一批遗留的隐藏弹窗。
    dialog = page.locator(
        ".el-dialog:visible, .ant-modal:visible, [role='dialog']:visible"
    ).first
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


def submit_form(page, task_id, batch_no):
    print("📨 提交投诉...")
    screenshot_dir = Path(__file__).resolve().parent / "task_results"
    screenshot_dir.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    before_path = screenshot_dir / f"{task_id}.batch_{batch_no:03d}.{ts}.before_submit.png"
    after_path = screenshot_dir / f"{task_id}.batch_{batch_no:03d}.{ts}.after_submit.png"

    try:
        page.screenshot(path=str(before_path), full_page=True)
        print(f"🖼️ 提交前截图已保存: {before_path}")
    except Exception as e:
        print(f"⚠️ 提交前截图保存失败: {e}")

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

    try:
        page.screenshot(path=str(after_path), full_page=True)
        print(f"🖼️ 提交后截图已保存: {after_path}")
    except Exception as e:
        print(f"⚠️ 提交后截图保存失败: {e}")


def get_success_dialog(page):
    dialogs = page.locator(
        ".el-message-box:visible, .ant-modal-wrap:visible, .ant-modal:visible, [role='dialog']:visible")
    dialogs.first.wait_for(state="visible", timeout=30000)
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


def verify_submit_success(page):
    """提交后确认出现『提交成功』对话框。失败时抛出，避免误判为已投诉。

    不点击对话框里的任何按钮——后续批次靠重新打开表单（reload）来重置页面，
    比依赖 UC 自身的『继续创建』在 200 行表格下原地重置可靠得多。
    """
    dialog = get_success_dialog(page)
    try:
        text = dialog.inner_text(timeout=5000)
    except Exception:
        text = ""
    if "提交成功" not in text:
        snippet = re.sub(r"\s+", " ", text)[:200]
        raise RuntimeError(f"提交后未出现『提交成功』提示，对话框内容：{snippet or '（空）'}")
    print("✅ 已确认提交成功")


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


# ========== 通过接口精确匹配投诉单号 ==========
UC_COMPLAIN_LIST_API = "https://ipp.uc.cn/api/complain/accuse"


def extract_xtstk_from_cookie(cookie_str):
    m = re.search(r'cmptstk=([^;]+)', cookie_str or '')
    if not m:
        raise RuntimeError("cookie 中找不到 cmptstk，无法构造 xtstk 请求头")
    return m.group(1).strip()


def fetch_complaints_via_api(cookie_str, page_size=100, page_no=1):
    xtstk = extract_xtstk_from_cookie(cookie_str)
    headers = {
        "accept": "*/*",
        "accept-language": "zh-CN,zh;q=0.9",
        "referer": "https://ipp.uc.cn/",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
        "xtstk": xtstk,
        "cookie": cookie_str,
    }
    resp = requests.get(
        UC_COMPLAIN_LIST_API,
        params={"pageNo": page_no, "pageSize": page_size, "platform": "uc"},
        headers=headers,
        timeout=15,
    )
    resp.raise_for_status()
    body = resp.json()
    if body.get("code") != 200:
        raise RuntimeError(f"投诉列表接口返回异常：{body}")
    return body.get("data", []) or []


def _parse_gmt_create(value):
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
    except ValueError:
        try:
            return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        except ValueError:
            return None


def match_complaints(records, expected_title, task_started_utc, batch_count):
    expected = normalize_company_name(expected_title)
    matched = []
    for r in records:
        created = _parse_gmt_create(r.get("gmt_create"))
        if not created or created < task_started_utc:
            continue
        evs = r.get("evidence_contents") or []
        if not evs:
            continue
        title = ((evs[0].get("work") or {}).get("url") or "").strip()
        if normalize_company_name(title) != expected:
            continue
        cid = r.get("complain_id")
        if cid:
            matched.append((created, cid))

    matched.sort(key=lambda x: x[0])
    return [cid for _, cid in matched[:batch_count]]


def resolve_complaint_numbers(cookie, work_name, task_started_utc, batch_count):
    """纯接口方式匹配；失败直接抛出，调用方转为任务失败。"""
    if not work_name:
        raise RuntimeError("缺少作品名称，无法通过接口匹配投诉单号")

    records = fetch_complaints_via_api(cookie, page_size=max(100, batch_count * 5))
    numbers = match_complaints(records, work_name, task_started_utc, batch_count)
    if len(numbers) != batch_count:
        raise RuntimeError(
            f"接口仅匹配到 {len(numbers)} 个投诉单号（剧名 '{work_name}'，"
            f"提交时间 ≥ {task_started_utc.isoformat()}），预期 {batch_count} 个，请人工核对"
        )
    return numbers


def resolve_one_batch_number(cookie, work_name, task_started_utc, seen_ids, retries=3):
    """逐批查询：提交某一批后立刻取该批新产生的那个投诉单号。

    用 seen_ids（已认领过的 complain_id 集合）排除前面批次的单号，返回本批的
    新单号；找不到时重试几次（接口有延迟）。失败返回 None，由调用方记占位。
    seen_ids 会被原地更新。
    """
    if not work_name:
        return None
    for attempt in range(retries):
        try:
            records = fetch_complaints_via_api(cookie, page_size=100)
        except Exception as e:
            print(f"  ⚠️ 查询单号接口异常(第{attempt+1}次): {e}")
            records = []

        expected = normalize_company_name(work_name)
        candidates = []
        for r in records:
            cid = r.get("complain_id")
            if not cid or cid in seen_ids:
                continue
            created = _parse_gmt_create(r.get("gmt_create"))
            if not created or created < task_started_utc:
                continue
            evs = r.get("evidence_contents") or []
            if not evs:
                continue
            title = ((evs[0].get("work") or {}).get("url") or "").strip()
            if normalize_company_name(title) != expected:
                continue
            candidates.append((created, cid))

        if candidates:
            candidates.sort(key=lambda x: x[0])
            cid = candidates[-1][1]  # 取最新的一个作为本批单号
            seen_ids.add(cid)
            return str(cid)

        if attempt < retries - 1:
            human_delay(2000, 3000)
    return None


def fill_initial_form(page, identity, agent, rights_holder, complaint_type, copyright_type,
                      module, content_type, description, proof_file, other_proof_files,
                      task_id=None, batch_no=0):
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
        normalized_target = normalize_company_name(rights_holder)
        print(f"🔎 原始目标值: {rights_holder}")
        print(f"🔎 归一化目标值: {normalized_target}")
        for idx in range(total_options):
            option_text = all_options.nth(idx).text_content().strip()
            normalized_option = normalize_company_name(option_text)
            print(f"  {idx + 1}. {option_text}")
            if (
                option_text == rights_holder
                or rights_holder in option_text
                or normalized_target == normalized_option
                or normalized_target in normalized_option
            ):
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
                    normalized_option = normalize_company_name(option_text)
                    if (
                        option_text == rights_holder
                        or rights_holder in option_text
                        or normalized_target == normalized_option
                        or normalized_target in normalized_option
                    ):
                        principal_option = current_options.nth(idx)
                        found = True
                        print(f"✅ 滚动后在第 {idx + 1} 个位置找到: {option_text}")
                        break

                if found:
                    break

                scroll_attempts += 1

            if not found:
                # 最后尝试：归一化匹配
                for idx in range(total_options):
                    option_text = all_options.nth(idx).text_content().strip()
                    normalized_option = normalize_company_name(option_text)
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
    if proof_file:
        print(f"🔎 证明文件路径: {proof_file}")
        print(f"🔎 证明文件存在: {os.path.exists(proof_file)}")
        if os.path.exists(proof_file):
            print(f"🔎 证明文件大小: {os.path.getsize(proof_file)} bytes")
    if proof_file and os.path.exists(proof_file):
        # 定位到"证明文件："区域的上传框（第一个 upload-wrapper）
        # //*[@id="evidences"]/div[1]/div/span/div/span/div/div
        proof_section = page.locator("#evidences").locator("h1:has-text('证明文件：')").first
        if proof_section.count() == 0:
            # 备用方案：直接找第一个 upload-wrapper
            proof_upload_wrapper = page.locator("#evidences .upload-wrapper").first
        else:
            # 找到证明文件区域下的 upload-wrapper
            proof_upload_wrapper = proof_section.locator("..").locator(".upload-wrapper").first

        if proof_upload_wrapper.count() == 0:
            proof_upload_wrapper = page.locator("#evidences .upload-wrapper").first

        upload_trigger = proof_upload_wrapper.locator("span.ant-upload[role='button'], span.ant-upload").first
        if upload_trigger.count() > 0:
            human_click(page, upload_trigger)
            human_delay(300, 800)

        file_input = proof_upload_wrapper.locator("input[type='file']")
        if file_input.count() == 0:
            raise RuntimeError("未找到证明文件上传框")

        file_input.set_input_files(proof_file)
        human_delay(1200, 1800)
        verify_file_input_has_file(page, file_input, os.path.basename(proof_file), "证明文件")
        print(f"✅ 已上传证明文件: {os.path.basename(proof_file)}")
        human_delay(1000, 1500)
        if task_id:
            log_upload_debug_state(page, task_id, batch_no, "after_proof_upload")

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
            upload_trigger = other_upload_wrappers[idx].locator("span.ant-upload[role='button'], span.ant-upload").first
            if upload_trigger.count() > 0:
                human_click(page, upload_trigger)
                human_delay(300, 800)

            file_input = other_upload_wrappers[idx].locator("input[type='file']")
            if file_input.count() == 0:
                print(f"⚠️ 第 {idx + 1} 个上传框未找到文件输入框，跳过")
                continue

            print(f"📤 第 {idx + 1} 个其他证明路径: {proof_path}")
            print(f"📤 第 {idx + 1} 个其他证明存在: {os.path.exists(proof_path)}")
            if os.path.exists(proof_path):
                print(f"📤 第 {idx + 1} 个其他证明大小: {os.path.getsize(proof_path)} bytes")
                file_input.set_input_files(proof_path)
                human_delay(1200, 1800)
                verify_file_input_has_file(page, file_input, os.path.basename(proof_path), f"其他证明#{idx + 1}")
                print(f"✅ 已上传第 {idx + 1} 个文件: {os.path.basename(proof_path)}")
            else:
                print(f"⚠️ 文件不存在: {proof_path}")

            human_delay(1500, 2000)

        print("✅ 其他证明文件上传完成")
        if task_id:
            log_upload_debug_state(page, task_id, batch_no, "after_other_proof_upload")


def log_upload_debug_state(page, task_id, batch_no, label):
    try:
        evidences = page.locator("#evidences").first
        if evidences.count() == 0:
            print(f"⚠️ {label}：未找到证明材料区域 #evidences")
            return

        screenshot_dir = Path(__file__).resolve().parent / "task_results"
        screenshot_dir.mkdir(parents=True, exist_ok=True)
        ts = time.strftime("%Y%m%d_%H%M%S")
        shot_path = screenshot_dir / f"{task_id}.batch_{batch_no:03d}.{ts}.{label}.png"
        evidences.screenshot(path=str(shot_path))
        print(f"🖼️ {label}区域截图已保存: {shot_path}")

        visible_text = evidences.inner_text().strip()
        visible_text = re.sub(r"\s+", " ", visible_text)
        print(f"📝 {label}区域文本: {visible_text[:500]}")

        file_lengths = page.evaluate("""
            () => Array.from(document.querySelectorAll('#evidences input[type="file"]'))
              .map((input, index) => ({ index, files: input.files ? input.files.length : -1 }))
        """)
        print(f"📎 {label} file inputs: {json.dumps(file_lengths, ensure_ascii=False)}")
    except Exception as e:
        print(f"⚠️ {label}调试信息采集失败: {e}")


def verify_file_input_has_file(page, locator, expected_name, label):
    wrapper = locator.locator("xpath=ancestor::div[contains(@class, 'upload-wrapper')][1]")
    try:
        preview = wrapper.locator(".preview-file, .preview").first
        preview.wait_for(state="visible", timeout=10000)
    except Exception:
        wrapper_html = wrapper.evaluate("el => el.outerHTML")
        wrapper_html = re.sub(r"\s+", " ", wrapper_html)
        raise RuntimeError(f"{label}上传后未出现成功预览: {expected_name}；wrapper={wrapper_html[:800]}")

    preview_text = ""
    try:
        preview_text = preview.text_content().strip()
    except Exception:
        preview_text = ""

    if expected_name and preview_text and expected_name[:4] not in preview_text:
        print(f"⚠️ {label}预览文本与文件名不完全匹配，预览={preview_text}，期望包含={expected_name[:4]}")
    print(f"✅ {label} UI 预览校验通过: {preview_text or expected_name}")


def open_complaint_form(page):
    print("📂 打开UC侵权投诉平台...")
    page.goto("https://ipp.uc.cn/#/home", wait_until="load")
    # 关键：goto 到“仅 hash 不同/相同”的 URL 不会触发真正的页面重载（同文档导航），
    # 分批重投时页面已停留在 #/home 的表单视图，goto 是空操作，表单不会重置、
    # “发起侵权投诉”按钮也不出现。这里显式 reload 强制 SPA 重新加载回首页落地页。
    try:
        page.reload(wait_until="load")
    except Exception as e:
        print(f"⚠️ 页面reload失败，继续尝试: {e}")
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

def replace_proof_files(page, new_proof_file, new_other_proof_files):
    """切换作品时替换证明文件和其他证明"""
    print("🔄 替换证明文件...")

    evidences_section = page.locator("#evidences")
    all_upload_wrappers = evidences_section.locator(".upload-wrapper").all()

    if not all_upload_wrappers:
        print("⚠️ 未找到上传区域")
        return

    # 1. 删除其他证明（从最后一个开始，保留第一个）
    if len(all_upload_wrappers) > 1:
        for i in range(len(all_upload_wrappers) - 1, 0, -1):
            wrapper = all_upload_wrappers[i]
            remove_btn = wrapper.locator(".remove.anticon")
            if remove_btn.count() > 0:
                remove_btn.first.click()
                human_delay(800, 1200)
                print(f"  ✅ 删除第 {i + 1} 个其他证明")
        # 重新获取
        all_upload_wrappers = evidences_section.locator(".upload-wrapper").all()

    # 2. 替换第一个（证明文件）：点击图片区域触发文件选择
    if all_upload_wrappers:
        first_wrapper = all_upload_wrappers[0]
        file_input = first_wrapper.locator("input[type='file']")
        if file_input.count() > 0 and os.path.exists(new_proof_file):
            file_input.set_input_files(new_proof_file)
            human_delay(1500, 2000)
            print(f"  ✅ 替换证明文件: {os.path.basename(new_proof_file)}")

    # 3. 上传新的其他证明文件
    for idx, proof_path in enumerate(new_other_proof_files):
        if not os.path.exists(proof_path):
            print(f"  ⚠️ 文件不存在: {proof_path}")
            continue

        if idx == 0 and len(all_upload_wrappers) > 1:
            # 第一个其他证明：替换已有的
            wrapper = evidences_section.locator(".upload-wrapper").all()[1]
            file_input = wrapper.locator("input[type='file']")
            if file_input.count() > 0:
                file_input.set_input_files(proof_path)
                human_delay(1200, 1800)
                print(f"  ✅ 替换第1个其他证明: {os.path.basename(proof_path)}")
        else:
            # 点击添加按钮
            add_button = evidences_section.get_by_text("添 加", exact=True)
            if add_button.count() == 0:
                add_button = evidences_section.get_by_text("添加", exact=True)
            if add_button.count() > 0:
                add_button.first.click()
                human_delay(1000, 1500)

            # 获取最新的 wrapper 列表，上传到最后一个
            current_wrappers = evidences_section.locator(".upload-wrapper").all()
            if current_wrappers:
                last_wrapper = current_wrappers[-1]
                file_input = last_wrapper.locator("input[type='file']")
                if file_input.count() > 0:
                    file_input.set_input_files(proof_path)
                    human_delay(1200, 1800)
                    print(f"  ✅ 上传第{idx + 1}个其他证明: {os.path.basename(proof_path)}")

    print("✅ 证明文件替换完成")


def main(args):
    task_id = args.task_id or f"uc_{int(time.time())}"
    cookie = args.cookie
    identity = args.identity
    agent = args.agent
    rights_holder = args.rights_holder
    module = args.module
    content_type = args.content_type
    description = args.description
    works_config = json.loads(args.works_config) if args.works_config else []
    complaint_type = args.complaint_type or ''
    copyright_type = args.copyright_type or ''

    # 计算总批次
    total_batches = sum(w.get('batch_count', len(w.get('excel_files', []))) for w in works_config)

    result = {
        "task_id": task_id,
        "status": "running",
        "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "completed_at": None,
        "complaint_numbers": [],
        "total_batches": total_batches,
        "completed_batches": 0,
        "failed_batches": 0,
        "current_batch": 0,
        "batches": [],
        "error": None,
    }

    # 初始化批次信息
    batch_no_global = 0
    for work in works_config:
        for ef in work.get('excel_files', []):
            batch_no_global += 1
            result["batches"].append({
                "batch_no": batch_no_global,
                "work_name": work['work_name'],
                "excel_file": ef,
                "status": "pending",
                "error": None,
            })

    if not cookie:
        result["status"] = "failed"
        result["error"] = "缺少Cookie"
        return result
    if not works_config:
        result["status"] = "failed"
        result["error"] = "缺少作品配置"
        return result

    print(f"🚀 开始执行UC多作品投诉任务: {task_id}")
    print(f"📦 作品数: {len(works_config)}，总批次: {total_batches}")
    current_step = "初始化浏览器"
    task_started_utc = datetime.now(timezone.utc)

    with sync_playwright() as p:
        chromium_path = os.getenv('PLAYWRIGHT_CHROMIUM_PATH', '').strip()
        launch_kwargs = {
            'headless': True,
            'args': [
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--lang=zh-CN,en-US",
            ],
        }
        if chromium_path:
            launch_kwargs['executable_path'] = chromium_path

        browser = p.chromium.launch(**launch_kwargs)
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
            current_step = "设置Cookie"
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

            current_step = "打开投诉表单"
            open_complaint_form(page)

            # 把所有作品的分片拍平成统一的批次列表，逐批独立处理。
            # 关键改动：不再依赖 UC 的「继续创建」原地重置表单——它在上一批
            # 表格有 200 行时清不掉，导致「批量导入」按钮再也不出现、第 2 批起必败。
            # 改为每批都重新打开表单（reload）并完整重填，保证每批都是干净的可导入状态。
            flat_batches = []
            for work in works_config:
                for excel_idx, excel_file in enumerate(work.get('excel_files', [])):
                    flat_batches.append((work, excel_idx, excel_file))

            # 逐批查询单号：已认领的 complain_id（避免后批匹配到前批的单号），
            # 以及随提交进度累积的单号列表（每批查到立刻落盘，超时也不丢已成功的）。
            seen_complain_ids = set()
            all_complaint_numbers = []

            for batch_idx, (work, excel_idx, excel_file) in enumerate(flat_batches):
                work_name = work['work_name']
                batch_no_global = batch_idx + 1
                result["current_batch"] = batch_no_global
                print(f"\n  --- 批次 {batch_no_global}/{total_batches}: "
                      f"{work_name} 第{excel_idx+1}片 ---")

                # 第一批用初始已填好的表单；从第二批起，重新打开并重填表单，
                # 用“当前这一批所属作品”的证明文件，彻底规避原地重置失效的问题。
                work_description = description.replace('${work_title}', work_name)
                if batch_idx == 0:
                    current_step = f"填写初始表单（{work_name}）"
                    fill_initial_form(
                        page, identity, agent, rights_holder, complaint_type, copyright_type,
                        module, content_type, work_description,
                        work['proof_file'], work.get('other_proof_files', []),
                        task_id=task_id, batch_no=0
                    )
                else:
                    current_step = f"批次{batch_no_global}重新打开表单"
                    open_complaint_form(page)
                    current_step = f"批次{batch_no_global}重填表单（{work_name}）"
                    fill_initial_form(
                        page, identity, agent, rights_holder, complaint_type, copyright_type,
                        module, content_type, work_description,
                        work['proof_file'], work.get('other_proof_files', []),
                        task_id=task_id, batch_no=batch_no_global
                    )

                current_step = f"批次{batch_no_global}导入Excel"
                upload_batch_excel(page, excel_file)

                current_step = f"批次{batch_no_global}提交投诉"
                submit_form(page, task_id, batch_no_global)

                current_step = f"批次{batch_no_global}确认提交成功"
                verify_submit_success(page)
                update_batch_result(result, batch_no_global, "completed")

                # 逐批查询单号并立刻落盘：哪怕后续批次超时/失败，
                # 已成功批次的单号也不会丢（之前是攒到最后统一查，最后一步超时会全丢）。
                current_step = f"批次{batch_no_global}查询单号"
                human_delay(2000, 3000)
                number = resolve_one_batch_number(
                    cookie, work_name, task_started_utc, seen_complain_ids
                )
                if number:
                    all_complaint_numbers.append(number)
                    update_batch_result(result, batch_no_global, "completed", complaint_number=number)
                    print(f"✅ 批次{batch_no_global}单号: {number}")
                else:
                    all_complaint_numbers.append(f"未获取到单号:{work_name}")
                    print(f"⚠️ 批次{batch_no_global}未查到单号（作品: {work_name}）")
                result["complaint_numbers"] = list(all_complaint_numbers)
                save_task_result(task_id, result)
                human_delay(1000, 2000)

            result["status"] = "completed"

        except Exception as e:
            batch_no = result.get("current_batch") or 1
            update_batch_result(result, batch_no, "failed", str(e))
            result["status"] = "partial_failed" if result["completed_batches"] > 0 else "failed"
            result["error"] = f"{current_step}失败：{str(e)}"
            print(f"❌ 执行失败（{current_step}）: {e}")
            try:
                fail_dir = Path(__file__).resolve().parent / "task_results"
                fail_dir.mkdir(parents=True, exist_ok=True)
                screenshot_path = fail_dir / f"{task_id}.fail.png"
                page.screenshot(path=str(screenshot_path), full_page=True)
                result["fail_screenshot"] = str(screenshot_path)
                result["fail_url"] = page.url
                print(f"🖼️ 失败截图已保存: {screenshot_path}")
            except Exception as snap_error:
                print(f"⚠️ 失败截图保存失败: {snap_error}")
        finally:
            result["completed_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
            save_task_result(task_id, result)
            context.close()

    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="UC投诉自动化脚本（多作品版）")
    parser.add_argument("--task-id", type=str, help="任务ID")
    parser.add_argument("--cookie", type=str, required=True, help="Cookie字符串")
    parser.add_argument("--identity", type=str, required=True, help="身份类型")
    parser.add_argument("--agent", type=str, required=True, help="代理人/权利人")
    parser.add_argument("--rights-holder", type=str, required=True, help="权利人名称")
    parser.add_argument("--module", type=str, required=True, help="功能模块")
    parser.add_argument("--content-type", type=str, required=True, help="内容类型")
    parser.add_argument("--description", type=str, required=True, help="投诉描述")
    parser.add_argument("--works-config", type=str, required=True, help="作品配置JSON")
    parser.add_argument("--complaint-type", type=str, default='', help="投诉类型")
    parser.add_argument("--copyright-type", type=str, default='', help="著作权类型")

    args = parser.parse_args()
    try:
        result = main(args)
    except Exception as e:
        traceback.print_exc()
        result = {
            "task_id": args.task_id,
            "status": "failed",
            "started_at": None,
            "completed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "complaint_number": None,
            "complaint_numbers": [],
            "total_batches": 0,
            "completed_batches": 0,
            "failed_batches": 0,
            "current_batch": 0,
            "batches": [],
            "error": f"脚本初始化或主流程失败：{str(e)}",
        }

    print("\n" + "=" * 50)
    print("JSON_RESULT_START")
    print(json.dumps(result, ensure_ascii=False))
    print("JSON_RESULT_END")