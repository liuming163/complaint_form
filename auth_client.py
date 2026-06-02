"""Authentication 服务客户端 — 对接远程认证网关"""

import base64
import json
import random
import string
import time
import hmac
import hashlib

import requests
import urllib3
from Crypto.Cipher import AES

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

AUTH_SERVICE_URL = 'https://hotdl.xinjudata.com'
AES_KEY = b'ir9crios932j60vm'
JWT_KEY = '77897159'
SYSTEM_TYPE = '5'


def _get_random_iv():
    chars = string.ascii_lowercase + string.digits
    return ''.join(random.choice(chars) for _ in range(16))


def _aes_encrypt(plaintext: str) -> str:
    """AES-CBC 加密，格式：iv(16) + base64(hex(ciphertext))"""
    iv = _get_random_iv()
    data = plaintext.encode('utf-8')
    pad_len = AES.block_size - (len(data) % AES.block_size)
    data += bytes([pad_len]) * pad_len
    cipher = AES.new(AES_KEY, AES.MODE_CBC, iv.encode('utf-8'))
    encrypted = cipher.encrypt(data)
    hex_str = encrypted.hex().upper()
    b64_str = base64.b64encode(hex_str.encode('utf-8')).decode('utf-8')
    return iv + b64_str


def _aes_decrypt(ciphertext: str) -> str:
    """AES-CBC 解密服务端响应，格式：iv(16) + base64(raw_ciphertext)"""
    iv = ciphertext[:16].encode('utf-8')
    b64_part = ciphertext[16:]
    encrypted_bytes = base64.b64decode(b64_part)
    cipher = AES.new(AES_KEY, AES.MODE_CBC, iv)
    decrypted = cipher.decrypt(encrypted_bytes)
    pad_len = decrypted[-1]
    return decrypted[:-pad_len].decode('utf-8')


def _b64encode_url(data: bytes) -> bytes:
    return base64.urlsafe_b64encode(data).replace(b'=', b'')


def _b64decode_url(data: bytes) -> bytes:
    rem = len(data) % 4
    if rem > 0:
        data += b'=' * (4 - rem)
    return base64.urlsafe_b64decode(data)


def jwt_decode(token: str) -> dict:
    """本地解析 JWT token（不调远程，共用同一个 key）"""
    if isinstance(token, str):
        token = token.encode('utf-8')
    parts = token.split(b'.')
    if len(parts) != 3:
        return None

    header_bs, payload_bs, signature_bs = parts
    hm = hmac.new(JWT_KEY.encode(), header_bs + b'.' + payload_bs, digestmod='SHA256')
    if signature_bs != _b64encode_url(hm.digest()):
        return None

    payload_json = _b64decode_url(payload_bs)
    payload = json.loads(payload_json)

    if time.time() > payload.get('exp', 0):
        return None

    return payload


def login(username: str, password: str, client_ip: str) -> dict:
    """
    调用远程 Authentication 服务登录。
    返回: {'success': True, 'token': '...', 'user_info': {...}} 或 {'success': False, 'error': '...'}
    """
    login_data = {
        'uname': username,
        'password': password,
        'presentIP': client_ip,
        'system_type': SYSTEM_TYPE,
    }

    sign = _aes_encrypt(json.dumps(login_data))

    try:
        resp = requests.post(
            f'{AUTH_SERVICE_URL}/first/verify_user/',
            json={'sign': sign},
            timeout=10,
            verify=False,
        )
        result = resp.json()
    except Exception as e:
        return {'success': False, 'error': f'认证服务连接失败: {e}'}

    if result.get('code') != 200:
        return {'success': False, 'error': result.get('msg', '登录失败')}

    encrypted_data = result.get('data', '')
    if not encrypted_data:
        return {'success': False, 'error': '认证服务返回数据异常'}

    try:
        raw_str = _aes_decrypt(encrypted_data)
        user_info = eval(raw_str)
    except Exception as e:
        return {'success': False, 'error': f'解密响应失败: {e}'}

    token = user_info.get('tok', '')
    if not token:
        return {'success': False, 'error': '未获取到token'}

    return {
        'success': True,
        'token': token,
        'user_info': {
            'uid': user_info.get('uid'),
            'level': user_info.get('level'),
            'username': username,
        }
    }


def verify_token(token: str, client_ip: str) -> dict:
    """
    验证 JWT token 有效性（本地解析，不调远程）。
    返回: {'valid': True, 'payload': {...}} 或 {'valid': False, 'error': '...'}
    """
    payload = jwt_decode(token)
    if payload is None:
        return {'valid': False, 'error': 'token无效或已过期'}

    if str(payload.get('present', '')) != str(client_ip):
        return {'valid': False, 'error': 'IP不匹配，请重新登录'}

    return {'valid': True, 'payload': payload}
