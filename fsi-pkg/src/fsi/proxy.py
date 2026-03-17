"""SOCKS 代理配置 — 自动 SSH 隧道 + 环境变量

配置文件 proxy-config.json 位于项目根目录，格式：
{
  "enabled": true,
  "proxy": "socks5h://127.0.0.1:1080",
  "ssh_tunnel": {
    "host": "ec2-52-81-27-194.cn-north-1.compute.amazonaws.com.cn",
    "user": "ubuntu",
    "key": "/home/ubuntu/aws/bj_ec2_keypair.pem",
    "local_port": 1080
  }
}

启动逻辑：
1. enabled 为 false 或文件不存在 → 静默跳过
2. 有 ssh_tunnel 配置 → 检查端口是否已监听，未监听则自动拉起 SSH -D 隧道
3. 设置 HTTP_PROXY/HTTPS_PROXY/ALL_PROXY 环境变量
"""

import json
import os
import socket
import subprocess
import time
from pathlib import Path

from fsi.config import FSI_HOME
_CONFIG_FILE = FSI_HOME / "proxy-config.json"


def _port_in_use(port: int) -> bool:
    """检查本地端口是否已被监听"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1)
        return s.connect_ex(("127.0.0.1", port)) == 0


def _start_ssh_tunnel(tunnel_cfg: dict) -> bool:
    """启动 SSH SOCKS5 隧道，返回是否成功

    使用 ssh -D <port> -N -f 后台运行，不开交互 shell。
    """
    host = tunnel_cfg.get("host", "")
    user = tunnel_cfg.get("user", "ubuntu")
    key = tunnel_cfg.get("key", "")
    port = tunnel_cfg.get("local_port", 1080)

    if not host or not key:
        return False

    if not Path(key).exists():
        print(f"代理: SSH 密钥不存在: {key}", flush=True)
        return False

    cmd = [
        "ssh",
        "-D", str(port),
        "-N", "-f",
        "-o", "StrictHostKeyChecking=no",
        "-o", "ServerAliveInterval=60",
        "-o", "ServerAliveCountMax=3",
        "-o", "ExitOnForwardFailure=yes",
        "-o", "ConnectTimeout=10",
        "-i", key,
        f"{user}@{host}",
    ]

    try:
        subprocess.run(cmd, check=True, capture_output=True, timeout=15)
    except subprocess.TimeoutExpired:
        print("代理: SSH 隧道连接超时", flush=True)
        return False
    except subprocess.CalledProcessError as e:
        print(f"代理: SSH 隧道启动失败: {e.stderr.decode().strip()}", flush=True)
        return False

    # 等待端口就绪
    for _ in range(10):
        if _port_in_use(port):
            return True
        time.sleep(0.3)

    print("代理: SSH 隧道启动后端口未就绪", flush=True)
    return False


def init_proxy(no_proxy: bool = False) -> str | None:
    """读取 proxy-config.json，自动拉起 SSH 隧道并设置代理环境变量

    Args:
        no_proxy: 为 True 时跳过代理（--no-proxy flag）

    Returns:
        代理 URL 字符串，或 None（未启用/跳过）
    """
    if no_proxy:
        return None

    if not _CONFIG_FILE.exists():
        return None

    try:
        with open(_CONFIG_FILE, "r", encoding="utf-8") as f:
            config = json.load(f)
    except (json.JSONDecodeError, OSError):
        return None

    if not config.get("enabled", False):
        return None

    proxy_url = config.get("proxy", "")
    if not proxy_url:
        return None

    # 自动拉起 SSH 隧道
    tunnel_cfg = config.get("ssh_tunnel")
    if tunnel_cfg:
        port = tunnel_cfg.get("local_port", 1080)
        if not _port_in_use(port):
            print(f"代理: 正在建立 SSH 隧道到 {tunnel_cfg.get('host')}...", flush=True)
            if _start_ssh_tunnel(tunnel_cfg):
                print(f"代理: SSH 隧道已建立 (本地端口 {port})", flush=True)
            else:
                print("代理: SSH 隧道建立失败，跳过代理", flush=True)
                return None

    os.environ["HTTP_PROXY"] = proxy_url
    os.environ["HTTPS_PROXY"] = proxy_url
    os.environ["ALL_PROXY"] = proxy_url

    return proxy_url
