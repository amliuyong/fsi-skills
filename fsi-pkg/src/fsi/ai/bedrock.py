"""Bedrock Claude 调用，多 profile failover，支持 aws-config.json 配置"""

import json
import random
import time
from pathlib import Path

import boto3
from botocore.config import Config

from fsi.config import FSI_DIR

# 默认值（aws-config.json 不存在时使用）
_DEFAULT_MODEL_ID = "global.anthropic.claude-opus-4-6-v1"
_DEFAULT_MAX_TOKENS = 8192
_DEFAULT_PROFILES = [
    {"profile_name": "my_bedrock", "region": "ap-northeast-1"},
    {"profile_name": "my_bedrock_jp", "region": "ap-northeast-1"},
    {"profile_name": "my_work_us", "region": "us-east-1"},
    {"profile_name": "my_work_jp", "region": "ap-northeast-1"},
    {"profile_name": "my_c1_us", "region": "us-east-1"},
    {"profile_name": "my_c1_jp", "region": "ap-northeast-1"},
]

_boto_config = Config(read_timeout=600, connect_timeout=10)

# 缓存加载结果
_aws_config = None


def _load_aws_config() -> dict:
    """加载 data/aws-config.json，不存在则返回默认配置。"""
    global _aws_config
    if _aws_config is not None:
        return _aws_config

    config_path = FSI_DIR.parent / "aws-config.json"
    if config_path.exists():
        with open(config_path, "r", encoding="utf-8") as f:
            _aws_config = json.load(f)
    else:
        _aws_config = {}
    return _aws_config


def call_bedrock(system: str, user_msg: str, max_retries: int = 3) -> str:
    """调用 Bedrock Claude，随机选 profile，失败重试。"""
    cfg = _load_aws_config()
    model_id = cfg.get("model", _DEFAULT_MODEL_ID)
    max_tokens = cfg.get("max_tokens", _DEFAULT_MAX_TOKENS)
    profiles = cfg.get("profiles", _DEFAULT_PROFILES)

    for attempt in range(max_retries):
        prof = random.choice(profiles)
        try:
            session = boto3.Session(
                profile_name=prof["profile_name"],
                region_name=prof["region"],
            )
            client = session.client("bedrock-runtime", config=_boto_config)

            body = json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": max_tokens,
                "system": system,
                "messages": [{"role": "user", "content": user_msg}],
            })

            response = client.invoke_model(
                modelId=model_id,
                body=body,
                contentType="application/json",
                accept="application/json",
            )

            result = json.loads(response["body"].read())
            return result["content"][0]["text"].strip()

        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                raise
