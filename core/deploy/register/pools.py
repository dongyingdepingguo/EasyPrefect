# -*- coding: utf-8 -*-
"""读取并校验根配置中的 Prefect work pool 定义。"""

from __future__ import annotations


def get_root_pools(root_config: dict) -> dict:
    """读取根 config.yaml 中的 deploy.pools。"""

    pools = root_config.get("deploy", {}).get("pools", {})
    if pools is None:
        return {}
    if not isinstance(pools, dict):
        raise ValueError("config.yaml 中的 `deploy.pools` 必须是 YAML 映射结构")
    return pools
