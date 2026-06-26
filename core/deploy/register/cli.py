# -*- coding: utf-8 -*-
"""定义 deployment 注册脚本使用的命令行参数。"""

from __future__ import annotations

import argparse

from core.deploy.register.paths import DEFAULT_ROOT_CONFIG_PATH


def parse_args() -> argparse.Namespace:
    """解析部署脚本参数。"""

    parser = argparse.ArgumentParser(
        description="Register Prefect work-pool deployments for enabled modules."
    )
    parser.add_argument(
        "--config",
        default=str(DEFAULT_ROOT_CONFIG_PATH),
        help="Path to root config file. Defaults to config.yaml; ENV overlays config.<ENV>.yaml.",
    )
    parser.add_argument(
        "--module",
        default=None,
        help="Deploy only one module. Defaults to all enabled modules from the selected root config.",
    )
    parser.add_argument(
        "--pool",
        default=None,
        help="Optional pool override. If omitted, deployment item `execution.pool` is used.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print deployment plan without calling Prefect Server.",
    )
    parser.add_argument(
        "--paused",
        action="store_true",
        help="Force deployments to be paused.",
    )
    return parser.parse_args()
