# -*- coding: utf-8 -*-
"""集中定义注册流程共用的路径常量和路径解析逻辑。"""

from __future__ import annotations

import sys
from pathlib import Path

from core.settings import BASE_CONFIG_FILE

# 项目根目录、业务模块根目录以及默认根配置路径。
PROJECT_ROOT = Path(__file__).resolve().parents[3]
MODULES_ROOT = PROJECT_ROOT / "modules"
DEFAULT_ROOT_CONFIG_PATH = PROJECT_ROOT / BASE_CONFIG_FILE

if str(PROJECT_ROOT) not in sys.path:
    # 允许直接运行根目录脚本时，通过模块名导入 <module>.flows。
    sys.path.insert(0, str(PROJECT_ROOT))


def resolve_project_path(path_value: str, field_path: str) -> Path:
    """把 deploy.yaml 中的相对路径解析为项目根目录下的绝对路径。"""
    path = Path(path_value).expanduser()
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    path = path.resolve()
    if not path.exists():
        raise FileNotFoundError(f"`{field_path}` 指向的路径不存在: {path}")
    return path
