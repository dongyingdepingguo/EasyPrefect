# -*- coding: utf-8 -*-
"""项目级 .env 加载入口。"""

from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv

# 项目根目录；其它配置路径都以这个目录为基准解析。
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# 标记 .env 是否已经加载，避免多处读取配置时重复解析文件。
_DOTENV_LOADED = False


def load_dotenv_file() -> None:
    """只加载一次项目根目录 .env，且不覆盖外部已经注入的环境变量。"""
    global _DOTENV_LOADED
    if _DOTENV_LOADED:
        return

    _DOTENV_LOADED = True
    # override=False 保证部署系统、Shell 或 Docker 注入的变量优先级更高。
    load_dotenv(PROJECT_ROOT / ".env", override=False)
