# -*- coding: utf-8 -*-
"""将启用模块的 Prefect Flow 注册为 work pool deployment。"""

from __future__ import annotations

import sys
from pathlib import Path

_PROJECT_ROOT_FOR_IMPORT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT_FOR_IMPORT) not in sys.path:
    # 允许从 scripts/ 目录直接执行本文件时仍能导入项目内的 core/deploy 模块。
    sys.path.insert(0, str(_PROJECT_ROOT_FOR_IMPORT))

from core.deploy.register import run_register
from core.deploy.register.cli import parse_args


def main() -> None:
    """解析命令行参数并执行 deployment 注册。"""

    run_register(parse_args())


if __name__ == "__main__":
    try:
        main()
    except (FileNotFoundError, ValueError) as exc:
        print(f"错误: {exc}", file=sys.stderr)
        sys.exit(1)
