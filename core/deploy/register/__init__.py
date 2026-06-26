"""提供 Prefect work-pool deployment 注册的包级入口。"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core.deploy.register.runner import run_register

__all__ = ["run_register"]


def __getattr__(name: str):
    """按需暴露注册入口，避免导入包时提前加载 Prefect 相关依赖。"""

    if name == "run_register":
        from core.deploy.register.runner import run_register

        return run_register
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
