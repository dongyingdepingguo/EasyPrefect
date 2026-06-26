# -*- coding: utf-8 -*-
"""负责注册阶段的配置读取、模块发现和 Flow 动态加载。"""

from __future__ import annotations

import importlib
from pathlib import Path
from typing import Any

from core.deploy.register.paths import MODULES_ROOT
from core.settings import load_yaml_file, root_config_from_path


def load_yaml(path: Path) -> dict[str, Any]:
    """读取 YAML 文件，并保证顶层结构是字典。"""
    return load_yaml_file(path, required=True)


def load_root_config(config_path: Path) -> dict[str, Any]:
    """读取项目根 config.yaml。"""
    return root_config_from_path(config_path)


def resolve_module_dir(module_name: str) -> Path:
    """解析业务模块目录。"""
    return MODULES_ROOT / module_name


def discover_module_names() -> list[str]:
    """发现当前代码镜像中存在 deploy.yaml 的业务模块名。"""

    module_names = set()
    if MODULES_ROOT.exists():
        for module_dir in MODULES_ROOT.iterdir():
            if module_dir.is_dir() and (module_dir / "deploy.yaml").exists():
                module_names.add(module_dir.name)

    return sorted(module_names)


def load_module_config(module_name: str) -> tuple[Path, dict[str, Any]]:
    """读取单个业务模块的 deploy.yaml，并校验必填部署字段。"""
    config_path = resolve_module_dir(module_name) / "deploy.yaml"
    data = load_yaml(config_path)
    if "deployments" not in data:
        raise ValueError(f"模块配置必须包含 `deployments`: {config_path}")
    if "module" not in data or "flow_name" not in data["module"]:
        raise ValueError(f"模块配置必须包含 `module.flow_name`: {config_path}")
    return config_path, data


def _module_names_from_list(value: Any, field_path: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f"config.yaml 的 `{field_path}` 必须是 YAML 列表结构")
    return [str(item) for item in value]


def enabled_module_names(root_config: dict[str, Any]) -> list[str]:
    """读取启用模块。"""
    deploy_config = root_config.get("deploy", {})
    return _module_names_from_list(
        deploy_config.get("enabled_modules") if isinstance(deploy_config, dict) else None,
        "deploy.enabled_modules",
    )


def resolve_modules(root_config: dict[str, Any], module_override: str | None) -> list[str]:
    """根据根配置和 --module 参数确定本次需要注册的模块。"""
    enabled_modules = enabled_module_names(root_config)
    if module_override:
        if module_override not in enabled_modules:
            raise ValueError(
                f"模块 {module_override!r} 未在 config.yaml 的 deploy.enabled_modules 中定义"
            )
        return [module_override]
    return enabled_modules


def load_flow(module_name: str, flow_name: str):
    """从业务模块的 flow.py 中动态加载 Prefect flow 对象。"""
    import_path = f"modules.{module_name}.flow"
    flow_module = importlib.import_module(import_path)
    try:
        return getattr(flow_module, flow_name)
    except AttributeError as exc:
        raise ValueError(f"在模块 {import_path} 中未找到 Flow {flow_name!r}") from exc
