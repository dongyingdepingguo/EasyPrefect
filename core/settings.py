# -*- coding: utf-8 -*-
"""环境变量、根配置和模块配置的统一读取入口。"""

from __future__ import annotations

import copy
import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

from core.env import PROJECT_ROOT, load_dotenv_file

BASE_CONFIG_FILE = "config.yaml"


def current_env() -> str:
    """读取当前运行环境名；返回小写 ENV，未设置时返回空字符串。"""
    load_dotenv_file()
    return os.getenv("ENV", "").strip().lower()


def load_yaml_file(path: Path, *, required: bool = False) -> dict[str, Any]:
    """读取 YAML 文件，并保证顶层结构是映射。"""
    if not path.exists():
        if required:
            raise FileNotFoundError(f"Config file not found: {path}")
        return {}

    with path.open("r", encoding="utf-8") as file:
        data = yaml.safe_load(file) or {}
    if not isinstance(data, dict):
        raise ValueError(f"配置文件顶层必须是 YAML 映射结构: {path}")
    return data


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """递归合并配置；映射递归合并，列表和标量由覆盖配置替换。"""
    merged = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(merged.get(key), dict) and isinstance(value, dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


@lru_cache(maxsize=1)
def root_config() -> dict[str, Any]:
    """读取根配置：先加载 config.yaml，再按 ENV 叠加 config.<ENV>.yaml。"""
    base = load_yaml_file(PROJECT_ROOT / BASE_CONFIG_FILE, required=True)
    env_name = current_env()
    if not env_name:
        return base

    overlay_path = PROJECT_ROOT / f"config.{env_name}.yaml"
    if not overlay_path.exists():
        return base
    return deep_merge(base, load_yaml_file(overlay_path))


def root_config_from_path(path: Path) -> dict[str, Any]:
    """读取命令行指定的根配置；默认 config.yaml 会自动套用 ENV 覆盖。"""
    path = path.expanduser()
    if not path.is_absolute():
        path = PROJECT_ROOT / path

    if path.resolve() == (PROJECT_ROOT / BASE_CONFIG_FILE).resolve():
        return root_config()
    return load_yaml_file(path, required=True)


def get_config(path: str | tuple[str, ...], default: Any = None) -> Any:
    """按点分路径或 key 元组读取根配置，例如 redis.host。"""
    keys = tuple(path.split(".")) if isinstance(path, str) else path
    value: Any = root_config()
    for key in keys:
        if not isinstance(value, dict) or key not in value:
            return default
        value = value[key]
    return value


def config_value(env_key: str, path: str | tuple[str, ...], default: Any = None) -> Any:
    """读取单项配置；环境变量优先，其次是 YAML，最后是默认值。"""
    value = env_value(env_key)
    if value is not None:
        return value
    return get_config(path, default)


def config_int(env_key: str, path: str | tuple[str, ...], default: int = 0) -> int:
    """读取整数配置，优先级同 config_value。"""
    return int(config_value(env_key, path, default))


def env_value(*names: str, default: str | None = None) -> str | None:
    """按顺序读取多个候选环境变量名，返回第一个已设置的值。"""
    load_dotenv_file()
    for name in names:
        value = os.getenv(name)
        if value is not None:
            return value
    return default


def env_bool(name: str, default: bool = False) -> bool:
    """读取布尔环境变量，支持 true/false、yes/no、on/off 和 1/0。"""
    value = env_value(name)
    if value is None:
        return default
    return as_bool(value, name)


def as_bool(value: Any, field_name: str) -> bool:
    """把 YAML 或环境变量中的布尔表达转换成 bool。"""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "no", "n", "off"}:
            return False
    if isinstance(value, int) and not isinstance(value, bool) and value in (0, 1):
        return bool(value)
    raise ValueError(f"{field_name} 必须是布尔值")


def module_deploy_config(module_name: str) -> dict[str, Any]:
    """读取模块自己的 deploy.yaml。"""
    module_path = PROJECT_ROOT / "modules" / module_name / "deploy.yaml"
    return load_yaml_file(module_path, required=True)


@lru_cache(maxsize=None)
def module_runtime(module_name: str, section: str | None = None) -> dict[str, Any]:
    """读取模块 deploy.yaml 的 runtime 配置，可进一步取某个子 section。"""
    runtime = module_deploy_config(module_name).get("runtime", {}) or {}
    if section is None:
        return runtime
    value = runtime.get(section, {}) or {}
    if not isinstance(value, dict):
        raise ValueError(f"{module_name}.runtime.{section} 必须是 YAML 映射结构")
    return value
