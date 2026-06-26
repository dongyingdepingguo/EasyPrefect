# -*- coding: utf-8 -*-
"""根据模块 deploy.yaml 构建 Prefect runner deployment 计划。"""

from __future__ import annotations

import copy
import datetime as dt
import os
import string
from dataclasses import dataclass
from typing import Any

from core.deploy.register.config import load_flow
from core.deploy.register.paths import PROJECT_ROOT
from core.settings import BASE_CONFIG_FILE, current_env

# Docker worker 运行在宿主机时，可通过环境变量显式指定宿主机项目根目录。
DOCKER_HOST_PROJECT_ROOT = os.environ.get("EASYPREFECT_DOCKER_HOST_ROOT", str(PROJECT_ROOT))
ENV_NAME = current_env()


def docker_base_volumes() -> list[str]:
    volumes = [
        f"{DOCKER_HOST_PROJECT_ROOT}/{BASE_CONFIG_FILE}:/app/{BASE_CONFIG_FILE}:ro",
    ]
    if ENV_NAME:
        overlay_name = f"config.{ENV_NAME}.yaml"
        if (PROJECT_ROOT / overlay_name).exists():
            volumes.append(
                f"{DOCKER_HOST_PROJECT_ROOT}/{overlay_name}:/app/{overlay_name}:ro"
            )
    volumes.append(f"{DOCKER_HOST_PROJECT_ROOT}/data:/app/data:rw")
    return volumes


DOCKER_BASE_JOB_ENV = {
    "TZ": "Asia/Shanghai",
    "PYTHONPATH": "/app",
    "DO_NOT_TRACK": "1",
}
if ENV_NAME:
    DOCKER_BASE_JOB_ENV["ENV"] = ENV_NAME

DOCKER_BASE_JOB_VARIABLES = {
    # Docker worker 的通用运行参数；deployment.execution.job_variables 会覆盖或补充这里。
    "image_pull_policy": "Never",
    "env": DOCKER_BASE_JOB_ENV,
    "volumes": docker_base_volumes(),
    "stream_output": True,
}


@dataclass
class RunnerDeploymentPlan:
    """描述一个待注册的 runner deployment 及其后续自动化元数据。"""

    runner_deployment: Any
    pool_name: str
    image: str | None
    image_build: dict[str, Any]
    automations: list[dict[str, Any]]
    start_on_deploy: bool
    flow_name: str
    deployment_name: str
    schedule_text: str
    module_name: str


@dataclass
class PlanRow:
    """描述打印到控制台的一行注册计划摘要。"""

    module_name: str
    flow_name: str
    deployment_name: str
    pool_name: str
    schedule: str
    image: str | None
    automation: str
    start_on_deploy: bool
    paused: bool


def optional_mapping(value: Any, field_path: str) -> dict[str, Any]:
    """读取可选 YAML 映射字段，并给出清晰的中文错误。"""
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"`{field_path}` 必须是 YAML 映射结构")
    return value


def optional_list(value: Any, field_path: str) -> list[Any]:
    """读取可选 YAML 列表字段，并给出清晰的中文错误。"""
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f"`{field_path}` 必须是 YAML 列表结构")
    return value


def merge_tags(*tag_lists: list[Any]) -> list[Any]:
    """合并标签列表，并保持原有顺序。"""
    merged = []
    for tags in tag_lists:
        for tag in tags:
            if tag not in merged:
                merged.append(tag)
    return merged


def deep_merge_config(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """递归合并 deployment 配置字典。

    映射值会递归合并，列表和标量由 override 覆盖；
    automation tags 例外，会追加后去重。
    """
    merged = copy.deepcopy(base)
    for key, value in override.items():
        if (
            key in merged
            and isinstance(merged[key], dict)
            and isinstance(value, dict)
        ):
            merged[key] = deep_merge_config(merged[key], value)
            continue
        if key == "tags" and isinstance(merged.get(key), list) and isinstance(value, list):
            merged[key] = merge_tags(merged[key], value)
            continue
        merged[key] = copy.deepcopy(value)
    return merged


def render_template_value(value: Any, variables: dict[str, Any], field_path: str) -> Any:
    """渲染配置值中的 `{name}` 占位符。

    当字符串本身正好是一个占位符时，保留原变量类型，
    让 schedule_after_seconds 这类 YAML 字段仍可保持整数。
    """
    if isinstance(value, dict):
        return {
            key: render_template_value(item, variables, f"{field_path}.{key}")
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [
            render_template_value(item, variables, f"{field_path}[{index}]")
            for index, item in enumerate(value)
        ]
    if not isinstance(value, str) or "{" not in value:
        return value

    parsed = list(string.Formatter().parse(value))
    field_names = [field_name for _, field_name, _, _ in parsed if field_name]
    if not field_names:
        return value

    if (
        len(parsed) == 1
        and parsed[0][0] == ""
        and parsed[0][1]
        and parsed[0][2] == ""
        and parsed[0][3] is None
    ):
        field_name = parsed[0][1]
        if field_name not in variables:
            raise ValueError(f"automation profile 字段 `{field_path}` 缺少变量 `{field_name}`。")
        return variables[field_name]

    missing = [field_name for field_name in field_names if field_name not in variables]
    if missing:
        raise ValueError(
            f"automation profile 字段 `{field_path}` 缺少变量: {', '.join(missing)}。"
        )
    return value.format(**variables)


def expand_automation_profile(
    automation_config: dict[str, Any],
    automation_profiles: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    """将 deployment automation profile 展开为完整的 automation 配置。"""
    if not automation_config:
        return {}

    profile_name = automation_config.get("profile")
    if not profile_name:
        return automation_config

    if profile_name not in automation_profiles:
        raise ValueError(f"automation profile {profile_name!r} 未在根 config.yaml 中定义。")

    profile_config = optional_mapping(
        automation_profiles[profile_name],
        f"deploy.automation_profiles.{profile_name}",
    )
    profile_vars = optional_mapping(
        profile_config.get("vars"),
        f"deploy.automation_profiles.{profile_name}.vars",
    )
    local_vars = optional_mapping(
        automation_config.get("vars"),
        "deployments[].automations[].vars",
    )

    profile_body = {
        key: value
        for key, value in profile_config.items()
        if key not in {"profile", "vars"}
    }
    local_body = {
        key: value
        for key, value in automation_config.items()
        if key not in {"profile", "vars"}
    }
    merged = deep_merge_config(profile_body, local_body)
    variables = dict(context)
    variables.update(
        render_template_value(
            profile_vars,
            variables,
            f"deploy.automation_profiles.{profile_name}.vars",
        )
    )
    variables.update(
        render_template_value(local_vars, variables, "deployments[].automations[].vars")
    )
    return render_template_value(merged, variables, f"automation profile {profile_name}")


def normalize_job_variable_value(key: str, value: Any) -> Any:
    """规范化 worker job_variables 中需要项目路径参与的字段。"""
    if key != "volumes":
        return value

    normalized = []
    for item in value or []:
        if isinstance(item, str) and item.startswith("./"):
            # 相对挂载路径统一转为项目根目录下的绝对路径，便于 Docker worker 识别。
            normalized.append(f"{PROJECT_ROOT}/{item[2:]}")
        else:
            normalized.append(item)
    return normalized


def merge_job_variables(*configs: dict[str, Any]) -> dict[str, Any]:
    """合并 worker job_variables；env 按键合并，其它字段后者覆盖前者。"""
    merged: dict[str, Any] = {}
    for config in configs:
        for key, value in config.items():
            if key == "env":
                # 环境变量按键合并，避免模块只改一个变量时丢掉默认变量。
                merged.setdefault("env", {}).update(value or {})
                continue
            merged[key] = normalize_job_variable_value(key, value)
    return merged


def add_deployment_env(
    job_variables: dict[str, Any],
    *,
    module_name: str,
    flow_name: str,
    deployment_id: str,
    deployment_name: str,
    pool_name: str,
) -> dict[str, Any]:
    """向 worker job_variables 注入当前 deployment 的运行时元信息。"""
    return merge_job_variables(
        job_variables,
        {
            "env": {
                "EASYPREFECT_MODULE_NAME": module_name,
                "EASYPREFECT_FLOW_NAME": flow_name,
                "EASYPREFECT_DEPLOYMENT_ID": deployment_id,
                "EASYPREFECT_DEPLOYMENT_NAME": deployment_name,
                "EASYPREFECT_WORK_POOL_NAME": pool_name,
            }
        },
    )


def resolve_deployment_runtime(
    pool_name: str,
    pool_cfg: dict[str, Any],
    execution_config: dict[str, Any],
) -> tuple[str | None, dict[str, Any], dict[str, Any]]:
    """根据 work pool 类型解析 image/build/job_variables。"""
    pool_type = pool_cfg.get("type")
    pool_job_variables = optional_mapping(pool_cfg.get("job_variables"), f"pools.{pool_name}.job_variables")
    execution_job_variables = optional_mapping(
        execution_config.get("job_variables"),
        "deployments[].execution.job_variables",
    )

    if pool_type == "docker":
        image = execution_config.get("image")
        if not image:
            raise ValueError(
                f"Docker pool {pool_name!r} 对应的 deployment 必须配置 `execution.image`"
            )

        build_config = execution_config.get("build", {}) or {}
        if not isinstance(build_config, dict):
            raise ValueError("`deployments[].execution.build` 必须是 YAML 映射结构")

        job_variables = merge_job_variables(
            copy.deepcopy(DOCKER_BASE_JOB_VARIABLES),
            pool_job_variables,
            execution_job_variables,
        )
        return str(image), job_variables, build_config

    if pool_type == "process":
        if not pool_job_variables and not execution_job_variables:
            raise ValueError(
                f"Process pool {pool_name!r} 对应的 deployment 必须配置 `execution.job_variables`"
            )
        return None, merge_job_variables(pool_job_variables, execution_job_variables), {}

    return None, merge_job_variables(pool_job_variables, execution_job_variables), {}


def collect_automation_configs(
    default_automations: list[Any],
    deployment_item: dict[str, Any],
    field_path: str,
) -> list[dict[str, Any]]:
    """合并根默认 automation 与 deployment 自己声明的 automation。"""
    configs: list[dict[str, Any]] = []
    for index, item in enumerate(default_automations):
        configs.append(optional_mapping(item, f"deploy.default_automations[{index}]"))

    deployment_automations = optional_list(
        deployment_item.get("automations"),
        f"{field_path}.automations",
    )
    for index, item in enumerate(deployment_automations):
        configs.append(optional_mapping(item, f"{field_path}.automations[{index}]"))

    return configs


def expand_automation_configs(
    automation_configs: list[dict[str, Any]],
    automation_profiles: dict[str, Any],
    context: dict[str, Any],
) -> list[dict[str, Any]]:
    """展开 deployment 上全部 automation 配置。"""
    return [
        expand_automation_profile(config, automation_profiles, context)
        for config in automation_configs
    ]


def resolve_pool(
    root_pools: dict[str, Any],
    module_config: dict[str, Any],
    deployment_item: dict[str, Any],
    execution_config: dict[str, Any],
    pool_override: str | None,
) -> tuple[str, dict[str, Any]]:
    """解析 deployment 使用的 work pool，模块 deploy.yaml 可覆盖根 config.yaml。"""
    module_pools = optional_mapping(module_config.get("pools"), "pools")
    pools = {**root_pools, **module_pools}
    pool_name = pool_override or execution_config.get("pool")
    if not pool_name:
        raise ValueError("每个 deployment 必须配置 `execution.pool`，或通过命令行传入 --pool。")
    if pool_name not in pools:
        raise ValueError(f"Pool {pool_name!r} 未在根 config.yaml 或模块 deploy.yaml 中定义")
    if not isinstance(pools[pool_name], dict):
        raise ValueError(f"Pool {pool_name!r} 必须是 YAML 映射结构")
    return pool_name, pools[pool_name]


def build_schedules(
    schedules_config: list[Any],
    field_path: str,
    default_timezone: str,
) -> tuple[list[Any], str]:
    """根据 deployments[].schedules 构建 Prefect schedule 列表。"""
    from prefect.client.schemas.schedules import CronSchedule, IntervalSchedule

    schedules = []
    schedule_texts = []
    for index, schedule_item in enumerate(schedules_config):
        item_path = f"{field_path}[{index}]"
        if not isinstance(schedule_item, dict):
            raise ValueError(f"`{item_path}` 必须是 YAML 映射结构")

        has_cron = "cron" in schedule_item
        has_interval = "interval_seconds" in schedule_item
        if has_cron == has_interval:
            raise ValueError(f"`{item_path}` 必须且只能配置 `cron` 或 `interval_seconds` 之一")

        allowed_keys = (
            {"cron", "timezone", "day_or"}
            if has_cron
            else {"interval_seconds", "timezone"}
        )
        unknown_keys = set(schedule_item) - allowed_keys
        if unknown_keys:
            raise ValueError(
                f"`{item_path}` 包含不支持的字段: {', '.join(sorted(unknown_keys))}"
            )

        timezone = schedule_item.get("timezone", default_timezone)
        if not isinstance(timezone, str):
            raise ValueError(f"`{item_path}.timezone` 必须是字符串")

        if has_cron:
            cron = schedule_item.get("cron")
            if not cron:
                raise ValueError(f"`{item_path}.cron` 不能为空")
            if not isinstance(cron, str):
                raise ValueError(f"`{item_path}.cron` 必须是字符串")

            schedule_kwargs = {
                "cron": cron,
                "timezone": timezone,
            }
            if "day_or" in schedule_item:
                day_or = schedule_item["day_or"]
                if not isinstance(day_or, bool):
                    raise ValueError(f"`{item_path}.day_or` 必须是布尔值")
                schedule_kwargs["day_or"] = day_or

            schedules.append(CronSchedule(**schedule_kwargs))
            schedule_texts.append(str(cron))
            continue

        interval_seconds = schedule_item.get("interval_seconds")
        if not isinstance(interval_seconds, int) or isinstance(interval_seconds, bool):
            raise ValueError(f"`{item_path}.interval_seconds` 必须是正整数")
        if interval_seconds <= 0:
            raise ValueError(f"`{item_path}.interval_seconds` 必须大于 0")

        schedules.append(
            IntervalSchedule(
                interval=dt.timedelta(seconds=interval_seconds),
                timezone=timezone,
            )
        )
        schedule_texts.append(f"every {interval_seconds}s")

    if not schedules:
        return [], "manual only"
    return schedules, "; ".join(schedule_texts)


def build_runner_deployments(
    module_name: str,
    root_pools: dict[str, Any],
    automation_profiles: dict[str, Any],
    default_automations: list[Any] | None,
    automation_context: dict[str, Any] | None,
    module_config: dict[str, Any],
    pool_override: str | None,
    force_paused: bool,
) -> tuple[list[RunnerDeploymentPlan], list[PlanRow]]:
    """把模块 deploy.yaml 中的 deployments 转换为 Prefect Runner deployment。"""
    from prefect.deployments.runner import EntrypointType

    defaults = module_config.get("defaults", {})
    default_timezone = defaults.get("timezone", "Asia/Shanghai")
    default_paused = bool(defaults.get("paused", False))
    default_tags = list(defaults.get("tags", []))
    flow_name = module_config["module"]["flow_name"]
    flow_obj = load_flow(module_name, flow_name)

    runner_deployments = []
    plan_rows = []

    for index, item in enumerate(module_config.get("deployments", [])):
        if not isinstance(item, dict):
            raise ValueError(f"`deployments[{index}]` 必须是 YAML 映射结构")
        if not item.get("enabled", True):
            # deployment 级别可以临时关闭，关闭后不会出现在注册计划里。
            continue

        deployment_name = item.get("name")
        if not deployment_name:
            raise ValueError(f"模块 {module_name!r} 的 deployment 缺少 `name`。")

        configured_deployment_id = item.get("id")
        deployment_id = configured_deployment_id or deployment_name
        if (
            "schedule" in item
            or "cron" in item
            or "interval_seconds" in item
            or "timezone" in item
        ):
            raise ValueError(
                f"Deployment {deployment_name!r} 已改用 `schedules` 列表配置调度，"
                "并使用 deployment 级 `paused` 字段。"
            )
        schedules_config = optional_list(
            item.get("schedules"),
            f"deployments[{index}].schedules",
        )
        execution_config = optional_mapping(item.get("execution"), f"deployments[{index}].execution")
        automation_configs = collect_automation_configs(
            default_automations or [],
            item,
            f"deployments[{index}]",
        )

        pool_name, pool_cfg = resolve_pool(
            root_pools,
            module_config,
            item,
            execution_config,
            pool_override,
        )
        image, job_variables, image_build = resolve_deployment_runtime(
            pool_name,
            pool_cfg,
            execution_config,
        )

        tags = execution_config.get("tags", [])
        deployment_id_tags = (
            [str(configured_deployment_id)]
            if configured_deployment_id
            else []
        )
        parameters = execution_config.get("parameters", {})
        concurrency_limit = execution_config.get("concurrency_limit", 1)
        start_on_deploy = bool(item.get("start_on_deploy", False))
        paused = item.get("paused", default_paused)
        job_variables = add_deployment_env(
            job_variables,
            module_name=module_name,
            flow_name=flow_obj.name,
            deployment_id=deployment_id,
            deployment_name=deployment_name,
            pool_name=pool_name,
        )
        render_context = {
            "module_name": module_name,
            "flow_name": flow_obj.name,
            "module_flow_name": flow_name,
            "deployment_name": deployment_name,
            "deployment_id": deployment_id,
            "pool_name": pool_name,
        }
        render_context.update(automation_context or {})
        automations = expand_automation_configs(
            automation_configs,
            automation_profiles,
            context=render_context,
        )

        deployment_tags = list(
            dict.fromkeys([*tags, *default_tags, module_name, *deployment_id_tags])
        )

        # to_deployment 负责描述 Prefect 侧的部署元信息；真正的 Docker 运行参数在
        # Prefect 的 job_variables 和 deploy(..., image=...) 中传入。
        to_deployment_kwargs: dict[str, Any] = {
            "name": deployment_name,
            "work_pool_name": pool_name,
            "job_variables": job_variables,
            "tags": deployment_tags,
            "paused": True if force_paused else bool(paused),
            "parameters": parameters,
            "concurrency_limit": int(concurrency_limit),
            "entrypoint_type": EntrypointType.MODULE_PATH,
        }

        schedules, schedule_text = build_schedules(
            schedules_config,
            f"deployments[{index}].schedules",
            default_timezone,
        )
        if schedules:
            to_deployment_kwargs["schedules"] = schedules

        runner_deployments.append(
            RunnerDeploymentPlan(
                runner_deployment=flow_obj.to_deployment(**to_deployment_kwargs),
                pool_name=pool_name,
                image=image,
                image_build=image_build,
                automations=automations,
                start_on_deploy=start_on_deploy,
                flow_name=flow_obj.name,
                deployment_name=deployment_name,
                schedule_text=schedule_text,
                module_name=module_name,
            )
        )

        # plan_rows 只用于 dry-run 或注册前打印计划，便于确认本次会注册哪些 deployment。
        plan_rows.append(
            PlanRow(
                module_name=module_name,
                flow_name=flow_obj.name,
                deployment_name=deployment_name,
                pool_name=pool_name,
                schedule=schedule_text,
                image=image,
                automation=(
                    f"{sum(1 for config in automations if config.get('enabled', False))}/{len(automations)} enabled"
                    if automations
                    else "disabled"
                ),
                start_on_deploy=start_on_deploy,
                paused=True if force_paused else bool(paused),
            )
        )

    return runner_deployments, plan_rows
