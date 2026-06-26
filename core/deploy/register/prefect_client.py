# -*- coding: utf-8 -*-
"""封装 deployment 注册流程中与 Prefect API 的交互。"""

from __future__ import annotations

import os
from datetime import timedelta
from typing import Any
from uuid import UUID

from core.deploy.register.planner import RunnerDeploymentPlan
from core.env import load_dotenv_file


def configure_prefect_api_url(root_config: dict[str, Any]) -> None:
    """环境变量未显式配置时，使用 config.yaml 中的 prefect.api_url。"""
    load_dotenv_file()
    if os.environ.get("PREFECT_API_URL"):
        return
    prefect_config = root_config.get("prefect", {})
    if not isinstance(prefect_config, dict):
        return
    api_url = prefect_config.get("api_url")
    if api_url:
        os.environ["PREFECT_API_URL"] = str(api_url)


def validate_work_pools_exist(pool_names: list[str]) -> None:
    """确认 config.yaml 声明的 work pools 已存在于 Prefect Server。"""
    if not pool_names:
        return

    from prefect import get_client
    from prefect.exceptions import ObjectNotFound

    missing_pool_names = []
    with get_client(sync_client=True) as client:
        for pool_name in pool_names:
            try:
                client.read_work_pool(pool_name)
            except ObjectNotFound:
                missing_pool_names.append(pool_name)

    if missing_pool_names:
        names = ", ".join(repr(name) for name in missing_pool_names)
        raise ValueError(f"config.yaml 中声明的 Prefect work pool 不存在: {names}")


def deploy_runner_deployments(runner_deployments: list[RunnerDeploymentPlan]) -> list[str]:
    """调用 Prefect 注册 runner deployments。"""
    from prefect.deployments.runner import deploy

    deployment_ids = []
    for item in runner_deployments:
        # 镜像已在注册前校验或自动构建；Prefect 注册阶段只绑定 image，不再 build/push。
        ids = deploy(
            item.runner_deployment,
            work_pool_name=item.pool_name,
            image=item.image,
            build=False,
            push=False,
            print_next_steps_message=False,
            ignore_warnings=True,
        )
        deployment_ids.extend(ids)
    return deployment_ids


def seconds_to_timedelta(config: dict[str, Any], key: str) -> timedelta:
    """从配置中读取秒数字段并转换为 timedelta，默认值为零。"""
    return timedelta(seconds=int(config.get(key, 0)))


def build_automation_trigger(config: dict[str, Any]):
    """根据 deploy.yaml 配置构建 Prefect Automation trigger。"""
    from prefect.automations import CompoundTrigger, EventTrigger

    trigger_type = config.get("type")
    trigger_kwargs = dict(config)
    trigger_kwargs.pop("type", None)

    if "within_seconds" in trigger_kwargs:
        trigger_kwargs["within"] = seconds_to_timedelta(trigger_kwargs, "within_seconds")
        trigger_kwargs.pop("within_seconds", None)

    if trigger_type == "event":
        return EventTrigger(**trigger_kwargs)

    if trigger_type == "compound":
        child_triggers = trigger_kwargs.get("triggers")
        if not isinstance(child_triggers, list) or not all(
            isinstance(item, dict) for item in child_triggers
        ):
            raise ValueError("compound automation trigger 必须配置 `triggers` 映射列表。")
        trigger_kwargs["triggers"] = [
            build_automation_trigger(item)
            for item in child_triggers
        ]
        return CompoundTrigger(**trigger_kwargs)

    raise ValueError(f"暂不支持的 automation trigger type: {trigger_type!r}")


def resolve_deployment_id(config: dict[str, Any], current_deployment_id: str) -> UUID:
    """解析 automation action 配置中使用的 deployment id 别名。"""
    deployment = config.get("deployment")
    deployment_id = config.get("deployment_id")
    if deployment == "self":
        return UUID(str(current_deployment_id))
    if deployment_id:
        return UUID(str(deployment_id))
    raise ValueError("deployment action 必须配置 `deployment: self` 或 `deployment_id`。")


def build_automation_action(config: dict[str, Any], current_deployment_id: str):
    """根据 deploy.yaml 配置构建 Prefect Automation action。"""
    from prefect.automations import (
        DoNothing,
        PauseDeployment,
        ResumeDeployment,
        RunDeployment,
        SendNotification,
    )

    action_type = config.get("type")
    action_kwargs = dict(config)

    if action_type in {"run-deployment", "pause-deployment", "resume-deployment"}:
        action_kwargs["deployment_id"] = resolve_deployment_id(
            action_kwargs,
            current_deployment_id,
        )
        action_kwargs.pop("deployment", None)

    if action_type == "run-deployment":
        if "schedule_after_seconds" in action_kwargs:
            action_kwargs["schedule_after"] = seconds_to_timedelta(
                action_kwargs,
                "schedule_after_seconds",
            )
            action_kwargs.pop("schedule_after_seconds", None)
        return RunDeployment(**action_kwargs)

    if action_type == "pause-deployment":
        return PauseDeployment(**action_kwargs)

    if action_type == "resume-deployment":
        return ResumeDeployment(**action_kwargs)

    if action_type == "send-notification":
        return SendNotification(**action_kwargs)

    if action_type == "do-nothing":
        return DoNothing(**action_kwargs)

    raise ValueError(f"暂不支持的 automation action type: {action_type!r}")


def build_automation_actions(configs: list[dict[str, Any]], current_deployment_id: str):
    """根据 deploy.yaml 配置构建全部 Automation actions。"""
    if not configs:
        raise ValueError("automation 必须至少配置一个 action。")
    return [
        build_automation_action(config, current_deployment_id)
        for config in configs
    ]


def build_deployment_automation(
    plan: RunnerDeploymentPlan,
    deployment_id: str,
    config: dict[str, Any],
):
    """根据 deployment 计划构建 Prefect Automation 对象。"""
    from prefect.automations import Automation

    trigger_config = config.get("trigger")
    actions_config = config.get("actions")
    if not isinstance(trigger_config, dict):
        raise ValueError(f"Deployment {plan.deployment_name!r} 的 automation 必须配置 `trigger`。")
    if not isinstance(actions_config, list) or not all(
        isinstance(item, dict) for item in actions_config
    ):
        raise ValueError(f"Deployment {plan.deployment_name!r} 的 automation 必须配置 `actions` 列表。")

    return Automation(
        name=config.get("name") or f"{plan.flow_name}/{plan.deployment_name} - automation",
        description=config.get("description", ""),
        enabled=bool(config.get("enabled", True)),
        tags=list(config.get("tags", [])),
        trigger=build_automation_trigger(trigger_config),
        actions=build_automation_actions(actions_config, deployment_id),
    )


def automation_config_name(plan: RunnerDeploymentPlan, config: dict[str, Any]) -> str:
    """计算 automation 配置对应的 Prefect 名称。"""
    return str(config.get("name") or f"{plan.flow_name}/{plan.deployment_name} - automation")


def register_deployment_automations(
    runner_deployments: list[RunnerDeploymentPlan],
    deployment_ids: list[str],
) -> list[str]:
    """创建或更新模块 deploy.yaml 中声明的 deployment automations。"""
    from prefect import get_client

    if len(runner_deployments) != len(deployment_ids):
        raise ValueError(
            "Deployment 注册结果数量与计划数量不一致，无法安全绑定 automation。"
        )

    registered_ids = []
    with get_client(sync_client=True) as client:
        for plan, deployment_id in zip(runner_deployments, deployment_ids, strict=True):
            for config in plan.automations:
                if not config.get("enabled", False):
                    automation_name = automation_config_name(plan, config)
                    for existing in client.read_automations_by_name(automation_name):
                        client.pause_automation(existing.id)
                    continue

                automation = build_deployment_automation(plan, deployment_id, config)

                existing = client.read_automations_by_name(automation.name)
                if existing:
                    automation.id = existing[0].id
                    automation.update()
                else:
                    automation.create()

                registered_ids.append(str(automation.id))

    return registered_ids


def create_start_on_deploy_flow_runs(
    runner_deployments: list[RunnerDeploymentPlan],
    deployment_ids: list[str],
) -> list[str]:
    """为选择启用的 deployments 创建一次 bootstrap flow run。"""
    from prefect import get_client

    if len(runner_deployments) != len(deployment_ids):
        raise ValueError(
            "Deployment 注册结果数量与计划数量不一致，无法安全启动 deployment。"
        )

    flow_run_ids = []
    with get_client(sync_client=True) as client:
        for plan, deployment_id in zip(runner_deployments, deployment_ids, strict=True):
            if not plan.start_on_deploy:
                continue

            flow_run = client.create_flow_run_from_deployment(UUID(str(deployment_id)))
            flow_run_ids.append(str(flow_run.id))

    return flow_run_ids
