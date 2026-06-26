# -*- coding: utf-8 -*-
"""编排启用模块的 Prefect Flow 注册流程。"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from core.deploy.register.config import load_module_config, load_root_config, resolve_modules
from core.deploy.register.docker import ensure_deployment_images
from core.deploy.register.paths import PROJECT_ROOT
from core.deploy.register.planner import PlanRow, build_runner_deployments, optional_list
from core.deploy.register.pools import get_root_pools
from core.deploy.register.prefect_client import (
    configure_prefect_api_url,
    create_start_on_deploy_flow_runs,
    deploy_runner_deployments,
    register_deployment_automations,
    validate_work_pools_exist,
)


def print_plan(plan_rows: list[PlanRow], root_config_path: Path, force_paused: bool) -> None:
    """打印本次部署计划；--dry-run 模式下只会执行到这里。"""

    print(f"Root config: {root_config_path}")
    print(f"Force paused: {force_paused}")
    print(f"Deployments: {len(plan_rows)}")
    for row in plan_rows:
        full_name = f"{row.flow_name}/{row.deployment_name}"
        start_text = "yes" if row.start_on_deploy else "no"
        print(
            f"- module={row.module_name} -> {full_name} -> pool={row.pool_name} -> {row.schedule} -> "
            f"image={row.image} -> automation={row.automation} -> start_on_deploy={start_text}"
        )


def run_register(args: argparse.Namespace) -> None:
    """加载配置、生成 deployment 对象，并调用 Prefect 注册。"""

    root_config_path = Path(args.config).expanduser().resolve()
    PROJECT_ROOT.joinpath("data").mkdir(exist_ok=True)
    root_config = load_root_config(root_config_path)
    configure_prefect_api_url(root_config)
    deploy_config = root_config.get("deploy", {})
    if not isinstance(deploy_config, dict):
        raise ValueError("config.yaml 的 `deploy` 必须是 YAML 映射结构")
    root_pools = get_root_pools(root_config)
    automation_profiles = deploy_config.get("automation_profiles", {})
    if not isinstance(automation_profiles, dict):
        raise ValueError("config.yaml 的 `deploy.automation_profiles` 必须是 YAML 映射结构")
    default_automations = optional_list(
        deploy_config.get("default_automations"),
        "deploy.default_automations",
    )
    automation_context: dict = {}
    module_names = resolve_modules(root_config, args.module)
    if not module_names:
        print_plan([], root_config_path=root_config_path, force_paused=args.paused)
        print("No enabled modules. Nothing to register.")
        return

    all_runner_deployments = []
    all_plan_rows = []
    for module_name in module_names:
        # 每个模块独立维护 deploy.yaml，根 config.yaml 只负责决定是否启用模块。
        _, module_config = load_module_config(module_name)
        runner_deployments, plan_rows = build_runner_deployments(
            module_name=module_name,
            root_pools=root_pools,
            automation_profiles=automation_profiles,
            default_automations=default_automations,
            automation_context=automation_context,
            module_config=module_config,
            pool_override=args.pool,
            force_paused=args.paused,
        )
        all_runner_deployments.extend(runner_deployments)
        all_plan_rows.extend(plan_rows)

    print_plan(all_plan_rows, root_config_path=root_config_path, force_paused=args.paused)
    sys.stdout.flush()
    if args.dry_run:
        return

    validate_work_pools_exist(list(root_pools))
    ensure_deployment_images(all_runner_deployments)
    deployment_ids = deploy_runner_deployments(all_runner_deployments)
    print(f"Registered deployments: {deployment_ids}")
    automation_ids = register_deployment_automations(all_runner_deployments, deployment_ids)
    if automation_ids:
        print(f"Registered automations: {automation_ids}")
    if not args.paused:
        flow_run_ids = create_start_on_deploy_flow_runs(all_runner_deployments, deployment_ids)
        if flow_run_ids:
            print(f"Started flow runs: {flow_run_ids}")
