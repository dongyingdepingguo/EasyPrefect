# -*- coding: utf-8 -*-
"""处理 Prefect deployment 注册前的 Docker 镜像检查与构建。"""

from __future__ import annotations

import subprocess
import sys
from typing import Any

from core.deploy.register.paths import resolve_project_path
from core.deploy.register.planner import RunnerDeploymentPlan


def local_docker_image_exists(image: str) -> bool:
    """检查本机 Docker 镜像是否存在。"""
    try:
        result = subprocess.run(
            ["docker", "image", "inspect", image],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
    except FileNotFoundError as exc:
        raise ValueError("未找到 docker 命令，无法校验 Flow 镜像是否存在") from exc

    return result.returncode == 0


def build_docker_image(image: str, build_config: dict[str, Any]) -> None:
    """根据 deploy.yaml 中的 build 配置构建本地 Docker 镜像。"""
    dockerfile = build_config.get("dockerfile")
    if not dockerfile:
        raise ValueError(
            f"Flow 镜像 {image!r} 不存在，且未配置 `execution.build.dockerfile`，无法自动构建"
        )

    dockerfile_path = resolve_project_path(str(dockerfile), "execution.build.dockerfile")
    context_path = resolve_project_path(str(build_config.get("context", ".")), "execution.build.context")
    command = [
        "docker",
        "build",
        "-t",
        image,
        "-f",
        str(dockerfile_path),
        str(context_path),
    ]

    print(f"Flow 镜像 {image!r} 不存在，开始构建镜像...")
    print(f"Build command: {' '.join(command)}")
    sys.stdout.flush()

    try:
        result = subprocess.run(command, check=False)
    except FileNotFoundError as exc:
        raise ValueError("未找到 docker 命令，无法构建 Flow 镜像") from exc

    if result.returncode != 0:
        raise ValueError(f"Flow 镜像 {image!r} 构建失败，deploy 已停止")


def ensure_local_docker_image(image: str, build_config: dict[str, Any]) -> None:
    """确保本机存在 Flow 镜像；不存在时按配置自动构建。"""
    if local_docker_image_exists(image):
        return

    build_docker_image(image, build_config)

    if not local_docker_image_exists(image):
        raise ValueError(f"Flow 镜像 {image!r} 构建完成后仍未在本机 Docker 中找到")


def ensure_deployment_images(runner_deployments: list[RunnerDeploymentPlan]) -> None:
    """注册 deployment 前确保所有声明的本地镜像可用。"""
    checked_images = set()
    for item in runner_deployments:
        if not item.image or item.image in checked_images:
            continue
        ensure_local_docker_image(item.image, item.image_build)
        checked_images.add(item.image)
