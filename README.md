# EasyPrefect

EasyPrefect 是一个精简的 Prefect 3 项目骨架。它保留了配置读取、日志辅助、
work pool deployment 注册和本地 Docker Compose 运行环境，不包含原项目中的
broker、cache、market、db、workflow 和任何业务 modules。

## 目录

- `core/`: 通用配置、日志和 deployment 注册逻辑。
- `modules/`: 业务 Flow 模块根目录，初始为空。
- `scripts/deploy_work_pool.py`: 读取 `config.yaml` 和模块 `deploy.yaml` 后注册 deployment。
- `config.yaml`: 根配置，声明启用模块和 work pool。
- `docker-compose.yml`: 本地 Prefect Server、PostgreSQL 和 Process worker。
- `docker-compose.server.yml`: 服务器 Compose 文件，使用已发布的 worker runtime 镜像。
- `flow-release.sh`: 注册 deployment，或更新并重建 `process-worker-*`。
- `release-image.sh`: 构建并推送 runtime 镜像，更新服务器 Compose 默认镜像。
- `example/.env.example`: 本地环境变量示例。

## 本地启动

```bash
cp example/.env.example .env
docker compose build
docker compose up -d database prefect-server process-worker-main
```

Prefect UI 默认地址：

```text
http://127.0.0.1:4200
```

注册 deployment：

```bash
./flow-release.sh deploy
```

初始 `deploy.enabled_modules` 为空，注册命令会正常输出 `Deployments: 0`。

更新本地 worker runtime 并重建所有 `process-worker-*`：

```bash
./flow-release.sh update
```

发布服务器 runtime 镜像：

```bash
./release-image.sh
```

`release-image.sh` 默认发布到
`192.168.2.212:5080/oak-quant/workflow-scheduling/easy-prefect-runtime`，
可通过 `REGISTRY_NAMESPACE` 或 `RUNTIME_IMAGE` 环境变量覆盖。

## ClickHouse

连接信息优先从 `.env` 读取，例如：

```text
CLICKHOUSE_HOST=localhost
CLICKHOUSE_DB=default
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=
CLICKHOUSE_HTTP_PORT=8123
CLICKHOUSE_SECURE=false
```

验证连接：

```bash
uv run python scripts/check_clickhouse.py
```

各模块在自己的 `deploy.yaml` 中通过 `runtime.clickhouse` 声明落库表和写入模式。
默认 `enabled: false`，确认目标表存在后再打开。

## 添加模块

每个业务模块建议保持以下结构：

```text
modules/example_job/
  __init__.py
  flow.py
  deploy.yaml
```

`flow.py` 示例：

```python
from prefect import flow


@flow(name="Example Job")
def example_job_flow() -> None:
    print("hello EasyPrefect")
```

`deploy.yaml` 示例：

```yaml
module:
  flow_name: example_job_flow

deployments:
  - name: example-job-manual
    schedules: []
    execution:
      pool: default-process-pool
      concurrency_limit: 1
      tags:
        - example
```

然后在 `config.yaml` 中启用：

```yaml
deploy:
  enabled_modules:
    - example_job
```
