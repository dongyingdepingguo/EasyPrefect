#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HOST_ROOT=""
ENV_NAME=""
COMPOSE_FILE=""

die() {
  echo "$*" >&2
  exit 1
}

read_required_env_name() {
  local env_file="$1"
  local env_name

  [[ -f "$env_file" ]] || die "未找到环境文件: $env_file"

  env_name="$(
    awk '
      /^[[:space:]]*#/ || /^[[:space:]]*$/ { next }
      {
        line = $0
        sub(/\r$/, "", line)
        sub(/^[[:space:]]*/, "", line)
        if (line ~ /^ENV[[:space:]]*=/) {
          sub(/^ENV[[:space:]]*=[[:space:]]*/, "", line)
          sub(/[[:space:]]+#.*$/, "", line)
          gsub(/^[[:space:]]+|[[:space:]]+$/, "", line)
          if ((line ~ /^".*"$/) || (line ~ /^'\''.*'\''$/)) {
            line = substr(line, 2, length(line) - 2)
          }
          value = line
          found = 1
        }
      }
      END {
        if (!found || value == "") {
          exit 1
        }
        print value
      }
    ' "$env_file"
  )" || die "$env_file 缺少 ENV，请设置为 dev、test 或 prod。"

  case "$env_name" in
    dev|test|prod)
      printf '%s\n' "$env_name"
      ;;
    *)
      die "$env_file 中 ENV 仅支持 dev、test、prod，当前值: $env_name"
      ;;
  esac
}

compose_file_for_env() {
  local project_dir="$1"
  local env_name="$2"

  case "$env_name" in
    dev)
      printf '%s\n' "$project_dir/docker-compose.yml"
      ;;
    test|prod)
      printf '%s\n' "$project_dir/docker-compose.server.yml"
      ;;
    *)
      die "不支持的 ENV: $env_name"
      ;;
  esac
}

configure_environment() {
  HOST_ROOT="${EASYPREFECT_DOCKER_HOST_ROOT:-$ROOT_DIR}"
  [[ -d "$HOST_ROOT" ]] || die "宿主机项目目录不存在: $HOST_ROOT"
  HOST_ROOT="$(cd "$HOST_ROOT" && pwd)"

  ENV_NAME="$(read_required_env_name "$HOST_ROOT/.env")"
  COMPOSE_FILE="$(compose_file_for_env "$HOST_ROOT" "$ENV_NAME")"
  [[ -f "$COMPOSE_FILE" ]] || die "未找到 docker compose 文件: $COMPOSE_FILE"

  export EASYPREFECT_DOCKER_HOST_ROOT="$HOST_ROOT"
  cd "$HOST_ROOT"
}

usage() {
  cat <<'EOF'
用法:
  ./flow-release.sh deploy [scripts/deploy_work_pool.py 参数...]
  ./flow-release.sh update [--dry-run] [--skip-deploy] [scripts/deploy_work_pool.py 参数...]

功能:
  deploy
    只重新注册 Prefect deployment。
    适用于新增 module/flow，或修改 deploy.yaml/config.yaml 中的 cron、参数、
    标签、并发等配置。

  update
    更新 worker 运行环境并重建 process-worker-* 容器。
    ENV=dev 时会 build 本地 runtime 镜像；ENV=test/prod 时会 pull server compose
    中声明的 worker 镜像。完成后默认执行一次 deploy。

常用示例:
  ./flow-release.sh deploy --dry-run
  ./flow-release.sh deploy --module example_job
  ./flow-release.sh update --dry-run
  ./flow-release.sh update
  ./flow-release.sh update --skip-deploy

环境变量:
  EASYPREFECT_DOCKER_HOST_ROOT  宿主机项目目录，默认使用当前仓库根目录。

环境识别:
  必须在 .env 中设置 ENV。
  ENV=dev 使用 docker-compose.yml。
  ENV=test 或 ENV=prod 使用 docker-compose.server.yml。
EOF
}

compose() {
  docker compose --env-file "$HOST_ROOT/.env" -f "$COMPOSE_FILE" --project-directory "$HOST_ROOT" "$@"
}

compose_deploy() {
  docker compose --env-file "$HOST_ROOT/.env" -f "$COMPOSE_FILE" --project-directory "$HOST_ROOT" --profile deploy "$@"
}

has_arg() {
  local expected="$1"
  shift

  local arg
  for arg in "$@"; do
    [[ "$arg" == "$expected" ]] && return 0
  done
  return 1
}

run_deploy() {
  local build_args=()

  if [[ "$ENV_NAME" == "dev" ]]; then
    build_args=(--build)
  fi

  if has_arg "--dry-run" "$@"; then
    compose_deploy run --rm --no-deps "${build_args[@]}" prefect-deploy python scripts/deploy_work_pool.py "$@"
    return
  fi

  compose_deploy run --rm "${build_args[@]}" prefect-deploy sh -c '
    prefect work-pool create default-process-pool --type process || true
    exec python scripts/deploy_work_pool.py "$@"
  ' sh "$@"
}

print_update_plan() {
  local worker_services=("$@")

  echo "Dry run: update worker runtime"
  echo "ENV=$ENV_NAME"
  echo "Compose 文件: $COMPOSE_FILE"
  if [[ "$ENV_NAME" == "dev" ]]; then
    echo "将执行: docker compose build prefect-deploy ${worker_services[*]}"
  else
    echo "将执行: docker compose pull prefect-deploy ${worker_services[*]}"
  fi
  echo "将执行: docker compose up -d --no-deps --force-recreate ${worker_services[*]}"
}

run_update() {
  local dry_run=false
  local skip_deploy=false
  local deploy_args=()
  local worker_services=()
  local arg

  for arg in "$@"; do
    case "$arg" in
      --dry-run)
        dry_run=true
        deploy_args+=("--dry-run")
        ;;
      --skip-deploy)
        skip_deploy=true
        ;;
      --yes)
        # 兼容旧脚本习惯；当前精简 update 不需要确认参数。
        ;;
      *)
        deploy_args+=("$arg")
        ;;
    esac
  done

  mapfile -t worker_services < <(compose_deploy config --services | awk '/^process-worker-/ { print }')
  ((${#worker_services[@]} > 0)) || die "compose 文件中未找到 process-worker-* service。"

  if [[ "$dry_run" == "true" ]]; then
    print_update_plan "${worker_services[@]}"
    if [[ "$skip_deploy" != "true" ]]; then
      run_deploy "${deploy_args[@]}"
    fi
    return
  fi

  if [[ "$ENV_NAME" == "dev" ]]; then
    compose_deploy build prefect-deploy "${worker_services[@]}"
  else
    compose_deploy pull prefect-deploy "${worker_services[@]}"
  fi

  compose up -d --no-deps --force-recreate "${worker_services[@]}"

  if [[ "$skip_deploy" != "true" ]]; then
    run_deploy "${deploy_args[@]}"
  fi
}

cmd="${1:-}"
if [[ -z "$cmd" || "$cmd" == "-h" || "$cmd" == "--help" ]]; then
  usage
  exit 0
fi
shift

configure_environment
echo "当前环境: ENV=$ENV_NAME"
echo "Compose 文件: $COMPOSE_FILE"

case "$cmd" in
  deploy)
    run_deploy "$@"
    ;;
  update)
    run_update "$@"
    ;;
  *)
    echo "未知命令: $cmd" >&2
    echo >&2
    usage >&2
    exit 2
    ;;
esac
