#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HOST_ROOT=""
ENV_NAME=""
COMPOSE_FILE=""
RUNTIME_IMAGE_HISTORY_FILE=""
RUNTIME_IMAGE_HISTORY_KEEP=3

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
  RUNTIME_IMAGE_HISTORY_FILE="$HOST_ROOT/data/.runtime-image-history"

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
    不会重建 runtime 镜像；需要更新运行环境请使用 update。

  update
    更新 worker 运行环境并重建 process-worker-* 容器。
    ENV=dev 时会 build 本地 runtime 镜像；ENV=test/prod 时会 pull server compose
    中声明的 worker 镜像。完成后默认执行一次 deploy。
    ENV=dev build 前会确保 Python 基础镜像已存在本地；不存在时才 pull。
    ENV=dev 重建后会按创建时间清理历史 runtime 镜像，只保留最近 3 个。

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
  if has_arg "--dry-run" "$@"; then
    compose_deploy run --rm --no-deps prefect-deploy python scripts/deploy_work_pool.py "$@"
    return
  fi

  compose_deploy run --rm prefect-deploy sh -c '
    prefect work-pool create default-process-pool --type process || true
    exec python scripts/deploy_work_pool.py "$@"
  ' sh "$@"
}

runtime_image_ref() {
  compose_deploy config | awk '
    /^  prefect-deploy:/ {
      in_service = 1
      next
    }
    in_service && /^  [^[:space:]][^:]*:/ {
      exit
    }
    in_service && /^[[:space:]]+image:/ {
      image = $2
      gsub(/^"|"$/, "", image)
      print image
      exit
    }
  '
}

python_base_image_ref() {
  compose_deploy config | awk '
    /^  prefect-deploy:/ {
      in_service = 1
      next
    }
    in_service && /^  [^[:space:]][^:]*:/ {
      exit
    }
    in_service && /^[[:space:]]+build:/ {
      in_build = 1
      next
    }
    in_service && in_build && /^[[:space:]]+args:/ {
      in_args = 1
      next
    }
    in_service && in_build && in_args && /^[[:space:]]+PYTHON_IMAGE:/ {
      sub(/^[[:space:]]+PYTHON_IMAGE:[[:space:]]*/, "")
      gsub(/^"|"$/, "")
      print
      exit
    }
  '
}

docker_image_id() {
  local image_ref="$1"

  docker image inspect "$image_ref" --format '{{.Id}}' 2>/dev/null || true
}

ensure_local_docker_image() {
  local image_ref="$1"

  [[ -n "$image_ref" ]] || die "镜像名不能为空。"
  if docker image inspect "$image_ref" >/dev/null 2>&1; then
    echo "Python 基础镜像已存在本地: $image_ref"
    return
  fi

  echo "本地未找到 Python 基础镜像，开始拉取: $image_ref"
  docker pull "$image_ref"
}

short_image_id() {
  local image_id="${1#sha256:}"

  printf '%s\n' "${image_id:0:12}"
}

remember_runtime_image() {
  local image_ref="$1"
  local required="${2:-false}"
  local image_id
  local tmp_file

  image_id="$(docker_image_id "$image_ref")"
  if [[ -z "$image_id" ]]; then
    [[ "$required" == "true" ]] && die "未找到 runtime 镜像: $image_ref"
    return
  fi

  mkdir -p "$(dirname "$RUNTIME_IMAGE_HISTORY_FILE")"
  tmp_file="${RUNTIME_IMAGE_HISTORY_FILE}.tmp.$$"
  {
    [[ -f "$RUNTIME_IMAGE_HISTORY_FILE" ]] && cat "$RUNTIME_IMAGE_HISTORY_FILE"
    printf '%s\n' "$image_id"
  } | awk 'NF && !seen[$0]++' >"$tmp_file"
  mv "$tmp_file" "$RUNTIME_IMAGE_HISTORY_FILE"
}

list_runtime_image_history() {
  local image_id
  local created

  [[ -f "$RUNTIME_IMAGE_HISTORY_FILE" ]] || return

  while IFS= read -r image_id; do
    [[ -n "$image_id" ]] || continue
    created="$(docker image inspect "$image_id" --format '{{.Created}}' 2>/dev/null || true)"
    [[ -n "$created" ]] && printf '%s\t%s\n' "$created" "$image_id"
  done <"$RUNTIME_IMAGE_HISTORY_FILE"
}

cleanup_runtime_image_history() {
  local runtime_image="${1:-}"
  local current_image_id=""
  local tmp_file="${RUNTIME_IMAGE_HISTORY_FILE}.tmp.$$"
  local kept_count=0
  local removed_count=0
  local skipped_count=0
  local created
  local image_id

  [[ -f "$RUNTIME_IMAGE_HISTORY_FILE" ]] || return

  : >"$tmp_file"
  if [[ -n "$runtime_image" ]]; then
    current_image_id="$(docker_image_id "$runtime_image")"
    if [[ -n "$current_image_id" ]]; then
      printf '%s\n' "$current_image_id" >>"$tmp_file"
      ((kept_count += 1))
    fi
  fi

  while IFS=$'\t' read -r created image_id; do
    [[ -n "$image_id" ]] || continue
    [[ "$image_id" == "$current_image_id" ]] && continue
    if ((kept_count < RUNTIME_IMAGE_HISTORY_KEEP)); then
      printf '%s\n' "$image_id" >>"$tmp_file"
      ((kept_count += 1))
      continue
    fi

    if docker image rm "$image_id" >/dev/null 2>&1; then
      echo "已删除历史 runtime 镜像: $(short_image_id "$image_id")"
      ((removed_count += 1))
    else
      echo "跳过无法删除的历史 runtime 镜像: $(short_image_id "$image_id")" >&2
      printf '%s\n' "$image_id" >>"$tmp_file"
      ((skipped_count += 1))
    fi
  done < <(list_runtime_image_history | sort -r | awk -F '\t' '!seen[$2]++')

  mv "$tmp_file" "$RUNTIME_IMAGE_HISTORY_FILE"
  echo "runtime 镜像历史清理完成：保留 ${kept_count} 个，删除 ${removed_count} 个，跳过 ${skipped_count} 个。"
}

print_update_plan() {
  local worker_services=("$@")
  local python_image=""

  echo "Dry run: update worker runtime"
  echo "ENV=$ENV_NAME"
  echo "Compose 文件: $COMPOSE_FILE"
  if [[ "$ENV_NAME" == "dev" ]]; then
    python_image="$(python_base_image_ref)"
    echo "将执行: 本地不存在时拉取 Python 基础镜像 ${python_image:-<无法解析>}"
    echo "将执行: docker compose build prefect-deploy ${worker_services[*]}"
    echo "将执行: 按创建时间清理历史 runtime 镜像，只保留最近 ${RUNTIME_IMAGE_HISTORY_KEEP} 个"
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
  local runtime_image=""
  local python_image=""
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
    python_image="$(python_base_image_ref)"
    [[ -n "$python_image" ]] || die "无法解析 Dockerfile 的 PYTHON_IMAGE 构建参数。"
    ensure_local_docker_image "$python_image"
    runtime_image="$(runtime_image_ref)"
    [[ -n "$runtime_image" ]] || die "无法解析 prefect-deploy 的 runtime 镜像名。"
    remember_runtime_image "$runtime_image"
    compose_deploy build prefect-deploy "${worker_services[@]}"
  else
    compose_deploy pull prefect-deploy "${worker_services[@]}"
  fi

  compose up -d --no-deps --force-recreate "${worker_services[@]}"

  if [[ "$ENV_NAME" == "dev" ]]; then
    remember_runtime_image "$runtime_image" true
    cleanup_runtime_image_history "$runtime_image"
  fi

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
