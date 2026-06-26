#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

REMOTE="${REMOTE:-origin}"
BRANCH="${BRANCH:-main}"
SERVER_COMPOSE_FILE="${SERVER_COMPOSE_FILE:-docker-compose.server.yml}"
REGISTRY_NAMESPACE="${REGISTRY_NAMESPACE:-192.168.2.212:5080/oak-quant/workflow-scheduling}"
RUNTIME_IMAGE="${RUNTIME_IMAGE:-$REGISTRY_NAMESPACE/easy-prefect-runtime}"
PYTHON_IMAGE="${PYTHON_IMAGE:-python:3.14-slim}"
EXPECTED_WORKER_IMAGE_DEFAULTS=2

die() {
    echo "$*" >&2
    exit 1
}

require_command() {
    local cmd="$1"

    command -v "$cmd" >/dev/null 2>&1 || die "未找到命令: $cmd"
}

require_git_repo() {
    git rev-parse --is-inside-work-tree >/dev/null 2>&1 || die "当前目录不是 git 仓库，无法按 tag 发布镜像。"
}

require_clean_worktree() {
    local status

    status="$(git status --porcelain)"
    [[ -z "$status" ]] || die "工作区存在未提交内容，请先提交或清理后再发布。"
}

require_main_branch() {
    local current_branch

    current_branch="$(git branch --show-current)"
    [[ "$current_branch" == "$BRANCH" ]] || die "当前分支是 $current_branch，只允许在 $BRANCH 分支发布镜像。"
}

fetch_release_refs() {
    git fetch --no-tags "$REMOTE" "+refs/heads/$BRANCH:refs/remotes/$REMOTE/$BRANCH"
}

require_branch_pushed() {
    local local_head remote_head

    local_head="$(git rev-parse HEAD)"
    remote_head="$(git rev-parse "$REMOTE/$BRANCH")"

    [[ "$local_head" == "$remote_head" ]] || die "本地 HEAD 与 $REMOTE/$BRANCH 不一致，请先同步并推送到远端后再发布。"
}

get_head_tag() {
    local tags=()

    mapfile -t tags < <(git tag --points-at HEAD)

    if (( ${#tags[@]} == 0 )); then
        die "当前最新提交没有 tag，禁止生成镜像。"
    fi

    if (( ${#tags[@]} > 1 )); then
        printf '当前最新提交存在多个 tag，发布语义不明确：\n' >&2
        printf '- %s\n' "${tags[@]}" >&2
        die "请保留一个明确的发布 tag 后再执行。"
    fi

    printf '%s\n' "${tags[0]}"
}

require_valid_image_tag() {
    local image_tag="$1"

    [[ ${#image_tag} -le 128 ]] || die "Docker image tag 超过 128 个字符: $image_tag"
    [[ "$image_tag" =~ ^[A-Za-z0-9_][A-Za-z0-9_.-]*$ ]] || die "Git tag 生成的 Docker image tag 不合法: $image_tag"
}

require_tag_pushed() {
    local git_tag="$1"
    local head_commit="$2"
    local remote_lines remote_commit

    remote_lines="$(git ls-remote --tags "$REMOTE" "refs/tags/$git_tag" "refs/tags/$git_tag^{}")"
    [[ -n "$remote_lines" ]] || die "tag $git_tag 未推送到远端 $REMOTE。"

    remote_commit="$(
        printf '%s\n' "$remote_lines" | awk '$2 ~ /\^\{\}$/ { print $1; found = 1; exit } END { if (!found) exit 1 }'
    )" || remote_commit="$(
        printf '%s\n' "$remote_lines" | awk '$2 !~ /\^\{\}$/ { print $1; exit }'
    )"

    [[ "$remote_commit" == "$head_commit" ]] || die "远端 tag $git_tag 未指向当前 HEAD，禁止生成镜像。"
}

preflight_release() {
    require_command git
    require_command docker
    require_command awk
    require_command mktemp
    require_command chmod

    require_git_repo
    [[ -f "$SERVER_COMPOSE_FILE" ]] || die "未找到部署文件: $SERVER_COMPOSE_FILE"

    require_main_branch
    require_clean_worktree
    fetch_release_refs
    require_branch_pushed
}

build_and_push_runtime_image() {
    local image_ref="$1"

    echo "开始构建 runtime 镜像..."
    docker build --target runtime --build-arg "PYTHON_IMAGE=$PYTHON_IMAGE" -t "$image_ref" .
    docker image inspect "$image_ref" >/dev/null 2>&1 || die "镜像构建完成后未找到: $image_ref"

    echo "开始推送镜像: $image_ref"
    docker push "$image_ref"
}

update_compose_images() {
    local runtime_image_ref="$1"
    local tmp_file

    tmp_file="$(mktemp "${SERVER_COMPOSE_FILE}.tmp.XXXXXX")"
    chmod --reference="$SERVER_COMPOSE_FILE" "$tmp_file"

    if awk \
        -v runtime_image_ref="$runtime_image_ref" \
        -v expected_worker="$EXPECTED_WORKER_IMAGE_DEFAULTS" '
        BEGIN {
            worker_pattern = "[$][{]EASYPREFECT_WORKER_IMAGE:-[^}]+[}]"
            worker_replacement = "${EASYPREFECT_WORKER_IMAGE:-" runtime_image_ref "}"
        }
        {
            line = $0
            if (line ~ worker_pattern) {
                gsub(worker_pattern, worker_replacement, line)
                worker_count++
            }
            print line
        }
        END {
            if (worker_count != expected_worker) {
                printf "%s 中 EASYPREFECT_WORKER_IMAGE 默认值数量异常: 期望 %d，实际 %d\n", FILENAME, expected_worker, worker_count > "/dev/stderr"
                exit 42
            }
        }
    ' "$SERVER_COMPOSE_FILE" > "$tmp_file"; then
        :
    else
        local rc=$?
        rm -f "$tmp_file"

        if [[ "$rc" == "42" ]]; then
            die "$SERVER_COMPOSE_FILE 镜像默认值结构与预期不一致，已停止发布。"
        fi

        die "更新 $SERVER_COMPOSE_FILE 失败。"
    fi

    mv "$tmp_file" "$SERVER_COMPOSE_FILE"
}

commit_and_push_compose() {
    local image_tag="$1"

    git add "$SERVER_COMPOSE_FILE"

    if git diff --cached --quiet -- "$SERVER_COMPOSE_FILE"; then
        git commit --allow-empty -m "chore: release runtime image $image_tag"
    else
        git commit -m "chore: release runtime image $image_tag"
    fi

    git push "$REMOTE" "$BRANCH"
}

main() {
    local git_tag commit_short head_commit image_tag runtime_image_ref

    preflight_release

    git_tag="$(get_head_tag)"
    head_commit="$(git rev-parse HEAD)"
    commit_short="$(git rev-parse --short=8 HEAD)"
    image_tag="${git_tag}-${commit_short}"
    runtime_image_ref="${RUNTIME_IMAGE}:${image_tag}"

    require_valid_image_tag "$image_tag"
    require_tag_pushed "$git_tag" "$head_commit"

    echo "========================================"
    echo "发布分支:   $BRANCH"
    echo "远端分支:   $REMOTE/$BRANCH"
    echo "git tag:    $git_tag"
    echo "commit:     $commit_short"
    echo "runtime:    $runtime_image_ref"
    echo "Python:     $PYTHON_IMAGE"
    echo "部署文件:   $SERVER_COMPOSE_FILE"
    echo "========================================"

    build_and_push_runtime_image "$runtime_image_ref"

    fetch_release_refs
    require_branch_pushed

    echo "更新 $SERVER_COMPOSE_FILE 的镜像默认值..."
    update_compose_images "$runtime_image_ref"

    echo "提交并推送部署文件变更..."
    commit_and_push_compose "$image_tag"

    echo "发布完成:"
    echo "- $runtime_image_ref"
}

main "$@"
