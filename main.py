"""Small local entrypoint for checking the EasyPrefect configuration."""

from __future__ import annotations

from core.settings import current_env, root_config


def main() -> None:
    """Print the active environment and enabled modules."""
    config = root_config()
    deploy_config = config.get("deploy", {})
    enabled_modules = deploy_config.get("enabled_modules", []) if isinstance(deploy_config, dict) else []
    modules_text = ", ".join(str(item) for item in enabled_modules) or "(none)"

    print(f"ENV={current_env() or '(default)'}")
    print(f"enabled_modules={modules_text}")


if __name__ == "__main__":
    main()
