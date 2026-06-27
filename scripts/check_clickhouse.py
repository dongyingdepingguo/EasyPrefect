"""Check ClickHouse connectivity using project settings."""

from __future__ import annotations

import sys
from pathlib import Path

_PROJECT_ROOT_FOR_IMPORT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT_FOR_IMPORT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT_FOR_IMPORT))

from core.db import ClickHouseClient


def main() -> None:
    """Print a small, non-sensitive ClickHouse connectivity summary."""
    with ClickHouseClient() as client:
        rows = client.query_records(
            "SELECT version() AS version, currentDatabase() AS database"
        )
    row = rows[0]
    print(f"ClickHouse OK version={row['version']} database={row['database']}")


if __name__ == "__main__":
    main()
