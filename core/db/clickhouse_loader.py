# -*- coding: utf-8 -*-
"""Config-driven ClickHouse write helpers for Prefect flows."""

from __future__ import annotations

import datetime as dt
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal, Self

import pandas as pd

from core.db.clickhouse import ClickHouseClient
from core.settings import as_bool

WriteMode = Literal["append", "upsert", "replace_where"]


@dataclass(frozen=True)
class ClickHouseWriteConfig:
    """Per-module ClickHouse write policy."""

    enabled: bool = False
    table: str = ""
    mode: WriteMode = "append"
    columns: tuple[str, ...] = field(default_factory=tuple)
    date_columns: tuple[str, ...] = field(default_factory=tuple)
    version_column: str = ""
    replace_where: str = ""
    mutations_sync: bool = True

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> Self:
        """Parse runtime.clickhouse mapping from a module deploy.yaml."""
        data = dict(value or {})
        mode = str(data.get("mode", "append")).strip() or "append"
        if mode not in {"append", "upsert", "replace_where"}:
            raise ValueError(f"Unsupported ClickHouse write mode: {mode!r}")

        columns = data.get("columns", ()) or ()
        if isinstance(columns, str):
            columns = [item.strip() for item in columns.split(",") if item.strip()]

        date_columns = data.get("date_columns", ()) or ()
        if isinstance(date_columns, str):
            date_columns = [item.strip() for item in date_columns.split(",") if item.strip()]

        enabled = data.get("enabled", False)
        mutations_sync = data.get("mutations_sync", True)

        return cls(
            enabled=as_bool(enabled, "runtime.clickhouse.enabled"),
            table=str(data.get("table", "")).strip(),
            mode=mode,  # type: ignore[arg-type]
            columns=tuple(str(column).strip() for column in columns if str(column).strip()),
            date_columns=tuple(
                str(column).strip() for column in date_columns if str(column).strip()
            ),
            version_column=str(data.get("version_column", "") or "").strip(),
            replace_where=str(data.get("replace_where", "") or "").strip(),
            mutations_sync=as_bool(mutations_sync, "runtime.clickhouse.mutations_sync"),
        )

    def validate_for_write(self) -> None:
        """Validate the config before a write operation."""
        if not self.enabled:
            return
        if not self.table:
            raise ValueError("ClickHouse write config requires table when enabled=true")
        if self.mode == "replace_where" and not self.replace_where:
            raise ValueError("ClickHouse replace_where mode requires replace_where SQL")


def write_records(
    records: Sequence[Mapping[str, Any]],
    config: ClickHouseWriteConfig,
    *,
    client: ClickHouseClient | None = None,
) -> int:
    """Write dictionaries to ClickHouse using the configured policy."""
    if not config.enabled or not records:
        return 0
    return write_dataframe(pd.DataFrame.from_records(records), config, client=client)


def write_dataframe(
    df: pd.DataFrame,
    config: ClickHouseWriteConfig,
    *,
    client: ClickHouseClient | None = None,
) -> int:
    """Write a DataFrame to ClickHouse using append/upsert/replace_where."""
    config.validate_for_write()
    if not config.enabled or df.empty:
        return 0

    insert_df = _prepare_dataframe(df, config)
    owns_client = client is None
    active_client = client or ClickHouseClient()
    try:
        if config.mode == "replace_where":
            active_client.delete_where(
                config.table,
                config.replace_where,
                sync=config.mutations_sync,
            )

        return active_client.insert_df(
            config.table,
            insert_df,
            columns=config.columns or None,
        )
    finally:
        if owns_client:
            active_client.close()


def _prepare_dataframe(df: pd.DataFrame, config: ClickHouseWriteConfig) -> pd.DataFrame:
    """Add metadata columns required by the configured write strategy."""
    insert_df = df.copy()
    if config.mode == "upsert" and config.version_column:
        if config.version_column not in insert_df.columns:
            insert_df[config.version_column] = dt.datetime.now(dt.UTC)

    for column in config.date_columns:
        if column not in insert_df.columns:
            raise ValueError(f"DataFrame missing ClickHouse date column: {column}")
        insert_df[column] = insert_df[column].map(_as_date)

    if config.columns:
        missing_columns = [column for column in config.columns if column not in insert_df.columns]
        if missing_columns:
            raise ValueError(f"DataFrame missing ClickHouse columns: {missing_columns}")
        insert_df = insert_df.loc[:, list(config.columns)]

    return insert_df


def _as_date(value: Any) -> dt.date | None:
    """Convert common date values from data APIs into Python date objects."""
    if value is None or pd.isna(value):
        return None
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value

    if isinstance(value, int):
        text = str(value)
    elif isinstance(value, float) and value.is_integer():
        text = str(int(value))
    else:
        text = str(value).strip()

    if not text:
        return None
    if len(text) == 8 and text.isdigit():
        return dt.datetime.strptime(text, "%Y%m%d").date()
    return pd.Timestamp(text).date()
