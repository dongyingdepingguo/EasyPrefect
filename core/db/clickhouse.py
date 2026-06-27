# -*- coding: utf-8 -*-
"""Small ClickHouse access layer for scheduled data jobs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Self

import pandas as pd

from core.settings import as_bool, config_int, config_value, env_value


def _env_bool_or_config(env_key: str, config_path: str, default: bool) -> bool:
    value = env_value(env_key)
    if value is None:
        value = config_value(env_key, config_path, default)
    return as_bool(value, env_key) if not isinstance(value, bool) else value


def quote_identifier(identifier: str) -> str:
    """Quote one ClickHouse identifier component with backticks."""
    normalized = identifier.strip()
    if not normalized:
        raise ValueError("ClickHouse identifier cannot be empty")
    return f"`{normalized.replace('`', '``')}`"


def quote_table_name(table: str) -> str:
    """Quote a ClickHouse table name, allowing database.table notation."""
    parts = [part.strip() for part in table.split(".")]
    if not parts or any(not part for part in parts):
        raise ValueError(f"Invalid ClickHouse table name: {table!r}")
    return ".".join(quote_identifier(part) for part in parts)


def _split_insert_table(table: str) -> tuple[str, str | None]:
    """Split optional database.table notation for clickhouse-connect insert APIs."""
    parts = [part.strip() for part in table.split(".")]
    if len(parts) == 1 and parts[0]:
        return parts[0], None
    if len(parts) == 2 and all(parts):
        return parts[1], parts[0]
    raise ValueError(f"Invalid ClickHouse table name: {table!r}")


@dataclass(frozen=True)
class ClickHouseConfig:
    """Connection settings loaded from env vars first, then config.yaml."""

    host: str = "localhost"
    port: int = 8123
    username: str = "default"
    password: str = ""
    database: str = "default"
    secure: bool = False
    connect_timeout: int = 10
    send_receive_timeout: int = 300

    @classmethod
    def from_settings(cls) -> Self:
        """Build ClickHouse config from project settings."""
        return cls(
            host=str(config_value("CLICKHOUSE_HOST", "clickhouse.host", "localhost")),
            port=int(
                env_value("CLICKHOUSE_HTTP_PORT", "CLICKHOUSE_PORT")
                or config_int("CLICKHOUSE_HTTP_PORT", "clickhouse.port", 8123)
            ),
            username=str(config_value("CLICKHOUSE_USER", "clickhouse.username", "default")),
            password=str(config_value("CLICKHOUSE_PASSWORD", "clickhouse.password", "")),
            database=str(config_value("CLICKHOUSE_DB", "clickhouse.database", "default")),
            secure=_env_bool_or_config("CLICKHOUSE_SECURE", "clickhouse.secure", False),
            connect_timeout=int(
                env_value("CLICKHOUSE_CONNECT_TIMEOUT")
                or config_int("CLICKHOUSE_CONNECT_TIMEOUT", "clickhouse.connect_timeout", 10)
            ),
            send_receive_timeout=int(
                env_value("CLICKHOUSE_SEND_RECEIVE_TIMEOUT")
                or config_int(
                    "CLICKHOUSE_SEND_RECEIVE_TIMEOUT",
                    "clickhouse.send_receive_timeout",
                    300,
                )
            ),
        )


class ClickHouseClient:
    """Thin wrapper around clickhouse-connect with project defaults."""

    def __init__(self, config: ClickHouseConfig | None = None) -> None:
        self.config = config or ClickHouseConfig.from_settings()
        self._client: Any | None = None

    def __enter__(self) -> Self:
        self.connect()
        return self

    def __exit__(self, *_exc_info: object) -> None:
        self.close()

    @property
    def raw_client(self) -> Any:
        """Return the underlying clickhouse-connect client, connecting lazily."""
        return self.connect()

    def connect(self) -> Any:
        """Create and cache a clickhouse-connect client."""
        if self._client is None:
            import clickhouse_connect

            self._client = clickhouse_connect.get_client(
                host=self.config.host,
                port=self.config.port,
                username=self.config.username,
                password=self.config.password,
                database=self.config.database,
                secure=self.config.secure,
                connect_timeout=self.config.connect_timeout,
                send_receive_timeout=self.config.send_receive_timeout,
            )
        return self._client

    def close(self) -> None:
        """Close the underlying client when supported by the driver."""
        if self._client is None:
            return
        close = getattr(self._client, "close", None)
        if callable(close):
            close()
        self._client = None

    def ping(self) -> bool:
        """Run a cheap server-side query to validate connectivity."""
        return self.command("SELECT 1") == 1

    def command(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        settings: Mapping[str, Any] | None = None,
    ) -> Any:
        """Execute DDL or a command-style SQL statement."""
        return self.raw_client.command(sql, parameters=parameters, settings=settings)

    def query_rows(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        settings: Mapping[str, Any] | None = None,
    ) -> list[tuple[Any, ...]]:
        """Execute a query and return raw result rows."""
        result = self.raw_client.query(sql, parameters=parameters, settings=settings)
        return list(result.result_rows)

    def query_records(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        settings: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Execute a query and return rows as dictionaries."""
        result = self.raw_client.query(sql, parameters=parameters, settings=settings)
        columns = list(result.column_names)
        return [dict(zip(columns, row, strict=True)) for row in result.result_rows]

    def query_df(
        self,
        sql: str,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        *,
        settings: Mapping[str, Any] | None = None,
    ) -> pd.DataFrame:
        """Execute a query and return a pandas DataFrame."""
        return self.raw_client.query_df(sql, parameters=parameters, settings=settings)

    def select_records(
        self,
        table: str,
        *,
        columns: Sequence[str] | None = None,
        where: str | None = None,
        limit: int | None = None,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Run a simple SELECT against a table."""
        column_sql = "*"
        if columns:
            column_sql = ", ".join(quote_identifier(column) for column in columns)

        sql = f"SELECT {column_sql} FROM {quote_table_name(table)}"
        if where:
            sql = f"{sql} WHERE {where}"
        if limit is not None:
            if limit < 0:
                raise ValueError("limit must be greater than or equal to 0")
            sql = f"{sql} LIMIT {limit}"
        return self.query_records(sql, parameters=parameters)

    def table_columns(self, table: str) -> list[str]:
        """Return column names for a ClickHouse table in physical order."""
        rows = self.query_records(f"DESCRIBE TABLE {quote_table_name(table)}")
        return [str(row["name"]) for row in rows if row.get("name")]

    def insert_records(
        self,
        table: str,
        records: Sequence[Mapping[str, Any]],
        *,
        columns: Sequence[str] | None = None,
    ) -> int:
        """Insert dictionaries into a ClickHouse table."""
        if not records:
            return 0

        column_names = list(columns or records[0].keys())
        data = [[record.get(column) for column in column_names] for record in records]
        table_name, database = _split_insert_table(table)
        self.raw_client.insert(
            table_name,
            data,
            column_names=column_names,
            database=database,
        )
        return len(records)

    def insert_df(
        self,
        table: str,
        df: pd.DataFrame,
        *,
        columns: Sequence[str] | None = None,
    ) -> int:
        """Insert a pandas DataFrame into a ClickHouse table."""
        if df.empty:
            return 0

        insert_df = df.loc[:, list(columns)] if columns else df
        insert_df = _normalize_dataframe(insert_df)
        table_name, database = _split_insert_table(table)
        self.raw_client.insert_df(table_name, insert_df, database=database)
        return len(insert_df)

    def update_where(
        self,
        table: str,
        set_expression: str,
        where: str,
        *,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        sync: bool = False,
    ) -> Any:
        """Run an ALTER UPDATE mutation. Prefer upserts for routine jobs."""
        settings = {"mutations_sync": 1} if sync else None
        sql = f"ALTER TABLE {quote_table_name(table)} UPDATE {set_expression} WHERE {where}"
        return self.command(sql, parameters=parameters, settings=settings)

    def delete_where(
        self,
        table: str,
        where: str,
        *,
        parameters: Mapping[str, Any] | Sequence[Any] | None = None,
        sync: bool = False,
    ) -> Any:
        """Run an ALTER DELETE mutation. Prefer partition replacement when possible."""
        settings = {"mutations_sync": 1} if sync else None
        sql = f"ALTER TABLE {quote_table_name(table)} DELETE WHERE {where}"
        return self.command(sql, parameters=parameters, settings=settings)


def _normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Convert pandas missing values to None for ClickHouse inserts."""
    return df.astype(object).where(pd.notna(df), None)


@contextmanager
def clickhouse_client(config: ClickHouseConfig | None = None) -> Any:
    """Context manager that yields a project ClickHouse client wrapper."""
    client = ClickHouseClient(config)
    try:
        yield client
    finally:
        client.close()
