# -*- coding: utf-8 -*-
"""从 Tushare Pro 获取 index_daily 指数日线行情数据。"""

from __future__ import annotations

import datetime as dt
import time
from collections.abc import Mapping, Sequence
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import tushare as ts
from prefect import flow, get_run_logger

from core.db import ClickHouseClient, ClickHouseWriteConfig, write_dataframe
from core.db.clickhouse import quote_identifier, quote_table_name
from core.settings import env_value, module_runtime

METADATA_COLUMNS = {"created_at"}
DEFAULT_TIMEZONE = ZoneInfo("Asia/Shanghai")
DEFAULT_INDEX_BASIC_TABLE = "index_base_basic"
DEFAULT_MARKETS = ("CSI", "SSE", "SZSE")
INDEX_DAILY_MAX_REQUESTS_PER_MINUTE = 500
INDEX_DAILY_REQUEST_INTERVAL_SECONDS = 60 / INDEX_DAILY_MAX_REQUESTS_PER_MINUTE

INDEX_DAILY_COLUMNS = (
    "ts_code",
    "trade_date",
    "close",
    "open",
    "high",
    "low",
    "pre_close",
    "change",
    "pct_chg",
    "vol",
    "amount",
)
NUMERIC_COLUMNS = tuple(
    column for column in INDEX_DAILY_COLUMNS if column not in {"ts_code", "trade_date"}
)


def _clean_index_daily_params(
    *,
    ts_code: str = "",
    trade_date: str = "",
    start_date: str = "",
    end_date: str = "",
) -> dict[str, str]:
    params = {
        "ts_code": ts_code,
        "trade_date": trade_date,
        "start_date": start_date,
        "end_date": end_date,
    }
    return {
        key: value.strip()
        for key, value in params.items()
        if value is not None and value.strip()
    }


def _default_trade_date(trade_date: str | None) -> str:
    if trade_date and trade_date.strip():
        return trade_date.strip()
    return dt.datetime.now(DEFAULT_TIMEZONE).date().strftime("%Y%m%d")


def _normalize_markets(value: Any) -> tuple[str, ...]:
    if value in (None, ""):
        return DEFAULT_MARKETS
    if isinstance(value, str):
        markets = [item.strip() for item in value.split(",")]
    elif isinstance(value, Sequence):
        markets = [str(item).strip() for item in value]
    else:
        raise ValueError("tushare_index_daily.runtime.source.markets 必须是列表或逗号分隔字符串")

    normalized = tuple(market for market in markets if market)
    return normalized or DEFAULT_MARKETS


def _source_config(runtime_config: Mapping[str, Any]) -> tuple[str, tuple[str, ...]]:
    source = runtime_config.get("source", {}) or {}
    if not isinstance(source, Mapping):
        raise ValueError("tushare_index_daily.runtime.source 必须是 YAML 映射结构")

    table = str(source.get("index_basic_table", DEFAULT_INDEX_BASIC_TABLE)).strip()
    if not table:
        table = DEFAULT_INDEX_BASIC_TABLE
    markets = _normalize_markets(source.get("markets", DEFAULT_MARKETS))
    return table, markets


def _create_table_sql(table: str) -> str:
    columns = [
        f"{quote_identifier('ts_code')} String COMMENT 'TS指数代码'",
        f"{quote_identifier('trade_date')} Date COMMENT '交易日期'",
    ]
    column_comments = {
        "close": "收盘点位",
        "open": "开盘点位",
        "high": "最高点位",
        "low": "最低点位",
        "pre_close": "昨日收盘点位",
        "change": "涨跌点",
        "pct_chg": "涨跌幅",
        "vol": "成交量",
        "amount": "成交额",
    }
    columns.extend(
        f"{quote_identifier(column)} Nullable(Float64) COMMENT '{column_comments[column]}'"
        for column in NUMERIC_COLUMNS
    )
    columns.extend(
        [
            f"{quote_identifier('created_at')} DateTime64(3) DEFAULT now64(3) "
            "COMMENT '创建时间'",
            f"{quote_identifier('updated_at')} DateTime64(3) DEFAULT now64(3) "
            "COMMENT '更新时间'",
        ]
    )
    column_sql = ",\n    ".join(columns)
    table_sql = quote_table_name(table)
    return f"""
CREATE TABLE IF NOT EXISTS {table_sql}
(
    {column_sql}
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(trade_date)
ORDER BY (trade_date, ts_code)
SETTINGS index_granularity = 8192
COMMENT '指数日线行情'
""".strip()


def _ensure_index_daily_table(config: ClickHouseWriteConfig) -> None:
    if not config.enabled:
        return
    config.validate_for_write()
    with ClickHouseClient() as client:
        client.command(_create_table_sql(config.table))


def _index_daily_fields_from_clickhouse(config: ClickHouseWriteConfig) -> str:
    if not config.enabled:
        return ""

    with ClickHouseClient() as client:
        columns = client.table_columns(config.table)

    excluded_columns = {config.version_column, *METADATA_COLUMNS}
    fields = [column for column in columns if column not in excluded_columns]
    if not fields:
        raise ValueError(f"ClickHouse 表 {config.table} 未解析到可用于 Tushare 的字段")
    return ",".join(fields)


def _quote_sql_string(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _load_index_ts_codes(table: str, markets: Sequence[str]) -> list[str]:
    if not markets:
        return []

    market_sql = ", ".join(_quote_sql_string(market) for market in markets)
    sql = f"""
SELECT DISTINCT {quote_identifier('ts_code')}
FROM {quote_table_name(table)}
WHERE {quote_identifier('market')} IN ({market_sql})
  AND {quote_identifier('ts_code')} != ''
ORDER BY {quote_identifier('ts_code')}
""".strip()
    with ClickHouseClient() as client:
        rows = client.query_rows(sql)
    return [str(row[0]).strip() for row in rows if row and str(row[0]).strip()]


def _query_index_daily(
    pro: Any,
    *,
    ts_code: str,
    trade_date: str,
    start_date: str,
    end_date: str,
    fields: str = "",
) -> pd.DataFrame:
    params = _clean_index_daily_params(
        ts_code=ts_code,
        trade_date=trade_date,
        start_date=start_date,
        end_date=end_date,
    )
    if fields:
        df = pro.index_daily(**params, fields=fields)
    else:
        df = pro.index_daily(**params)
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"index_daily 返回了非预期类型: {type(df)!r}，参数={params}")
    return df.drop_duplicates(ignore_index=True)


def _wait_for_index_daily_rate_limit(last_request_at: float | None) -> float:
    if last_request_at is not None:
        elapsed = time.monotonic() - last_request_at
        remaining = INDEX_DAILY_REQUEST_INTERVAL_SECONDS - elapsed
        if remaining > 0:
            time.sleep(remaining)
    return time.monotonic()


@flow(name="Tushare 指数日线行情")
def tushare_index_daily_flow(
    trade_date: str = "",
    start_date: str = "",
    end_date: str = "",
    timeout: int = 30,
) -> None:
    """获取 Tushare index_daily 指数日线行情数据，并写入 ClickHouse。

    参数:
        trade_date: 交易日期，格式 YYYYMMDD；为空时默认使用运行当天。
        start_date: 交易起始日期，格式 YYYYMMDD；为空时不设置区间起点。
        end_date: 交易结束日期，格式 YYYYMMDD；为空时不设置区间终点。
        timeout: Tushare API 请求超时时间，单位为秒。
    """

    logger = get_run_logger()
    runtime_config = module_runtime("tushare_index_daily")
    index_basic_table, markets = _source_config(runtime_config)
    write_config = ClickHouseWriteConfig.from_mapping(runtime_config.get("clickhouse"))

    _ensure_index_daily_table(write_config)
    fields = _index_daily_fields_from_clickhouse(write_config)
    trade_date = _default_trade_date(trade_date)
    ts_codes = _load_index_ts_codes(index_basic_table, markets)
    params = _clean_index_daily_params(
        trade_date=trade_date,
        start_date=start_date,
        end_date=end_date,
    )

    logger.info(
        "已从 ClickHouse 表 %s 获取 %s 个指数代码，markets=%s",
        index_basic_table,
        len(ts_codes),
        list(markets),
    )

    token = env_value("TUSHARE_TOKEN", default="") or ""
    pro = ts.pro_api(token=token, timeout=timeout)
    frames: list[pd.DataFrame] = []
    empty_template: pd.DataFrame | None = None
    last_request_at: float | None = None
    for ts_code in ts_codes:
        last_request_at = _wait_for_index_daily_rate_limit(last_request_at)
        current_df = _query_index_daily(
            pro,
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            fields=fields,
        )
        if current_df.empty:
            if empty_template is None:
                empty_template = current_df
        else:
            frames.append(current_df)

    if frames:
        df = pd.concat(frames, ignore_index=True).drop_duplicates(ignore_index=True)
    elif empty_template is not None:
        df = empty_template
    else:
        df = pd.DataFrame()

    logger.info(
        "已获取 %s 条 Tushare index_daily 数据，参数=%s，字段=%r",
        len(df),
        params,
        list(df.columns),
    )
    written = write_dataframe(df, write_config)
    if write_config.enabled:
        logger.info(
            "已写入 %s 条 index_daily 数据到 ClickHouse 表 %s，模式=%s",
            written,
            write_config.table,
            write_config.mode,
        )
