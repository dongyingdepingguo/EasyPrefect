# -*- coding: utf-8 -*-
"""从 Tushare Pro 获取 sw_daily 申万行业日线行情数据。"""

from __future__ import annotations

import datetime as dt
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
MAX_PAGE_SIZE = 4000

SW_DAILY_COLUMNS = (
    "ts_code",
    "trade_date",
    "name",
    "open",
    "low",
    "high",
    "close",
    "change",
    "pct_change",
    "vol",
    "amount",
    "pe",
    "pb",
    "float_mv",
    "total_mv",
)
NUMERIC_COLUMNS = tuple(
    column for column in SW_DAILY_COLUMNS if column not in {"ts_code", "trade_date", "name"}
)


def _clean_sw_daily_params(
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


def _create_table_sql(table: str) -> str:
    column_comments = {
        "open": "开盘点位",
        "low": "最低点位",
        "high": "最高点位",
        "close": "收盘点位",
        "change": "涨跌点",
        "pct_change": "涨跌幅",
        "vol": "成交量",
        "amount": "成交额",
        "pe": "市盈率",
        "pb": "市净率",
        "float_mv": "流通市值",
        "total_mv": "总市值",
    }
    columns = [
        f"{quote_identifier('ts_code')} String COMMENT '申万行业代码'",
        f"{quote_identifier('trade_date')} Date COMMENT '交易日期'",
        f"{quote_identifier('name')} String COMMENT '申万行业名称'",
    ]
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
COMMENT '申万行业日线行情'
""".strip()


def _ensure_sw_daily_table(config: ClickHouseWriteConfig) -> None:
    if not config.enabled:
        return
    config.validate_for_write()
    with ClickHouseClient() as client:
        client.command(_create_table_sql(config.table))


def _sw_daily_fields_from_clickhouse(config: ClickHouseWriteConfig) -> str:
    if not config.enabled:
        return ""

    with ClickHouseClient() as client:
        columns = client.table_columns(config.table)

    excluded_columns = {config.version_column, *METADATA_COLUMNS}
    fields = [column for column in columns if column not in excluded_columns]
    if not fields:
        raise ValueError(f"ClickHouse 表 {config.table} 未解析到可用于 Tushare 的字段")
    return ",".join(fields)


def _normalize_sw_daily_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    result = df.copy()
    for column in ("ts_code", "name"):
        if column in result.columns:
            result[column] = result[column].fillna("").astype(str)
    return result


def _query_sw_daily_page(
    pro: Any,
    *,
    ts_code: str,
    trade_date: str,
    start_date: str,
    end_date: str,
    limit: int,
    offset: int,
    fields: str = "",
) -> pd.DataFrame:
    params: dict[str, Any] = _clean_sw_daily_params(
        ts_code=ts_code,
        trade_date=trade_date,
        start_date=start_date,
        end_date=end_date,
    )
    params["limit"] = limit
    params["offset"] = offset
    if fields:
        df = pro.sw_daily(**params, fields=fields)
    else:
        df = pro.sw_daily(**params)
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"sw_daily 返回了非预期类型: {type(df)!r}，参数={params}")
    return df


def _query_sw_daily_all(
    pro: Any,
    *,
    ts_code: str,
    trade_date: str,
    start_date: str,
    end_date: str,
    fields: str = "",
    page_size: int = MAX_PAGE_SIZE,
) -> pd.DataFrame:
    if page_size <= 0 or page_size > MAX_PAGE_SIZE:
        raise ValueError(f"page_size 必须在 1 到 {MAX_PAGE_SIZE} 之间")

    frames: list[pd.DataFrame] = []
    empty_template: pd.DataFrame | None = None
    offset = 0
    while True:
        current_df = _query_sw_daily_page(
            pro,
            ts_code=ts_code,
            trade_date=trade_date,
            start_date=start_date,
            end_date=end_date,
            limit=page_size,
            offset=offset,
            fields=fields,
        )
        if current_df.empty:
            if empty_template is None:
                empty_template = current_df
            break

        frames.append(current_df)
        if len(current_df) < page_size:
            break
        offset += page_size

    if frames:
        df = pd.concat(frames, ignore_index=True).drop_duplicates(ignore_index=True)
        return _normalize_sw_daily_dataframe(df)
    if empty_template is not None:
        return _normalize_sw_daily_dataframe(empty_template)
    return pd.DataFrame()


@flow(name="Tushare 申万行业日线行情")
def tushare_sw_daily_flow(
    ts_code: str = "",
    trade_date: str = "",
    start_date: str = "",
    end_date: str = "",
    timeout: int = 30,
) -> None:
    """获取 Tushare sw_daily 申万行业日线行情数据，并写入 ClickHouse。

    参数:
        ts_code: 申万行业代码，例如 801010.SI；为空时不按行业过滤。
        trade_date: 交易日期，格式 YYYYMMDD；为空时默认使用运行当天。
        start_date: 交易起始日期，格式 YYYYMMDD；为空时不设置区间起点。
        end_date: 交易结束日期，格式 YYYYMMDD；为空时不设置区间终点。
        timeout: Tushare API 请求超时时间，单位为秒。
    """

    logger = get_run_logger()
    token = env_value("TUSHARE_TOKEN", default="") or ""
    pro = ts.pro_api(token=token, timeout=timeout)
    write_config = ClickHouseWriteConfig.from_mapping(
        module_runtime("tushare_sw_daily", "clickhouse")
    )
    _ensure_sw_daily_table(write_config)
    fields = _sw_daily_fields_from_clickhouse(write_config)
    trade_date = _default_trade_date(trade_date)
    params = _clean_sw_daily_params(
        ts_code=ts_code,
        trade_date=trade_date,
        start_date=start_date,
        end_date=end_date,
    )
    df = _query_sw_daily_all(
        pro,
        ts_code=ts_code,
        trade_date=trade_date,
        start_date=start_date,
        end_date=end_date,
        fields=fields,
    )

    logger.info(
        "已获取 %s 条 Tushare sw_daily 数据，参数=%s，字段=%r",
        len(df),
        params,
        list(df.columns),
    )
    written = write_dataframe(df, write_config)
    if write_config.enabled:
        logger.info(
            "已写入 %s 条 sw_daily 数据到 ClickHouse 表 %s，模式=%s",
            written,
            write_config.table,
            write_config.mode,
        )
