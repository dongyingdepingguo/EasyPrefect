# -*- coding: utf-8 -*-
"""从 Tushare Pro 获取 share_float 股票每日限售股解禁数据。"""

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

SHARE_FLOAT_COLUMNS = (
    "ts_code",
    "ann_date",
    "float_date",
    "float_share",
    "float_ratio",
    "holder_name",
    "share_type",
)
DATE_COLUMNS = {"ann_date", "float_date"}
NUMERIC_COLUMNS = {"float_share", "float_ratio"}
STRING_COLUMNS = tuple(
    column
    for column in SHARE_FLOAT_COLUMNS
    if column not in DATE_COLUMNS and column not in NUMERIC_COLUMNS
)


def _clean_share_float_params(
    *,
    ts_code: str = "",
    ann_date: str = "",
    float_date: str = "",
    start_date: str = "",
    end_date: str = "",
) -> dict[str, str]:
    params = {
        "ts_code": ts_code,
        "ann_date": ann_date,
        "float_date": float_date,
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
    table_sql = quote_table_name(table)
    return f"""
CREATE TABLE IF NOT EXISTS {table_sql}
(
    {quote_identifier('ts_code')} String COMMENT 'TS股票代码',
    {quote_identifier('ann_date')} Nullable(Date) COMMENT '公告日期',
    {quote_identifier('float_date')} Date COMMENT '解禁日期',
    {quote_identifier('float_share')} Nullable(Float64) COMMENT '解禁股份数量',
    {quote_identifier('float_ratio')} Nullable(Float64) COMMENT '解禁股份占总股本比例',
    {quote_identifier('holder_name')} String COMMENT '股东名称',
    {quote_identifier('share_type')} String COMMENT '股份类型',
    {quote_identifier('created_at')} DateTime64(3) DEFAULT now64(3) COMMENT '创建时间',
    {quote_identifier('updated_at')} DateTime64(3) DEFAULT now64(3) COMMENT '更新时间'
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(float_date)
ORDER BY (float_date, ts_code, holder_name, share_type)
SETTINGS index_granularity = 8192
COMMENT '股票每日限售股解禁'
""".strip()


def _ensure_share_float_table(config: ClickHouseWriteConfig) -> None:
    if not config.enabled:
        return
    config.validate_for_write()
    with ClickHouseClient() as client:
        client.command(_create_table_sql(config.table))


def _share_float_fields_from_clickhouse(config: ClickHouseWriteConfig) -> str:
    if not config.enabled:
        return ""

    with ClickHouseClient() as client:
        columns = client.table_columns(config.table)

    excluded_columns = {config.version_column, *METADATA_COLUMNS}
    fields = [column for column in columns if column not in excluded_columns]
    if not fields:
        raise ValueError(f"ClickHouse 表 {config.table} 未解析到可用于 Tushare 的字段")
    return ",".join(fields)


def _normalize_share_float_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    result = df.copy()
    for column in STRING_COLUMNS:
        if column in result.columns:
            result[column] = result[column].fillna("").astype(str)
    return result


def _query_share_float(
    pro: Any,
    *,
    ts_code: str,
    ann_date: str,
    float_date: str,
    start_date: str,
    end_date: str,
    fields: str = "",
) -> pd.DataFrame:
    params = _clean_share_float_params(
        ts_code=ts_code,
        ann_date=ann_date,
        float_date=float_date,
        start_date=start_date,
        end_date=end_date,
    )
    if fields:
        df = pro.share_float(**params, fields=fields)
    else:
        df = pro.share_float(**params)
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"share_float 返回了非预期类型: {type(df)!r}，参数={params}")
    return _normalize_share_float_dataframe(df.drop_duplicates(ignore_index=True))


@flow(name="Tushare 股票每日限售股解禁")
def tushare_share_float_flow(
    ts_code: str = "",
    trade_date: str = "",
    ann_date: str = "",
    start_date: str = "",
    end_date: str = "",
    timeout: int = 30,
) -> None:
    """获取 Tushare share_float 股票每日限售股解禁数据，并写入 ClickHouse。

    参数:
        ts_code: 股票 TS 代码，例如 000001.SZ；为空时不按股票过滤。
        trade_date: 解禁日期，格式 YYYYMMDD；为空时默认使用运行当天。
        ann_date: 公告日期，格式 YYYYMMDD；为空时不按公告日期过滤。
        start_date: 解禁起始日期，格式 YYYYMMDD；为空时不设置区间起点。
        end_date: 解禁结束日期，格式 YYYYMMDD；为空时不设置区间终点。
        timeout: Tushare API 请求超时时间，单位为秒。
    """

    logger = get_run_logger()
    token = env_value("TUSHARE_TOKEN", default="") or ""
    pro = ts.pro_api(token=token, timeout=timeout)
    write_config = ClickHouseWriteConfig.from_mapping(
        module_runtime("tushare_share_float", "clickhouse")
    )
    _ensure_share_float_table(write_config)
    fields = _share_float_fields_from_clickhouse(write_config)
    trade_date = _default_trade_date(trade_date)
    params = _clean_share_float_params(
        ts_code=ts_code,
        ann_date=ann_date,
        float_date=trade_date,
        start_date=start_date,
        end_date=end_date,
    )
    df = _query_share_float(
        pro,
        ts_code=ts_code,
        ann_date=ann_date,
        float_date=trade_date,
        start_date=start_date,
        end_date=end_date,
        fields=fields,
    )

    logger.info(
        "已获取 %s 条 Tushare share_float 数据，参数=%s，字段=%r",
        len(df),
        params,
        list(df.columns),
    )
    written = write_dataframe(df, write_config)
    if write_config.enabled:
        logger.info(
            "已写入 %s 条 share_float 数据到 ClickHouse 表 %s，模式=%s",
            written,
            write_config.table,
            write_config.mode,
        )
