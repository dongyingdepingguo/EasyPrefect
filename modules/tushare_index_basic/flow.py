# -*- coding: utf-8 -*-
"""从 Tushare Pro 获取 index_basic 指数基本信息数据。"""

from __future__ import annotations

from typing import Any

import pandas as pd
import tushare as ts
from prefect import flow, get_run_logger

from core.db import ClickHouseClient, ClickHouseWriteConfig, write_dataframe
from core.db.clickhouse import quote_identifier, quote_table_name
from core.settings import env_value, module_runtime

METADATA_COLUMNS = {"created_at"}
DEFAULT_MARKETS = ("CSI", "SSE", "SZSE","SW")

INDEX_BASIC_COLUMNS = (
    "ts_code",
    "name",
    "fullname",
    "market",
    "publisher",
    "index_type",
    "category",
    "base_date",
    "base_point",
    "list_date",
    "weight_rule",
    "desc",
    "exp_date",
)
DATE_COLUMNS = {"base_date", "list_date", "exp_date"}
NUMERIC_COLUMNS = {"base_point"}
STRING_COLUMNS = tuple(
    column
    for column in INDEX_BASIC_COLUMNS
    if column not in DATE_COLUMNS and column not in NUMERIC_COLUMNS
)


def _clean_index_basic_params(
    *,
    ts_code: str = "",
    name: str = "",
    market: str = "",
    publisher: str = "",
    category: str = "",
) -> dict[str, str]:
    params = {
        "ts_code": ts_code,
        "name": name,
        "market": market,
        "publisher": publisher,
        "category": category,
    }
    return {
        key: value.strip()
        for key, value in params.items()
        if value is not None and value.strip()
    }


def _create_table_sql(table: str) -> str:
    table_sql = quote_table_name(table)
    return f"""
CREATE TABLE IF NOT EXISTS {table_sql}
(
    {quote_identifier('ts_code')} String COMMENT 'TS指数代码',
    {quote_identifier('name')} String COMMENT '指数简称',
    {quote_identifier('fullname')} String COMMENT '指数全称',
    {quote_identifier('market')} String COMMENT '市场',
    {quote_identifier('publisher')} String COMMENT '发布方',
    {quote_identifier('index_type')} String COMMENT '指数风格',
    {quote_identifier('category')} String COMMENT '指数类别',
    {quote_identifier('base_date')} Nullable(Date) COMMENT '基期',
    {quote_identifier('base_point')} Nullable(Float64) COMMENT '基点',
    {quote_identifier('list_date')} Nullable(Date) COMMENT '发布日期',
    {quote_identifier('weight_rule')} String COMMENT '加权方式',
    {quote_identifier('desc')} String COMMENT '描述',
    {quote_identifier('exp_date')} Nullable(Date) COMMENT '终止日期',
    {quote_identifier('created_at')} DateTime64(3) DEFAULT now64(3) COMMENT '创建时间',
    {quote_identifier('updated_at')} DateTime64(3) DEFAULT now64(3) COMMENT '更新时间'
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (market, ts_code)
SETTINGS index_granularity = 8192
COMMENT '指数基本信息'
""".strip()


def _ensure_index_basic_table(config: ClickHouseWriteConfig) -> None:
    if not config.enabled:
        return
    config.validate_for_write()
    with ClickHouseClient() as client:
        client.command(_create_table_sql(config.table))


def _index_basic_fields_from_clickhouse(config: ClickHouseWriteConfig) -> str:
    if not config.enabled:
        return ""

    with ClickHouseClient() as client:
        columns = client.table_columns(config.table)

    excluded_columns = {config.version_column, *METADATA_COLUMNS}
    fields = [column for column in columns if column not in excluded_columns]
    if not fields:
        raise ValueError(f"ClickHouse 表 {config.table} 未解析到可用于 Tushare 的字段")
    return ",".join(fields)


def _normalize_index_basic_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    result = df.copy()
    for column in STRING_COLUMNS:
        if column in result.columns:
            result[column] = result[column].fillna("").astype(str)
    return result


def _query_index_basic(
    pro: Any,
    *,
    ts_code: str,
    name: str,
    market: str,
    publisher: str,
    category: str,
    fields: str = "",
) -> pd.DataFrame:
    params = _clean_index_basic_params(
        ts_code=ts_code,
        name=name,
        market=market,
        publisher=publisher,
        category=category,
    )
    if fields:
        df = pro.index_basic(**params, fields=fields)
    else:
        df = pro.index_basic(**params)
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"index_basic 返回了非预期类型: {type(df)!r}，参数={params}")
    return _normalize_index_basic_dataframe(df.drop_duplicates(ignore_index=True))


@flow(name="Tushare 指数基本信息")
def tushare_index_basic_flow(
    ts_code: str = "",
    name: str = "",
    publisher: str = "",
    category: str = "",
    timeout: int = 30,
) -> None:
    """获取 Tushare index_basic 指数基本信息数据，并写入 ClickHouse。

    参数:
        ts_code: 指数 TS 代码，例如 000001.SH；为空时不按指数代码过滤。
        name: 指数简称；为空时不按简称过滤。
        publisher: 发布方；为空时不按发布方过滤。
        category: 指数类别；为空时不按类别过滤。
        timeout: Tushare API 请求超时时间，单位为秒。
    """

    logger = get_run_logger()
    token = env_value("TUSHARE_TOKEN", default="") or ""
    pro = ts.pro_api(token=token, timeout=timeout)
    write_config = ClickHouseWriteConfig.from_mapping(
        module_runtime("tushare_index_basic", "clickhouse")
    )
    _ensure_index_basic_table(write_config)
    fields = _index_basic_fields_from_clickhouse(write_config)

    frames: list[pd.DataFrame] = []
    empty_template: pd.DataFrame | None = None
    for market in DEFAULT_MARKETS:
        current_df = _query_index_basic(
            pro,
            ts_code=ts_code,
            name=name,
            market=market,
            publisher=publisher,
            category=category,
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

    filter_params = _clean_index_basic_params(
        ts_code=ts_code,
        name=name,
        publisher=publisher,
        category=category,
    )
    logger.info(
        "已获取 %s 条 Tushare index_basic 数据，markets=%s，参数=%s，字段=%r",
        len(df),
        list(DEFAULT_MARKETS),
        filter_params,
        list(df.columns),
    )
    written = write_dataframe(df, write_config)
    if write_config.enabled:
        logger.info(
            "已写入 %s 条 index_basic 数据到 ClickHouse 表 %s，模式=%s",
            written,
            write_config.table,
            write_config.mode,
        )
