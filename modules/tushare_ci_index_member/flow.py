# -*- coding: utf-8 -*-
"""从 Tushare Pro 获取 ci_index_member 中信行业成分数据。"""

from __future__ import annotations

from typing import Any

import pandas as pd
import tushare as ts
from prefect import flow, get_run_logger

from core.db import ClickHouseClient, ClickHouseWriteConfig, write_dataframe
from core.db.clickhouse import quote_identifier, quote_table_name
from core.settings import env_value, module_runtime

METADATA_COLUMNS = {"created_at"}
MAX_PAGE_SIZE = 5000

CI_INDEX_MEMBER_COLUMNS = (
    "l1_code",
    "l1_name",
    "l2_code",
    "l2_name",
    "l3_code",
    "l3_name",
    "ts_code",
    "name",
    "in_date",
    "out_date",
    "is_new",
)
DATE_COLUMNS = {"in_date", "out_date"}
STRING_COLUMNS = tuple(
    column for column in CI_INDEX_MEMBER_COLUMNS if column not in DATE_COLUMNS
)


def _clean_ci_index_member_params(
    *,
    l1_code: str = "",
    l2_code: str = "",
    l3_code: str = "",
    ts_code: str = "",
    is_new: str = "",
) -> dict[str, str]:
    params = {
        "l1_code": l1_code,
        "l2_code": l2_code,
        "l3_code": l3_code,
        "ts_code": ts_code,
        "is_new": is_new,
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
    {quote_identifier('l1_code')} String COMMENT '中信一级行业代码',
    {quote_identifier('l1_name')} String COMMENT '中信一级行业名称',
    {quote_identifier('l2_code')} String COMMENT '中信二级行业代码',
    {quote_identifier('l2_name')} String COMMENT '中信二级行业名称',
    {quote_identifier('l3_code')} String COMMENT '中信三级行业代码',
    {quote_identifier('l3_name')} String COMMENT '中信三级行业名称',
    {quote_identifier('ts_code')} String COMMENT '股票 TS 代码',
    {quote_identifier('name')} String COMMENT '股票名称',
    {quote_identifier('in_date')} Nullable(Date) COMMENT '纳入日期',
    {quote_identifier('out_date')} Nullable(Date) COMMENT '剔除日期',
    {quote_identifier('is_new')} String COMMENT '是否最新成分',
    {quote_identifier('created_at')} DateTime64(3) DEFAULT now64(3) COMMENT '创建时间',
    {quote_identifier('updated_at')} DateTime64(3) DEFAULT now64(3) COMMENT '更新时间'
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (
    l1_code,
    l2_code,
    l3_code,
    ts_code,
    ifNull(in_date, toDate('1970-01-01')),
    ifNull(out_date, toDate('1970-01-01'))
)
SETTINGS index_granularity = 8192
COMMENT '中信行业成分'
""".strip()


def _ensure_ci_index_member_table(config: ClickHouseWriteConfig) -> None:
    if not config.enabled:
        return
    config.validate_for_write()
    with ClickHouseClient() as client:
        client.command(_create_table_sql(config.table))


def _ci_index_member_fields_from_clickhouse(config: ClickHouseWriteConfig) -> str:
    if not config.enabled:
        return ""

    with ClickHouseClient() as client:
        columns = client.table_columns(config.table)

    excluded_columns = {config.version_column, *METADATA_COLUMNS}
    fields = [column for column in columns if column not in excluded_columns]
    if not fields:
        raise ValueError(f"ClickHouse 表 {config.table} 未解析到可用于 Tushare 的字段")
    return ",".join(fields)


def _normalize_ci_index_member_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    result = df.copy()
    for column in STRING_COLUMNS:
        if column in result.columns:
            result[column] = result[column].fillna("").astype(str)
    return result


def _query_ci_index_member_page(
    pro: Any,
    *,
    l1_code: str,
    l2_code: str,
    l3_code: str,
    ts_code: str,
    is_new: str,
    limit: int,
    offset: int,
    fields: str = "",
) -> pd.DataFrame:
    params: dict[str, Any] = _clean_ci_index_member_params(
        l1_code=l1_code,
        l2_code=l2_code,
        l3_code=l3_code,
        ts_code=ts_code,
        is_new=is_new,
    )
    params["limit"] = limit
    params["offset"] = offset
    if fields:
        df = pro.ci_index_member(**params, fields=fields)
    else:
        df = pro.ci_index_member(**params)
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"ci_index_member 返回了非预期类型: {type(df)!r}，参数={params}")
    return df


def _query_ci_index_member_all(
    pro: Any,
    *,
    l1_code: str,
    l2_code: str,
    l3_code: str,
    ts_code: str,
    is_new: str,
    fields: str = "",
    page_size: int = MAX_PAGE_SIZE,
) -> pd.DataFrame:
    if page_size <= 0 or page_size > MAX_PAGE_SIZE:
        raise ValueError(f"page_size 必须在 1 到 {MAX_PAGE_SIZE} 之间")

    frames: list[pd.DataFrame] = []
    empty_template: pd.DataFrame | None = None
    offset = 0
    while True:
        current_df = _query_ci_index_member_page(
            pro,
            l1_code=l1_code,
            l2_code=l2_code,
            l3_code=l3_code,
            ts_code=ts_code,
            is_new=is_new,
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
        return _normalize_ci_index_member_dataframe(df)
    if empty_template is not None:
        return _normalize_ci_index_member_dataframe(empty_template)
    return pd.DataFrame()


@flow(name="Tushare 中信行业成分")
def tushare_ci_index_member_flow(
    l1_code: str = "",
    l2_code: str = "",
    l3_code: str = "",
    ts_code: str = "",
    is_new: str = "",
    timeout: int = 30,
) -> None:
    """获取 Tushare ci_index_member 中信行业成分数据，并写入 ClickHouse。

    参数:
        l1_code: 中信一级行业代码；为空时不按一级行业过滤。
        l2_code: 中信二级行业代码；为空时不按二级行业过滤。
        l3_code: 中信三级行业代码；为空时不按三级行业过滤。
        ts_code: 股票 TS 代码，例如 000001.SZ；为空时不按股票过滤。
        is_new: 是否最新成分，通常为 Y/N；为空时不按状态过滤。
        timeout: Tushare API 请求超时时间，单位为秒。
    """

    logger = get_run_logger()
    token = env_value("TUSHARE_TOKEN", default="") or ""
    pro = ts.pro_api(token=token, timeout=timeout)
    write_config = ClickHouseWriteConfig.from_mapping(
        module_runtime("tushare_ci_index_member", "clickhouse")
    )
    _ensure_ci_index_member_table(write_config)
    fields = _ci_index_member_fields_from_clickhouse(write_config)
    params = _clean_ci_index_member_params(
        l1_code=l1_code,
        l2_code=l2_code,
        l3_code=l3_code,
        ts_code=ts_code,
        is_new=is_new,
    )
    df = _query_ci_index_member_all(
        pro,
        l1_code=l1_code,
        l2_code=l2_code,
        l3_code=l3_code,
        ts_code=ts_code,
        is_new=is_new,
        fields=fields,
    )

    logger.info(
        "已获取 %s 条 Tushare ci_index_member 数据，参数=%s，字段=%r",
        len(df),
        params,
        list(df.columns),
    )
    written = write_dataframe(df, write_config)
    if write_config.enabled:
        logger.info(
            "已写入 %s 条 ci_index_member 数据到 ClickHouse 表 %s，模式=%s",
            written,
            write_config.table,
            write_config.mode,
        )
