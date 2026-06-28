# -*- coding: utf-8 -*-
"""从 Tushare Pro 获取 cn_pmi 采购经理人指数数据。"""

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
MAX_PAGE_SIZE = 2000

CN_PMI_COLUMNS = (
    "PMI020102",
    "PMI020500",
    "UPDATE_TIME",
    "PMI010700",
    "PMI010703",
    "PMI010801",
    "PMI011500",
    "PMI020201",
    "PMI020301",
    "PMI020402",
    "PMI020601",
    "PMI010100",
    "PMI010600",
    "PMI012000",
    "PMI020300",
    "PMI020502",
    "PMI020602",
    "PMI030000",
    "MONTH",
    "PMI010503",
    "PMI020100",
    "UPDATE_BY",
    "PMI010400",
    "PMI010601",
    "PMI010602",
    "PMI010702",
    "PMI020401",
    "CREATE_BY",
    "PMI010403",
    "PMI011400",
    "ID",
    "PMI010000",
    "PMI010200",
    "PMI010800",
    "PMI010803",
    "PMI010900",
    "PMI020600",
    "PMI010402",
    "PMI010502",
    "PMI011000",
    "PMI011900",
    "PMI020302",
    "PMI020900",
    "CREATE_TIME",
    "PMI010701",
    "PMI020202",
    "PMI020501",
    "PMI020700",
    "PMI011600",
    "PMI020101",
    "PMI010401",
    "PMI021000",
    "PMI011200",
    "PMI011700",
    "PMI011800",
    "PMI020400",
    "PMI010300",
    "PMI010501",
    "PMI020800",
    "PMI010500",
    "PMI010802",
    "PMI020200",
    "PMI010603",
    "PMI011300",
    "PMI011100",
)
STRING_COLUMNS = ("MONTH", "CREATE_BY", "UPDATE_BY", "CREATE_TIME", "UPDATE_TIME")
INTEGER_COLUMNS = ("ID",)
NUMERIC_COLUMNS = tuple(
    column
    for column in CN_PMI_COLUMNS
    if column not in {*STRING_COLUMNS, *INTEGER_COLUMNS}
)


def _clean_cn_pmi_params(
    *,
    m: str = "",
    start_m: str = "",
    end_m: str = "",
) -> dict[str, str]:
    params = {
        "m": m,
        "start_m": start_m,
        "end_m": end_m,
    }
    return {
        key: value.strip()
        for key, value in params.items()
        if value is not None and value.strip()
    }


def _default_month(month: str | None) -> str:
    if month and month.strip():
        return month.strip()
    return dt.datetime.now(DEFAULT_TIMEZONE).strftime("%Y%m")


def _column_sql(column: str) -> str:
    if column == "MONTH":
        return f"{quote_identifier(column)} String COMMENT '月份，格式 YYYYMM'"
    if column == "ID":
        return f"{quote_identifier(column)} Nullable(Int64) COMMENT '记录 ID'"
    if column in {"CREATE_BY", "UPDATE_BY"}:
        return f"{quote_identifier(column)} Nullable(String) COMMENT '{column}'"
    if column in {"CREATE_TIME", "UPDATE_TIME"}:
        return f"{quote_identifier(column)} Nullable(String) COMMENT '{column}'"
    return f"{quote_identifier(column)} Nullable(Float64) COMMENT 'PMI 指标 {column}'"


def _create_table_sql(table: str) -> str:
    columns = [_column_sql(column) for column in CN_PMI_COLUMNS]
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
ORDER BY {quote_identifier('MONTH')}
SETTINGS index_granularity = 8192
COMMENT '采购经理人指数'
""".strip()


def _ensure_cn_pmi_table(config: ClickHouseWriteConfig) -> None:
    if not config.enabled:
        return
    config.validate_for_write()
    with ClickHouseClient() as client:
        client.command(_create_table_sql(config.table))


def _cn_pmi_fields_from_clickhouse(config: ClickHouseWriteConfig) -> str:
    if not config.enabled:
        return ""

    with ClickHouseClient() as client:
        columns = client.table_columns(config.table)

    excluded_columns = {config.version_column, *METADATA_COLUMNS}
    fields = [column for column in columns if column not in excluded_columns]
    if not fields:
        raise ValueError(f"ClickHouse 表 {config.table} 未解析到可用于 Tushare 的字段")
    return ",".join(fields)


def _normalize_cn_pmi_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    result = df.copy()
    for column in STRING_COLUMNS:
        if column in result.columns:
            result[column] = result[column].fillna("").astype(str)
    return result


def _query_cn_pmi_page(
    pro: Any,
    *,
    m: str,
    start_m: str,
    end_m: str,
    limit: int,
    offset: int,
    fields: str = "",
) -> pd.DataFrame:
    params: dict[str, Any] = _clean_cn_pmi_params(
        m=m,
        start_m=start_m,
        end_m=end_m,
    )
    params["limit"] = limit
    params["offset"] = offset
    if fields:
        df = pro.cn_pmi(**params, fields=fields)
    else:
        df = pro.cn_pmi(**params)
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"cn_pmi 返回了非预期类型: {type(df)!r}，参数={params}")
    return df


def _query_cn_pmi_all(
    pro: Any,
    *,
    m: str,
    start_m: str,
    end_m: str,
    fields: str = "",
    page_size: int = MAX_PAGE_SIZE,
) -> pd.DataFrame:
    if page_size <= 0 or page_size > MAX_PAGE_SIZE:
        raise ValueError(f"page_size 必须在 1 到 {MAX_PAGE_SIZE} 之间")

    frames: list[pd.DataFrame] = []
    empty_template: pd.DataFrame | None = None
    offset = 0
    while True:
        current_df = _query_cn_pmi_page(
            pro,
            m=m,
            start_m=start_m,
            end_m=end_m,
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
        return _normalize_cn_pmi_dataframe(df)
    if empty_template is not None:
        return _normalize_cn_pmi_dataframe(empty_template)
    return pd.DataFrame()


@flow(name="Tushare 采购经理人指数")
def tushare_cn_pmi_flow(
    m: str = "",
    start_m: str = "",
    end_m: str = "",
    timeout: int = 30,
) -> None:
    """获取 Tushare cn_pmi 采购经理人指数数据，并写入 ClickHouse。

    参数:
        m: 数据月份，格式 YYYYMM；为空时默认使用运行当月。
        start_m: 起始月份，格式 YYYYMM；为空时不设置区间起点。
        end_m: 结束月份，格式 YYYYMM；为空时不设置区间终点。
        timeout: Tushare API 请求超时时间，单位为秒。
    """

    logger = get_run_logger()
    token = env_value("TUSHARE_TOKEN", default="") or ""
    pro = ts.pro_api(token=token, timeout=timeout)
    write_config = ClickHouseWriteConfig.from_mapping(
        module_runtime("tushare_cn_pmi", "clickhouse")
    )
    _ensure_cn_pmi_table(write_config)
    fields = _cn_pmi_fields_from_clickhouse(write_config)
    m = _default_month(m)
    params = _clean_cn_pmi_params(
        m=m,
        start_m=start_m,
        end_m=end_m,
    )
    df = _query_cn_pmi_all(
        pro,
        m=m,
        start_m=start_m,
        end_m=end_m,
        fields=fields,
    )

    logger.info(
        "已获取 %s 条 Tushare cn_pmi 数据，参数=%s，字段=%r",
        len(df),
        params,
        list(df.columns),
    )
    written = write_dataframe(df, write_config)
    if write_config.enabled:
        logger.info(
            "已写入 %s 条 cn_pmi 数据到 ClickHouse 表 %s，模式=%s",
            written,
            write_config.table,
            write_config.mode,
        )
