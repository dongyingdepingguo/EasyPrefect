# -*- coding: utf-8 -*-
"""从 Tushare Pro 获取 cn_ppi 工业生产者出厂价格指数数据。"""

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
MAX_PAGE_SIZE = 5000

CN_PPI_COLUMNS = (
    "month",
    "ppi_yoy",
    "ppi_mp_yoy",
    "ppi_mp_qm_yoy",
    "ppi_mp_rm_yoy",
    "ppi_mp_p_yoy",
    "ppi_cg_yoy",
    "ppi_cg_f_yoy",
    "ppi_cg_c_yoy",
    "ppi_cg_adu_yoy",
    "ppi_cg_dcg_yoy",
    "ppi_mom",
    "ppi_mp_mom",
    "ppi_mp_qm_mom",
    "ppi_mp_rm_mom",
    "ppi_mp_p_mom",
    "ppi_cg_mom",
    "ppi_cg_f_mom",
    "ppi_cg_c_mom",
    "ppi_cg_adu_mom",
    "ppi_cg_dcg_mom",
    "ppi_accu",
    "ppi_mp_accu",
    "ppi_mp_qm_accu",
    "ppi_mp_rm_accu",
    "ppi_mp_p_accu",
    "ppi_cg_accu",
    "ppi_cg_f_accu",
    "ppi_cg_c_accu",
    "ppi_cg_adu_accu",
    "ppi_cg_dcg_accu",
)
NUMERIC_COLUMNS = tuple(column for column in CN_PPI_COLUMNS if column != "month")


def _clean_cn_ppi_params(
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


def _default_month(
    month: str | None,
    *,
    start_m: str = "",
    end_m: str = "",
) -> str:
    if month and month.strip():
        return month.strip()
    if start_m.strip() or end_m.strip():
        return ""
    return dt.datetime.now(DEFAULT_TIMEZONE).strftime("%Y%m")


def _create_table_sql(table: str) -> str:
    column_comments = {
        "ppi_yoy": "工业生产者出厂价格指数同比",
        "ppi_mp_yoy": "生产资料价格指数同比",
        "ppi_mp_qm_yoy": "生产资料-采掘工业价格指数同比",
        "ppi_mp_rm_yoy": "生产资料-原材料工业价格指数同比",
        "ppi_mp_p_yoy": "生产资料-加工工业价格指数同比",
        "ppi_cg_yoy": "生活资料价格指数同比",
        "ppi_cg_f_yoy": "生活资料-食品类价格指数同比",
        "ppi_cg_c_yoy": "生活资料-衣着类价格指数同比",
        "ppi_cg_adu_yoy": "生活资料-一般日用品类价格指数同比",
        "ppi_cg_dcg_yoy": "生活资料-耐用消费品类价格指数同比",
        "ppi_mom": "工业生产者出厂价格指数环比",
        "ppi_mp_mom": "生产资料价格指数环比",
        "ppi_mp_qm_mom": "生产资料-采掘工业价格指数环比",
        "ppi_mp_rm_mom": "生产资料-原材料工业价格指数环比",
        "ppi_mp_p_mom": "生产资料-加工工业价格指数环比",
        "ppi_cg_mom": "生活资料价格指数环比",
        "ppi_cg_f_mom": "生活资料-食品类价格指数环比",
        "ppi_cg_c_mom": "生活资料-衣着类价格指数环比",
        "ppi_cg_adu_mom": "生活资料-一般日用品类价格指数环比",
        "ppi_cg_dcg_mom": "生活资料-耐用消费品类价格指数环比",
        "ppi_accu": "工业生产者出厂价格指数累计同比",
        "ppi_mp_accu": "生产资料价格指数累计同比",
        "ppi_mp_qm_accu": "生产资料-采掘工业价格指数累计同比",
        "ppi_mp_rm_accu": "生产资料-原材料工业价格指数累计同比",
        "ppi_mp_p_accu": "生产资料-加工工业价格指数累计同比",
        "ppi_cg_accu": "生活资料价格指数累计同比",
        "ppi_cg_f_accu": "生活资料-食品类价格指数累计同比",
        "ppi_cg_c_accu": "生活资料-衣着类价格指数累计同比",
        "ppi_cg_adu_accu": "生活资料-一般日用品类价格指数累计同比",
        "ppi_cg_dcg_accu": "生活资料-耐用消费品类价格指数累计同比",
    }
    columns = [
        f"{quote_identifier('month')} String COMMENT '月份，格式 YYYYMM'",
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
ORDER BY month
SETTINGS index_granularity = 8192
COMMENT '工业生产者出厂价格指数'
""".strip()


def _ensure_cn_ppi_table(config: ClickHouseWriteConfig) -> None:
    if not config.enabled:
        return
    config.validate_for_write()
    with ClickHouseClient() as client:
        client.command(_create_table_sql(config.table))


def _cn_ppi_fields_from_clickhouse(config: ClickHouseWriteConfig) -> str:
    if not config.enabled:
        return ""

    with ClickHouseClient() as client:
        columns = client.table_columns(config.table)

    excluded_columns = {config.version_column, *METADATA_COLUMNS}
    fields = [column for column in columns if column not in excluded_columns]
    if not fields:
        raise ValueError(f"ClickHouse 表 {config.table} 未解析到可用于 Tushare 的字段")
    return ",".join(fields)


def _normalize_cn_ppi_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    result = df.copy()
    if "month" in result.columns:
        result["month"] = result["month"].fillna("").astype(str)
    for column in NUMERIC_COLUMNS:
        if column in result.columns:
            result[column] = pd.to_numeric(result[column], errors="coerce")
    return result


def _query_cn_ppi_page(
    pro: Any,
    *,
    m: str,
    start_m: str,
    end_m: str,
    limit: int,
    offset: int,
    fields: str = "",
) -> pd.DataFrame:
    params: dict[str, Any] = _clean_cn_ppi_params(
        m=m,
        start_m=start_m,
        end_m=end_m,
    )
    params["limit"] = limit
    params["offset"] = offset
    if fields:
        df = pro.cn_ppi(**params, fields=fields)
    else:
        df = pro.cn_ppi(**params)
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"cn_ppi 返回了非预期类型: {type(df)!r}，参数={params}")
    return df


def _query_cn_ppi_all(
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
        current_df = _query_cn_ppi_page(
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
        return _normalize_cn_ppi_dataframe(df)
    if empty_template is not None:
        return _normalize_cn_ppi_dataframe(empty_template)
    return pd.DataFrame()


@flow(name="Tushare 工业生产者出厂价格指数")
def tushare_cn_ppi_flow(
    m: str = "",
    start_m: str = "",
    end_m: str = "",
    timeout: int = 30,
) -> None:
    """获取 Tushare cn_ppi 工业生产者出厂价格指数数据，并写入 ClickHouse。

    参数:
        m: 数据月份，格式 YYYYMM；为空且未设置起止月份时默认使用运行当月。
        start_m: 起始月份，格式 YYYYMM；为空时不设置区间起点。
        end_m: 结束月份，格式 YYYYMM；为空时不设置区间终点。
        timeout: Tushare API 请求超时时间，单位为秒。
    """

    logger = get_run_logger()
    token = env_value("TUSHARE_TOKEN", default="") or ""
    pro = ts.pro_api(token=token, timeout=timeout)
    write_config = ClickHouseWriteConfig.from_mapping(
        module_runtime("tushare_cn_ppi", "clickhouse")
    )
    _ensure_cn_ppi_table(write_config)
    fields = _cn_ppi_fields_from_clickhouse(write_config)
    m = _default_month(m, start_m=start_m, end_m=end_m)
    params = _clean_cn_ppi_params(
        m=m,
        start_m=start_m,
        end_m=end_m,
    )
    df = _query_cn_ppi_all(
        pro,
        m=m,
        start_m=start_m,
        end_m=end_m,
        fields=fields,
    )

    logger.info(
        "已获取 %s 条 Tushare cn_ppi 数据，参数=%s，字段=%r",
        len(df),
        params,
        list(df.columns),
    )
    written = write_dataframe(df, write_config)
    if write_config.enabled:
        logger.info(
            "已写入 %s 条 cn_ppi 数据到 ClickHouse 表 %s，模式=%s",
            written,
            write_config.table,
            write_config.mode,
        )
