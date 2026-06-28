# -*- coding: utf-8 -*-
"""从 Tushare Pro 获取 cyq_perf 股票每日筹码及胜率数据。"""

from __future__ import annotations

import datetime as dt
import time
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
REQUEST_INTERVAL_SECONDS = 60 / 200
STOCK_BASIC_TABLE = "stock_base_basic"

CYQ_PERF_COLUMNS = (
    "ts_code",
    "trade_date",
    "his_low",
    "his_high",
    "cost_5pct",
    "cost_15pct",
    "cost_50pct",
    "cost_85pct",
    "cost_95pct",
    "weight_avg",
    "winner_rate",
)
NUMERIC_COLUMNS = tuple(
    column for column in CYQ_PERF_COLUMNS if column not in {"ts_code", "trade_date"}
)


def _clean_cyq_perf_params(
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
        "his_low": "历史最低价",
        "his_high": "历史最高价",
        "cost_5pct": "5%成本分位",
        "cost_15pct": "15%成本分位",
        "cost_50pct": "50%成本分位",
        "cost_85pct": "85%成本分位",
        "cost_95pct": "95%成本分位",
        "weight_avg": "加权平均成本",
        "winner_rate": "胜率",
    }
    columns = [
        f"{quote_identifier('ts_code')} String COMMENT 'TS股票代码'",
        f"{quote_identifier('trade_date')} Date COMMENT '交易日期'",
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
COMMENT '股票每日筹码及胜率'
""".strip()


def _ensure_cyq_perf_table(config: ClickHouseWriteConfig) -> None:
    if not config.enabled:
        return
    config.validate_for_write()
    with ClickHouseClient() as client:
        client.command(_create_table_sql(config.table))


def _cyq_perf_fields_from_clickhouse(config: ClickHouseWriteConfig) -> str:
    if not config.enabled:
        return ""

    with ClickHouseClient() as client:
        columns = client.table_columns(config.table)

    excluded_columns = {config.version_column, *METADATA_COLUMNS}
    fields = [column for column in columns if column not in excluded_columns]
    if not fields:
        raise ValueError(f"ClickHouse 表 {config.table} 未解析到可用于 Tushare 的字段")
    return ",".join(fields)


def _stock_basic_ts_codes_from_clickhouse() -> list[str]:
    with ClickHouseClient() as client:
        rows = client.select_records(STOCK_BASIC_TABLE, columns=("ts_code",))

    ts_codes = sorted(
        {
            str(row.get("ts_code", "")).strip()
            for row in rows
            if str(row.get("ts_code", "")).strip()
        }
    )
    if not ts_codes:
        raise ValueError(f"ClickHouse 表 {STOCK_BASIC_TABLE} 未读取到股票 TS 代码")
    return ts_codes


def _query_cyq_perf(
    pro: Any,
    *,
    ts_code: str,
    trade_date: str,
    start_date: str,
    end_date: str,
    fields: str = "",
) -> pd.DataFrame:
    params = _clean_cyq_perf_params(
        ts_code=ts_code,
        trade_date=trade_date,
        start_date=start_date,
        end_date=end_date,
    )
    if fields:
        df = pro.cyq_perf(**params, fields=fields)
    else:
        df = pro.cyq_perf(**params)
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"cyq_perf 返回了非预期类型: {type(df)!r}，参数={params}")
    return df.drop_duplicates(ignore_index=True)


@flow(name="Tushare 股票每日筹码及胜率")
def tushare_cyq_perf_flow(
    ts_code: str = "",
    trade_date: str = "",
    start_date: str = "",
    end_date: str = "",
    timeout: int = 30,
) -> None:
    """获取 Tushare cyq_perf 股票每日筹码及胜率数据，并写入 ClickHouse。

    参数:
        ts_code: 股票 TS 代码，例如 000001.SZ；为空时从 stock_base_basic
            读取全部股票并逐只获取。
        trade_date: 交易日期，格式 YYYYMMDD；为空时默认使用运行当天。
        start_date: 交易起始日期，格式 YYYYMMDD；为空时不设置区间起点。
        end_date: 交易结束日期，格式 YYYYMMDD；为空时不设置区间终点。
        timeout: Tushare API 请求超时时间，单位为秒。
    """

    logger = get_run_logger()
    token = env_value("TUSHARE_TOKEN", default="") or ""
    pro = ts.pro_api(token=token, timeout=timeout)
    write_config = ClickHouseWriteConfig.from_mapping(
        module_runtime("tushare_cyq_perf", "clickhouse")
    )
    _ensure_cyq_perf_table(write_config)
    fields = _cyq_perf_fields_from_clickhouse(write_config)
    trade_date = _default_trade_date(trade_date)
    ts_code = ts_code.strip() if ts_code else ""
    if ts_code:
        target_ts_codes = [ts_code]
    else:
        target_ts_codes = _stock_basic_ts_codes_from_clickhouse()
        logger.info(
            "ts_code 为空，已从 ClickHouse 表 %s 获取 %s 只股票",
            STOCK_BASIC_TABLE,
            len(target_ts_codes),
        )

    frames: list[pd.DataFrame] = []
    empty_template: pd.DataFrame | None = None
    for index, target_ts_code in enumerate(target_ts_codes, start=1):
        current_df = _query_cyq_perf(
            pro,
            ts_code=target_ts_code,
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

        if index % 100 == 0:
            logger.info(
                "cyq_perf 获取进度: %s/%s，当前股票=%s",
                index,
                len(target_ts_codes),
                target_ts_code,
            )
        if index < len(target_ts_codes):
            time.sleep(REQUEST_INTERVAL_SECONDS)

    if frames:
        df = pd.concat(frames, ignore_index=True).drop_duplicates(ignore_index=True)
    elif empty_template is not None:
        df = empty_template
    else:
        df = pd.DataFrame()

    params = _clean_cyq_perf_params(
        ts_code=ts_code,
        trade_date=trade_date,
        start_date=start_date,
        end_date=end_date,
    )
    logger.info(
        "已获取 %s 条 Tushare cyq_perf 数据，股票数=%s，参数=%s，字段=%r",
        len(df),
        len(target_ts_codes),
        params,
        list(df.columns),
    )
    written = write_dataframe(df, write_config)
    if write_config.enabled:
        logger.info(
            "已写入 %s 条 cyq_perf 数据到 ClickHouse 表 %s，模式=%s",
            written,
            write_config.table,
            write_config.mode,
        )
