# -*- coding: utf-8 -*-
"""从 Tushare Pro 获取 daily_basic 每日行情指标数据。"""

from __future__ import annotations

import datetime as dt
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import tushare as ts
from prefect import flow, get_run_logger

from core.db import ClickHouseClient, ClickHouseWriteConfig, write_dataframe
from core.settings import env_value, module_runtime

METADATA_COLUMNS = {"created_at"}
DEFAULT_TIMEZONE = ZoneInfo("Asia/Shanghai")


def _clean_daily_basic_params(
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


def _daily_basic_fields_from_clickhouse(config: ClickHouseWriteConfig) -> str:
    if not config.enabled:
        return ""

    with ClickHouseClient() as client:
        columns = client.table_columns(config.table)

    excluded_columns = {config.version_column, *METADATA_COLUMNS}
    fields = [column for column in columns if column not in excluded_columns]
    if not fields:
        raise ValueError(f"ClickHouse 表 {config.table} 未解析到可用于 Tushare 的字段")
    return ",".join(fields)


def _query_daily_basic(
    pro: Any,
    *,
    ts_code: str,
    trade_date: str,
    start_date: str,
    end_date: str,
    fields: str = "",
) -> pd.DataFrame:
    params = _clean_daily_basic_params(
        ts_code=ts_code,
        trade_date=trade_date,
        start_date=start_date,
        end_date=end_date,
    )
    if fields:
        df = pro.daily_basic(**params, fields=fields)
    else:
        df = pro.daily_basic(**params)
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"daily_basic 返回了非预期类型: {type(df)!r}，参数={params}")
    return df.drop_duplicates(ignore_index=True)


@flow(name="Tushare 每日行情指标")
def tushare_daily_basic_flow(
    ts_code: str = "",
    trade_date: str = "",
    start_date: str = "",
    end_date: str = "",
    timeout: int = 30,
) -> None:
    """获取 Tushare daily_basic 每日行情指标数据，并按配置写入 ClickHouse。

    参数:
        ts_code: 股票 TS 代码，例如 000001.SZ；为空时不按股票过滤。
        trade_date: 交易日期，格式 YYYYMMDD；为空时默认使用运行当天。
        start_date: 交易起始日期，格式 YYYYMMDD；为空时不设置区间起点。
        end_date: 交易结束日期，格式 YYYYMMDD；为空时不设置区间终点。
        timeout: Tushare API 请求超时时间，单位为秒。
    """

    logger = get_run_logger()
    token = env_value("TUSHARE_TOKEN", default="") or ""
    pro = ts.pro_api(token=token, timeout=timeout)
    write_config = ClickHouseWriteConfig.from_mapping(
        module_runtime("tushare_daily_basic", "clickhouse")
    )
    fields = _daily_basic_fields_from_clickhouse(write_config)
    trade_date = _default_trade_date(trade_date)
    params = _clean_daily_basic_params(
        ts_code=ts_code,
        trade_date=trade_date,
        start_date=start_date,
        end_date=end_date,
    )
    df = _query_daily_basic(
        pro,
        ts_code=ts_code,
        trade_date=trade_date,
        start_date=start_date,
        end_date=end_date,
        fields=fields,
    )

    logger.info(
        "已获取 %s 条 Tushare daily_basic 数据，参数=%s，字段=%r",
        len(df),
        params,
        list(df.columns),
    )
    written = write_dataframe(df, write_config)
    if write_config.enabled:
        logger.info(
            "已写入 %s 条 daily_basic 数据到 ClickHouse 表 %s，模式=%s",
            written,
            write_config.table,
            write_config.mode,
        )
