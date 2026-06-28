# -*- coding: utf-8 -*-
"""从 Tushare Pro 获取 stk_week_month_adj 周/月线复权行情数据。"""

from __future__ import annotations

import calendar
import datetime as dt
from typing import Any, Literal
from zoneinfo import ZoneInfo

import pandas as pd
import tushare as ts
from prefect import flow, get_run_logger

from core.db import ClickHouseClient, ClickHouseWriteConfig, write_dataframe
from core.settings import env_value, module_runtime

Freq = Literal["week", "month"]

METADATA_COLUMNS = {"created_at"}
DEFAULT_TIMEZONE = ZoneInfo("Asia/Shanghai")
VALID_FREQS = {"week", "month"}


def _normalize_freq(freq: str) -> Freq:
    normalized = freq.strip().lower()
    if normalized not in VALID_FREQS:
        raise ValueError("freq 必须是 week 或 month")
    return normalized  # type: ignore[return-value]


def _clean_stk_week_month_adj_params(
    *,
    ts_code: str = "",
    trade_date: str = "",
    start_date: str = "",
    end_date: str = "",
    freq: str = "",
) -> dict[str, str]:
    params = {
        "ts_code": ts_code,
        "trade_date": trade_date,
        "start_date": start_date,
        "end_date": end_date,
        "freq": freq,
    }
    return {
        key: value.strip()
        for key, value in params.items()
        if value is not None and value.strip()
    }


def _current_week_friday(run_date: dt.date | None = None) -> str:
    run_date = run_date or dt.datetime.now(DEFAULT_TIMEZONE).date()
    friday = run_date + dt.timedelta(days=4 - run_date.weekday())
    return friday.strftime("%Y%m%d")


def _current_month_last_date(run_date: dt.date | None = None) -> str:
    run_date = run_date or dt.datetime.now(DEFAULT_TIMEZONE).date()
    day = calendar.monthrange(run_date.year, run_date.month)[1]
    return dt.date(run_date.year, run_date.month, day).strftime("%Y%m%d")


def _default_trade_date(trade_date: str | None, freq: str) -> str:
    if trade_date and trade_date.strip():
        return trade_date.strip()

    normalized_freq = _normalize_freq(freq)
    if normalized_freq == "week":
        return _current_week_friday()
    return _current_month_last_date()


def _stk_week_month_adj_fields_from_clickhouse(config: ClickHouseWriteConfig) -> str:
    if not config.enabled:
        return ""

    with ClickHouseClient() as client:
        columns = client.table_columns(config.table)

    excluded_columns = {config.version_column, *METADATA_COLUMNS}
    fields = [column for column in columns if column not in excluded_columns]
    if not fields:
        raise ValueError(f"ClickHouse 表 {config.table} 未解析到可用于 Tushare 的字段")
    return ",".join(fields)


def _ensure_stk_week_month_adj_columns(
    df: pd.DataFrame,
    *,
    trade_date: str,
    end_date: str,
    freq: str,
) -> pd.DataFrame:
    result = df.copy()
    if "trade_date" not in result.columns:
        result["trade_date"] = trade_date
    if "freq" not in result.columns:
        result["freq"] = freq
    if "end_date" not in result.columns:
        result["end_date"] = end_date or trade_date
    return result


def _query_stk_week_month_adj(
    pro: Any,
    *,
    ts_code: str,
    trade_date: str,
    start_date: str,
    end_date: str,
    freq: str,
    fields: str = "",
) -> pd.DataFrame:
    params = _clean_stk_week_month_adj_params(
        ts_code=ts_code,
        trade_date=trade_date,
        start_date=start_date,
        end_date=end_date,
        freq=freq,
    )
    if fields:
        df = pro.stk_week_month_adj(**params, fields=fields)
    else:
        df = pro.stk_week_month_adj(**params)
    if not isinstance(df, pd.DataFrame):
        raise TypeError(
            f"stk_week_month_adj 返回了非预期类型: {type(df)!r}，参数={params}"
        )
    df = _ensure_stk_week_month_adj_columns(
        df,
        trade_date=trade_date,
        end_date=end_date,
        freq=freq,
    )
    return df.drop_duplicates(ignore_index=True)


@flow(name="Tushare 周月线复权行情")
def tushare_stk_week_month_adj_flow(
    ts_code: str = "",
    trade_date: str = "",
    start_date: str = "",
    end_date: str = "",
    freq: str = "week",
    timeout: int = 30,
) -> None:
    """获取 Tushare stk_week_month_adj 周/月线复权行情数据，并写入 ClickHouse。

    参数:
        ts_code: 股票 TS 代码，例如 000001.SZ；为空时不按股票过滤。
        trade_date: 交易日期，格式 YYYYMMDD；为空时 week 默认当前周五，month 默认当前月最后一天。
        start_date: 交易起始日期，格式 YYYYMMDD；为空时不设置区间起点。
        end_date: 交易结束日期，格式 YYYYMMDD；为空时不设置区间终点。
        freq: 周期，week 表示周线，month 表示月线。
        timeout: Tushare API 请求超时时间，单位为秒。
    """

    logger = get_run_logger()
    freq = _normalize_freq(freq)
    token = env_value("TUSHARE_TOKEN", default="") or ""
    pro = ts.pro_api(token=token, timeout=timeout)
    write_config = ClickHouseWriteConfig.from_mapping(
        module_runtime("tushare_stk_week_month_adj", "clickhouse")
    )
    fields = _stk_week_month_adj_fields_from_clickhouse(write_config)
    trade_date = _default_trade_date(trade_date, freq)
    params = _clean_stk_week_month_adj_params(
        ts_code=ts_code,
        trade_date=trade_date,
        start_date=start_date,
        end_date=end_date,
        freq=freq,
    )
    df = _query_stk_week_month_adj(
        pro,
        ts_code=ts_code,
        trade_date=trade_date,
        start_date=start_date,
        end_date=end_date,
        freq=freq,
        fields=fields,
    )

    logger.info(
        "已获取 %s 条 Tushare stk_week_month_adj 数据，参数=%s，字段=%r",
        len(df),
        params,
        list(df.columns),
    )
    written = write_dataframe(df, write_config)
    if write_config.enabled:
        logger.info(
            "已写入 %s 条 stk_week_month_adj 数据到 ClickHouse 表 %s，模式=%s",
            written,
            write_config.table,
            write_config.mode,
        )
