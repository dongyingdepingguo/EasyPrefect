# -*- coding: utf-8 -*-
"""从 Tushare Pro 获取交易日历数据。"""

from __future__ import annotations

import datetime as dt

import pandas as pd
import tushare as ts
from prefect import flow, get_run_logger

from core.db import ClickHouseClient, ClickHouseWriteConfig, write_dataframe
from core.settings import env_value, module_runtime


def _clean_trade_cal_params(
    *,
    exchange: str = "",
    start_date: str = "",
    end_date: str = "",
    is_open: str = "",
) -> dict[str, str]:
    params = {
        "exchange": exchange,
        "start_date": start_date,
        "end_date": end_date,
        "is_open": is_open,
    }
    return {key: value for key, value in params.items() if value not in ("", None)}


def _default_start_date(start_date: str | None) -> str:
    if start_date and start_date.strip():
        return start_date.strip()
    return dt.date.today().strftime("%Y%m%d")


def _trade_cal_fields_from_clickhouse(config: ClickHouseWriteConfig) -> str:
    if not config.enabled:
        return ""

    with ClickHouseClient() as client:
        columns = client.table_columns(config.table)

    excluded_columns = {config.version_column}
    fields = [column for column in columns if column not in excluded_columns]
    if not fields:
        raise ValueError(f"ClickHouse 表 {config.table} 未解析到可用于 Tushare 的字段")
    return ",".join(fields)


@flow(name="Tushare 交易日历")
def tushare_trade_cal_flow(
    exchange: str = "",
    start_date: str = "",
    end_date: str = "",
    is_open: str = "",
    timeout: int = 30,
) -> None:
    """获取 Tushare trade_cal 交易日历数据，并按配置写入 ClickHouse。

    参数:
        exchange: 交易所代码，例如 SSE 或 SZSE；为空时不按交易所过滤。
        start_date: 起始日期，格式 YYYYMMDD；为空时默认使用运行当天。
        end_date: 结束日期，格式 YYYYMMDD；为空时由 Tushare 接口默认处理。
        is_open: 是否交易，1 表示交易日，0 表示休市日；为空时不过滤。
        timeout: Tushare API 请求超时时间，单位为秒。
    """

    logger = get_run_logger()
    token = env_value("TUSHARE_TOKEN", default="") or ""
    pro = ts.pro_api(token=token, timeout=timeout)
    write_config = ClickHouseWriteConfig.from_mapping(
        module_runtime("tushare_trade_cal", "clickhouse")
    )
    fields = _trade_cal_fields_from_clickhouse(write_config)
    start_date = _default_start_date(start_date)
    params = _clean_trade_cal_params(
        exchange=exchange,
        start_date=start_date,
        end_date=end_date,
        is_open=is_open,
    )
    if fields:
        df = pro.trade_cal(**params, fields=fields)
    else:
        df = pro.trade_cal(**params)
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"trade_cal 返回了非预期类型: {type(df)!r}")

    logger.info(
        "已获取 %s 条 Tushare trade_cal 数据，参数=%s，字段=%r",
        len(df),
        params,
        list(df.columns),
    )
    written = write_dataframe(df, write_config)
    if write_config.enabled:
        logger.info(
            "已写入 %s 条 trade_cal 数据到 ClickHouse 表 %s，模式=%s",
            written,
            write_config.table,
            write_config.mode,
        )
