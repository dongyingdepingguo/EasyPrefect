# -*- coding: utf-8 -*-
"""从 Tushare Pro 获取交易日历数据。"""

from __future__ import annotations

import datetime as dt
from typing import Any

import pandas as pd
import tushare as ts
from prefect import flow, get_run_logger

from core.db import ClickHouseWriteConfig, write_dataframe
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


@flow(name="Tushare 交易日历")
def tushare_trade_cal_flow(
    exchange: str = "",
    start_date: str = "",
    end_date: str = "",
    is_open: str = "",
    fields: str = "",
    timeout: int = 30,
) -> list[dict[str, Any]]:
    """获取 trade_cal 数据，并按字典列表返回结果。"""

    logger = get_run_logger()
    token = env_value("TUSHARE_TOKEN", default="") or ""
    pro = ts.pro_api(token=token, timeout=timeout)
    start_date = _default_start_date(start_date)
    params = _clean_trade_cal_params(
        exchange=exchange,
        start_date=start_date,
        end_date=end_date,
        is_open=is_open,
    )
    df = pro.trade_cal(**params, fields=fields)
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"trade_cal 返回了非预期类型: {type(df)!r}")

    logger.info(
        "已获取 %s 条 Tushare trade_cal 数据，参数=%s，字段=%r",
        len(df),
        params,
        fields,
    )
    write_config = ClickHouseWriteConfig.from_mapping(
        module_runtime("tushare_trade_cal", "clickhouse")
    )
    written = write_dataframe(df, write_config)
    if write_config.enabled:
        logger.info(
            "已写入 %s 条 trade_cal 数据到 ClickHouse 表 %s，模式=%s",
            written,
            write_config.table,
            write_config.mode,
        )
    return df.to_dict(orient="records")
