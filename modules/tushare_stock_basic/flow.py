# -*- coding: utf-8 -*-
"""从 Tushare Pro 获取 stock_basic 股票基础信息。"""

from __future__ import annotations

import pandas as pd
import tushare as ts
from prefect import flow, get_run_logger

from core.db import ClickHouseWriteConfig, write_dataframe
from core.settings import env_value, module_runtime

DEFAULT_STOCK_BASIC_FIELDS = (
    "ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,"
    "curr_type,list_status,list_date,delist_date,is_hs,act_name,act_ent_type"
)


def _clean_stock_basic_params(
    *,
    ts_code: str = "",
    exchange: str = "",
    market: str = "",
    is_hs: str = "",
    list_status: str = "",
) -> dict[str, str]:
    params = {
        "ts_code": ts_code,
        "exchange": exchange,
        "market": market,
        "is_hs": is_hs,
        "list_status": list_status,
    }
    return {
        key: value.strip()
        for key, value in params.items()
        if value is not None and value.strip()
    }

@flow(name="Tushare 股票基础信息")
def tushare_stock_basic_flow(
    ts_code: str = "",
    exchange: str = "",
    market: str = "",
    is_hs: str = "",
    list_status: str = "L",
    timeout: int = 30,
) -> None:
    """获取 Tushare stock_basic 股票基础信息，并按配置写入 ClickHouse。

    参数:
        ts_code: 股票 TS 代码，例如 000001.SZ；为空时不按股票过滤。
        exchange: 交易所代码，例如 SSE、SZSE 或 BSE；为空时不按交易所过滤。
        market: 市场类别，例如 主板、创业板、科创板、北交所；为空时不按市场过滤。
        is_hs: 是否沪深港通标的，N 否、H 沪股通、S 深股通；为空时不过滤。
        list_status: 上市状态，L 上市、D 退市、P 暂停上市；默认 L。
        timeout: Tushare API 请求超时时间，单位为秒。
    """

    logger = get_run_logger()
    token = env_value("TUSHARE_TOKEN", default="") or ""
    pro = ts.pro_api(token=token, timeout=timeout)
    params = _clean_stock_basic_params(
        ts_code=ts_code,
        exchange=exchange,
        market=market,
        is_hs=is_hs,
        list_status=list_status,
    )
    df = pro.stock_basic(**params)
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"stock_basic 返回了非预期类型: {type(df)!r}")

    logger.info(
        "已获取 %s 条 Tushare stock_basic 数据，参数=%s，字段=%r",
        len(df),
        params,
    )
    write_config = ClickHouseWriteConfig.from_mapping(
        module_runtime("tushare_stock_basic", "clickhouse")
    )
    written = write_dataframe(df, write_config)
    if write_config.enabled:
        logger.info(
            "已写入 %s 条 stock_basic 数据到 ClickHouse 表 %s，模式=%s",
            written,
            write_config.table,
            write_config.mode,
        )
