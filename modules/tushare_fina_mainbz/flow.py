# -*- coding: utf-8 -*-
"""从 Tushare Pro 获取 fina_mainbz_vip 主营业务构成数据。"""

from __future__ import annotations

import calendar
import datetime as dt
from typing import Any

import pandas as pd
import tushare as ts
from prefect import flow, get_run_logger

from core.db import ClickHouseClient, ClickHouseWriteConfig, write_dataframe
from core.settings import env_value, module_runtime

METADATA_COLUMNS = {"created_at"}


def _clean_fina_mainbz_vip_params(
    *,
    ts_code: str = "",
    start_date: str = "",
    end_date: str = "",
    period: str = "",
    bz_code: str = "",
) -> dict[str, str]:
    params = {
        "ts_code": ts_code,
        "start_date": start_date,
        "end_date": end_date,
        "period": period,
        "type": bz_code,
    }
    return {
        key: value.strip()
        for key, value in params.items()
        if value is not None and value.strip()
    }


def _previous_quarter_end_date(run_date: dt.date | None = None) -> str:
    run_date = run_date or dt.date.today()
    current_quarter = (run_date.month - 1) // 3 + 1
    if current_quarter == 1:
        year = run_date.year - 1
        month = 12
    else:
        year = run_date.year
        month = (current_quarter - 1) * 3

    day = calendar.monthrange(year, month)[1]
    return dt.date(year, month, day).strftime("%Y%m%d")


def _default_period(period: str | None) -> str:
    if period and period.strip():
        return period.strip()
    return _previous_quarter_end_date()


def _fina_mainbz_fields_from_clickhouse(config: ClickHouseWriteConfig) -> str:
    if not config.enabled:
        return ""

    with ClickHouseClient() as client:
        columns = client.table_columns(config.table)

    excluded_columns = {config.version_column, *METADATA_COLUMNS}
    fields = [column for column in columns if column not in excluded_columns]
    if not fields:
        raise ValueError(f"ClickHouse 表 {config.table} 未解析到可用于 Tushare 的字段")
    return ",".join(fields)


def _query_fina_mainbz_vip(
    pro: Any,
    *,
    ts_code: str,
    start_date: str,
    end_date: str,
    period: str,
    bz_code: str,
    fields: str = "",
) -> pd.DataFrame:
    params = _clean_fina_mainbz_vip_params(
        ts_code=ts_code,
        start_date=start_date,
        end_date=end_date,
        period=period,
        bz_code=bz_code,
    )
    if fields:
        df = pro.fina_mainbz_vip(**params, fields=fields)
    else:
        df = pro.fina_mainbz_vip(**params)
    if not isinstance(df, pd.DataFrame):
        raise TypeError(
            f"fina_mainbz_vip 返回了非预期类型: {type(df)!r}，参数={params}"
        )
    return df.drop_duplicates(ignore_index=True)


@flow(name="Tushare 主营业务构成 VIP")
def tushare_fina_mainbz_flow(
    ts_code: str = "",
    start_date: str = "",
    end_date: str = "",
    period: str = "",
    bz_code: str = "",
    timeout: int = 30,
) -> None:
    """获取 Tushare fina_mainbz_vip 主营业务构成数据，并按配置写入 ClickHouse。

    参数:
        ts_code: 股票 TS 代码，例如 000001.SZ；为空时不按股票过滤。
        start_date: 报告期起始日期，格式 YYYYMMDD；为空时不设置报告期区间起点。
        end_date: 报告期结束日期，格式 YYYYMMDD；为空时不设置报告期区间终点。
        period: 报告期，格式 YYYYMMDD；为空时默认取上一季度末日期。
        bz_code: 主营业务来源类型，P 按产品、D 按地区、I 按行业；为空时不按类型过滤。
        timeout: Tushare API 请求超时时间，单位为秒。
    """

    logger = get_run_logger()
    token = env_value("TUSHARE_TOKEN", default="") or ""
    pro = ts.pro_api(token=token, timeout=timeout)
    write_config = ClickHouseWriteConfig.from_mapping(
        module_runtime("tushare_fina_mainbz", "clickhouse")
    )
    fields = _fina_mainbz_fields_from_clickhouse(write_config)
    period = _default_period(period)
    params = _clean_fina_mainbz_vip_params(
        ts_code=ts_code,
        start_date=start_date,
        end_date=end_date,
        period=period,
        bz_code=bz_code,
    )
    df = _query_fina_mainbz_vip(
        pro,
        ts_code=ts_code,
        start_date=start_date,
        end_date=end_date,
        period=period,
        bz_code=bz_code,
        fields=fields,
    )

    logger.info(
        "已获取 %s 条 Tushare fina_mainbz_vip 数据，参数=%s，字段=%r",
        len(df),
        params,
        list(df.columns),
    )
    written = write_dataframe(df, write_config)
    if write_config.enabled:
        logger.info(
            "已写入 %s 条 fina_mainbz_vip 数据到 ClickHouse 表 %s，模式=%s",
            written,
            write_config.table,
            write_config.mode,
        )
