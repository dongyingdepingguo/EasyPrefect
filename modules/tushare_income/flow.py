# -*- coding: utf-8 -*-
"""从 Tushare Pro 获取 income_vip 利润表数据。"""

from __future__ import annotations

import calendar
import datetime as dt
from typing import Any

import pandas as pd
import tushare as ts
from prefect import flow, get_run_logger

from core.db import ClickHouseWriteConfig, write_dataframe
from core.settings import env_value, module_runtime


def _clean_income_vip_params(
    *,
    ts_code: str = "",
    ann_date: str = "",
    f_ann_date: str = "",
    start_date: str = "",
    end_date: str = "",
    period: str = "",
    report_type: str = "",
    comp_type: str = "",
) -> dict[str, str]:
    params = {
        "ts_code": ts_code,
        "ann_date": ann_date,
        "f_ann_date": f_ann_date,
        "start_date": start_date,
        "end_date": end_date,
        "period": period,
        "report_type": report_type,
        "comp_type": comp_type,
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


@flow(name="Tushare 利润表 VIP")
def tushare_income_vip_flow(
    ts_code: str = "",
    ann_date: str = "",
    f_ann_date: str = "",
    start_date: str = "",
    end_date: str = "",
    period: str = "",
    report_type: str = "",
    comp_type: str = "",
    timeout: int = 30,
) -> list[dict[str, Any]]:
    """获取 income_vip 数据，并按字典列表返回结果。"""

    logger = get_run_logger()
    token = env_value("TUSHARE_TOKEN", default="") or ""
    pro = ts.pro_api(token=token, timeout=timeout)
    period = _default_period(period)
    params = _clean_income_vip_params(
        ts_code=ts_code,
        ann_date=ann_date,
        f_ann_date=f_ann_date,
        start_date=start_date,
        end_date=end_date,
        period=period,
        report_type=report_type,
        comp_type=comp_type,
    )
    df = pro.income_vip(**params)
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"income_vip 返回了非预期类型: {type(df)!r}")

    logger.info(
        "已获取 %s 条 Tushare income_vip 数据，参数=%s",
        len(df),
        params,
    )
    write_config = ClickHouseWriteConfig.from_mapping(
        module_runtime("tushare_income", "clickhouse")
    )
    written = write_dataframe(df, write_config)
    if write_config.enabled:
        logger.info(
            "已写入 %s 条 income_vip 数据到 ClickHouse 表 %s，模式=%s",
            written,
            write_config.table,
            write_config.mode,
        )
    return df.to_dict(orient="records")
