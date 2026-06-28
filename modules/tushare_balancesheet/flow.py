# -*- coding: utf-8 -*-
"""从 Tushare Pro 获取 balancesheet_vip 资产负债表数据。"""

from __future__ import annotations

import calendar
import datetime as dt

import pandas as pd
import tushare as ts
from prefect import flow, get_run_logger

from core.db import ClickHouseWriteConfig, write_dataframe
from core.settings import env_value, module_runtime


def _clean_balancesheet_vip_params(
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


@flow(name="Tushare 资产负债表 VIP")
def tushare_balancesheet_flow(
    ts_code: str = "",
    ann_date: str = "",
    f_ann_date: str = "",
    start_date: str = "",
    end_date: str = "",
    period: str = "",
    report_type: str = "",
    comp_type: str = "",
    timeout: int = 30,
) -> None:
    """获取 Tushare balancesheet_vip 资产负债表数据，并按配置写入 ClickHouse。

    参数:
        ts_code: 股票 TS 代码，例如 000001.SZ；为空时不按股票过滤。
        ann_date: 公告日期，格式 YYYYMMDD；为空时不按公告日期精确过滤。
        f_ann_date: 实际公告日期，格式 YYYYMMDD；为空时不按实际公告日期过滤。
        start_date: 公告起始日期，格式 YYYYMMDD；为空时不设置公告区间起点。
        end_date: 公告结束日期，格式 YYYYMMDD；为空时不设置公告区间终点。
        period: 报告期，格式 YYYYMMDD；为空时默认取上一季度末日期。
        report_type: 报告类型；为空时不按报告类型过滤。
        comp_type: 公司类型，1 一般工商业，2 银行，3 保险，4 证券；为空时不按公司类型过滤。
        timeout: Tushare API 请求超时时间，单位为秒。
    """

    logger = get_run_logger()
    token = env_value("TUSHARE_TOKEN", default="") or ""
    pro = ts.pro_api(token=token, timeout=timeout)
    period = _default_period(period)
    params = _clean_balancesheet_vip_params(
        ts_code=ts_code,
        ann_date=ann_date,
        f_ann_date=f_ann_date,
        start_date=start_date,
        end_date=end_date,
        period=period,
        report_type=report_type,
        comp_type=comp_type,
    )
    df = pro.balancesheet_vip(**params)
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"balancesheet_vip 返回了非预期类型: {type(df)!r}")

    logger.info(
        "已获取 %s 条 Tushare balancesheet_vip 数据，参数=%s",
        len(df),
        params,
    )
    write_config = ClickHouseWriteConfig.from_mapping(
        module_runtime("tushare_balancesheet", "clickhouse")
    )
    written = write_dataframe(df, write_config)
    if write_config.enabled:
        logger.info(
            "已写入 %s 条 balancesheet_vip 数据到 ClickHouse 表 %s，模式=%s",
            written,
            write_config.table,
            write_config.mode,
        )
