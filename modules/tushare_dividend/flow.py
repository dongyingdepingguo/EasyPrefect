# -*- coding: utf-8 -*-
"""从 Tushare Pro 获取 dividend 分红送股数据。"""

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


def _clean_dividend_params(
    *,
    ts_code: str = "",
    end_date: str = "",
    ann_date: str = "",
    record_date: str = "",
    ex_date: str = "",
    imp_ann_date: str = "",
) -> dict[str, str]:
    params = {
        "ts_code": ts_code,
        "end_date": end_date,
        "ann_date": ann_date,
        "record_date": record_date,
        "ex_date": ex_date,
        "imp_ann_date": imp_ann_date,
    }
    return {
        key: value.strip()
        for key, value in params.items()
        if value is not None and value.strip()
    }


def _current_quarter_end_date(run_date: dt.date | None = None) -> str:
    run_date = run_date or dt.date.today()
    quarter = (run_date.month - 1) // 3 + 1
    month = quarter * 3
    day = calendar.monthrange(run_date.year, month)[1]
    return dt.date(run_date.year, month, day).strftime("%Y%m%d")


def _default_end_date(end_date: str | None) -> str:
    if end_date and end_date.strip():
        return end_date.strip()
    return _current_quarter_end_date()


def _dividend_fields_from_clickhouse(config: ClickHouseWriteConfig) -> str:
    if not config.enabled:
        return ""

    with ClickHouseClient() as client:
        columns = client.table_columns(config.table)

    excluded_columns = {config.version_column, *METADATA_COLUMNS}
    fields = [column for column in columns if column not in excluded_columns]
    if not fields:
        raise ValueError(f"ClickHouse 表 {config.table} 未解析到可用于 Tushare 的字段")
    return ",".join(fields)


def _query_dividend(
    pro: Any,
    *,
    ts_code: str,
    end_date: str,
    ann_date: str,
    record_date: str,
    ex_date: str,
    imp_ann_date: str,
    fields: str = "",
) -> pd.DataFrame:
    params = _clean_dividend_params(
        ts_code=ts_code,
        end_date=end_date,
        ann_date=ann_date,
        record_date=record_date,
        ex_date=ex_date,
        imp_ann_date=imp_ann_date,
    )
    if fields:
        df = pro.dividend(**params, fields=fields)
    else:
        df = pro.dividend(**params)
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"dividend 返回了非预期类型: {type(df)!r}，参数={params}")
    return df.drop_duplicates(ignore_index=True)


@flow(name="Tushare 分红送股")
def tushare_dividend_flow(
    ts_code: str = "",
    end_date: str = "",
    ann_date: str = "",
    record_date: str = "",
    ex_date: str = "",
    imp_ann_date: str = "",
    timeout: int = 30,
) -> None:
    """获取 Tushare dividend 分红送股数据，并按配置写入 ClickHouse。

    参数:
        ts_code: 股票 TS 代码，例如 000001.SZ；为空时不按股票过滤。
        end_date: 分红年度，格式 YYYYMMDD；为空时默认取当前季度最后一天。
        ann_date: 预案公告日，格式 YYYYMMDD；为空时不按预案公告日过滤。
        record_date: 股权登记日，格式 YYYYMMDD；为空时不按股权登记日过滤。
        ex_date: 除权除息日，格式 YYYYMMDD；为空时不按除权除息日过滤。
        imp_ann_date: 实施公告日，格式 YYYYMMDD；为空时不按实施公告日过滤。
        timeout: Tushare API 请求超时时间，单位为秒。
    """

    logger = get_run_logger()
    token = env_value("TUSHARE_TOKEN", default="") or ""
    pro = ts.pro_api(token=token, timeout=timeout)
    write_config = ClickHouseWriteConfig.from_mapping(
        module_runtime("tushare_dividend", "clickhouse")
    )
    fields = _dividend_fields_from_clickhouse(write_config)
    end_date = _default_end_date(end_date)
    ts_code = ts_code.strip() if ts_code else ""
    filter_params = _clean_dividend_params(
        ts_code=ts_code,
        end_date=end_date,
        ann_date=ann_date,
        record_date=record_date,
        ex_date=ex_date,
        imp_ann_date=imp_ann_date,
    )

    df = _query_dividend(
        pro,
        ts_code=ts_code,
        end_date=end_date,
        ann_date=ann_date,
        record_date=record_date,
        ex_date=ex_date,
        imp_ann_date=imp_ann_date,
        fields=fields,
    )

    logger.info(
        "已获取 %s 条 Tushare dividend 数据，参数=%s，字段=%r",
        len(df),
        filter_params,
        list(df.columns),
    )
    written = write_dataframe(df, write_config)
    if write_config.enabled:
        logger.info(
            "已写入 %s 条 dividend 数据到 ClickHouse 表 %s，模式=%s",
            written,
            write_config.table,
            write_config.mode,
        )
