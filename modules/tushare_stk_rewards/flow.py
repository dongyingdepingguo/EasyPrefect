# -*- coding: utf-8 -*-
"""从 Tushare Pro 获取 stk_rewards 管理层薪酬和持股数据。"""

from __future__ import annotations

import datetime as dt
import time
from typing import Any

import pandas as pd
import tushare as ts
from prefect import flow, get_run_logger

from core.db import ClickHouseClient, ClickHouseWriteConfig, write_dataframe
from core.settings import env_value, module_runtime

METADATA_COLUMNS = {"created_at"}
STK_REWARDS_MAX_REQUESTS_PER_MINUTE = 500
STK_REWARDS_REQUEST_INTERVAL_SECONDS = (
    60 / STK_REWARDS_MAX_REQUESTS_PER_MINUTE + 0.01
)
STOCK_BASIC_TABLE = "stock_base_basic"


def _clean_stk_rewards_params(
    *,
    ts_code: str = "",
    end_date: str = "",
) -> dict[str, str]:
    params = {
        "ts_code": ts_code,
        "end_date": end_date,
    }
    return {
        key: value.strip()
        for key, value in params.items()
        if value is not None and value.strip()
    }


def _previous_year_end_date(run_date: dt.date | None = None) -> str:
    run_date = run_date or dt.date.today()
    return dt.date(run_date.year - 1, 12, 31).strftime("%Y%m%d")


def _default_end_date(end_date: str | None) -> str:
    if end_date and end_date.strip():
        return end_date.strip()
    return _previous_year_end_date()


def _stk_rewards_fields_from_clickhouse(config: ClickHouseWriteConfig) -> str:
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


def _query_stk_rewards(
    pro: Any,
    *,
    ts_code: str,
    end_date: str,
    fields: str = "",
) -> pd.DataFrame:
    params = _clean_stk_rewards_params(
        ts_code=ts_code,
        end_date=end_date,
    )
    if fields:
        df = pro.stk_rewards(**params, fields=fields)
    else:
        df = pro.stk_rewards(**params)
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"stk_rewards 返回了非预期类型: {type(df)!r}，参数={params}")
    return df.drop_duplicates(ignore_index=True)


@flow(name="Tushare 管理层薪酬和持股")
def tushare_stk_rewards_flow(
    ts_code: str = "",
    end_date: str = "",
    timeout: int = 30,
) -> None:
    """获取 Tushare stk_rewards 管理层薪酬和持股数据，并按配置写入 ClickHouse。

    参数:
        ts_code: 股票 TS 代码，例如 000001.SZ；为空时从 stock_base_basic 读取全部股票并逐只获取。
        end_date: 报告期，格式 YYYYMMDD；为空时默认取去年最后一天。
        timeout: Tushare API 请求超时时间，单位为秒。
    """

    logger = get_run_logger()
    token = env_value("TUSHARE_TOKEN", default="") or ""
    pro = ts.pro_api(token=token, timeout=timeout)
    write_config = ClickHouseWriteConfig.from_mapping(
        module_runtime("tushare_stk_rewards", "clickhouse")
    )
    fields = _stk_rewards_fields_from_clickhouse(write_config)
    end_date = _default_end_date(end_date)
    logger.info(end_date)
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
        current_df = _query_stk_rewards(
            pro,
            ts_code=target_ts_code,
            end_date=end_date,
            fields=fields,
        )
        if current_df.empty:
            if empty_template is None:
                empty_template = current_df
        else:
            frames.append(current_df)
        if index < len(target_ts_codes):
            time.sleep(STK_REWARDS_REQUEST_INTERVAL_SECONDS)

    if frames:
        df = pd.concat(frames, ignore_index=True)
    elif empty_template is not None:
        df = empty_template
    else:
        df = pd.DataFrame()

    logger.info(
        "已获取 %s 条 Tushare stk_rewards 数据，股票数=%s，end_date=%s，字段=%r",
        len(df),
        len(target_ts_codes),
        end_date,
        list(df.columns),
    )
    written = write_dataframe(df, write_config)
    if write_config.enabled:
        logger.info(
            "已写入 %s 条 stk_rewards 数据到 ClickHouse 表 %s，模式=%s",
            written,
            write_config.table,
            write_config.mode,
        )
