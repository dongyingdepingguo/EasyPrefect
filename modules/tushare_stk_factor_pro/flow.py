# -*- coding: utf-8 -*-
"""从 Tushare Pro 获取 stk_factor_pro 股票技术面因子数据。"""

from __future__ import annotations

import datetime as dt
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import tushare as ts
from prefect import flow, get_run_logger

from core.db import ClickHouseClient, ClickHouseWriteConfig, write_dataframe
from core.db.clickhouse import quote_identifier, quote_table_name
from core.settings import env_value, module_runtime

METADATA_COLUMNS = {"created_at"}
DEFAULT_TIMEZONE = ZoneInfo("Asia/Shanghai")

STK_FACTOR_PRO_COLUMNS = tuple(
    """
    ts_code trade_date open open_hfq open_qfq high high_hfq high_qfq low low_hfq
    low_qfq close close_hfq close_qfq pre_close change pct_chg vol amount
    turnover_rate turnover_rate_f volume_ratio pe pe_ttm pb ps ps_ttm dv_ratio
    dv_ttm total_share float_share free_share total_mv circ_mv adj_factor
    asi_bfq asi_hfq asi_qfq asit_bfq asit_hfq asit_qfq atr_bfq atr_hfq atr_qfq
    bbi_bfq bbi_hfq bbi_qfq bias1_bfq bias1_hfq bias1_qfq bias2_bfq bias2_hfq
    bias2_qfq bias3_bfq bias3_hfq bias3_qfq boll_lower_bfq boll_lower_hfq
    boll_lower_qfq boll_mid_bfq boll_mid_hfq boll_mid_qfq boll_upper_bfq
    boll_upper_hfq boll_upper_qfq brar_ar_bfq brar_ar_hfq brar_ar_qfq
    brar_br_bfq brar_br_hfq brar_br_qfq cci_bfq cci_hfq cci_qfq cr_bfq cr_hfq
    cr_qfq dfma_dif_bfq dfma_dif_hfq dfma_dif_qfq dfma_difma_bfq
    dfma_difma_hfq dfma_difma_qfq dmi_adx_bfq dmi_adx_hfq dmi_adx_qfq
    dmi_adxr_bfq dmi_adxr_hfq dmi_adxr_qfq dmi_mdi_bfq dmi_mdi_hfq
    dmi_mdi_qfq dmi_pdi_bfq dmi_pdi_hfq dmi_pdi_qfq downdays updays dpo_bfq
    dpo_hfq dpo_qfq madpo_bfq madpo_hfq madpo_qfq ema_bfq_10 ema_bfq_20
    ema_bfq_250 ema_bfq_30 ema_bfq_5 ema_bfq_60 ema_bfq_90 ema_hfq_10
    ema_hfq_20 ema_hfq_250 ema_hfq_30 ema_hfq_5 ema_hfq_60 ema_hfq_90
    ema_qfq_10 ema_qfq_20 ema_qfq_250 ema_qfq_30 ema_qfq_5 ema_qfq_60
    ema_qfq_90 emv_bfq emv_hfq emv_qfq maemv_bfq maemv_hfq maemv_qfq
    expma_12_bfq expma_12_hfq expma_12_qfq expma_50_bfq expma_50_hfq
    expma_50_qfq kdj_bfq kdj_hfq kdj_qfq kdj_d_bfq kdj_d_hfq kdj_d_qfq
    kdj_k_bfq kdj_k_hfq kdj_k_qfq ktn_down_bfq ktn_down_hfq ktn_down_qfq
    ktn_mid_bfq ktn_mid_hfq ktn_mid_qfq ktn_upper_bfq ktn_upper_hfq
    ktn_upper_qfq lowdays topdays ma_bfq_10 ma_bfq_20 ma_bfq_250 ma_bfq_30
    ma_bfq_5 ma_bfq_60 ma_bfq_90 ma_hfq_10 ma_hfq_20 ma_hfq_250 ma_hfq_30
    ma_hfq_5 ma_hfq_60 ma_hfq_90 ma_qfq_10 ma_qfq_20 ma_qfq_250 ma_qfq_30
    ma_qfq_5 ma_qfq_60 ma_qfq_90 macd_bfq macd_hfq macd_qfq macd_dea_bfq
    macd_dea_hfq macd_dea_qfq macd_dif_bfq macd_dif_hfq macd_dif_qfq
    mass_bfq mass_hfq mass_qfq ma_mass_bfq ma_mass_hfq ma_mass_qfq mfi_bfq
    mfi_hfq mfi_qfq mtm_bfq mtm_hfq mtm_qfq mtmma_bfq mtmma_hfq mtmma_qfq
    obv_bfq obv_hfq obv_qfq psy_bfq psy_hfq psy_qfq psyma_bfq psyma_hfq
    psyma_qfq roc_bfq roc_hfq roc_qfq maroc_bfq maroc_hfq maroc_qfq
    rsi_bfq_12 rsi_bfq_24 rsi_bfq_6 rsi_hfq_12 rsi_hfq_24 rsi_hfq_6
    rsi_qfq_12 rsi_qfq_24 rsi_qfq_6 taq_down_bfq taq_down_hfq taq_down_qfq
    taq_mid_bfq taq_mid_hfq taq_mid_qfq taq_up_bfq taq_up_hfq taq_up_qfq
    trix_bfq trix_hfq trix_qfq trma_bfq trma_hfq trma_qfq vr_bfq vr_hfq
    vr_qfq wr_bfq wr_hfq wr_qfq wr1_bfq wr1_hfq wr1_qfq xsii_td1_bfq
    xsii_td1_hfq xsii_td1_qfq xsii_td2_bfq xsii_td2_hfq xsii_td2_qfq
    xsii_td3_bfq xsii_td3_hfq xsii_td3_qfq xsii_td4_bfq xsii_td4_hfq
    xsii_td4_qfq
    """.split()
)
NUMERIC_COLUMNS = tuple(
    column for column in STK_FACTOR_PRO_COLUMNS if column not in {"ts_code", "trade_date"}
)


def _clean_stk_factor_pro_params(
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


def _create_table_sql(table: str) -> str:
    columns = [
        f"{quote_identifier('ts_code')} String COMMENT 'TS股票代码'",
        f"{quote_identifier('trade_date')} Date COMMENT '交易日期'",
    ]
    columns.extend(
        f"{quote_identifier(column)} Nullable(Float64)" for column in NUMERIC_COLUMNS
    )
    columns.extend(
        [
            f"{quote_identifier('created_at')} DateTime64(3) DEFAULT now64(3) "
            "COMMENT '创建时间'",
            f"{quote_identifier('updated_at')} DateTime64(3) DEFAULT now64(3) "
            "COMMENT '更新时间'",
        ]
    )
    column_sql = ",\n    ".join(columns)
    table_sql = quote_table_name(table)
    return f"""
CREATE TABLE IF NOT EXISTS {table_sql}
(
    {column_sql}
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(trade_date)
ORDER BY (trade_date, ts_code)
SETTINGS index_granularity = 8192
COMMENT '股票技术面因子'
""".strip()


def _ensure_stk_factor_pro_table(config: ClickHouseWriteConfig) -> None:
    if not config.enabled:
        return
    config.validate_for_write()
    with ClickHouseClient() as client:
        client.command(_create_table_sql(config.table))


def _stk_factor_pro_fields_from_clickhouse(config: ClickHouseWriteConfig) -> str:
    if not config.enabled:
        return ""

    with ClickHouseClient() as client:
        columns = client.table_columns(config.table)

    excluded_columns = {config.version_column, *METADATA_COLUMNS}
    fields = [column for column in columns if column not in excluded_columns]
    if not fields:
        raise ValueError(f"ClickHouse 表 {config.table} 未解析到可用于 Tushare 的字段")
    return ",".join(fields)


def _query_stk_factor_pro(
    pro: Any,
    *,
    ts_code: str,
    trade_date: str,
    start_date: str,
    end_date: str,
    fields: str = "",
) -> pd.DataFrame:
    params = _clean_stk_factor_pro_params(
        ts_code=ts_code,
        trade_date=trade_date,
        start_date=start_date,
        end_date=end_date,
    )
    if fields:
        df = pro.stk_factor_pro(**params, fields=fields)
    else:
        df = pro.stk_factor_pro(**params)
    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"stk_factor_pro 返回了非预期类型: {type(df)!r}，参数={params}")
    return df.drop_duplicates(ignore_index=True)


@flow(name="Tushare 股票技术面因子")
def tushare_stk_factor_pro_flow(
    ts_code: str = "",
    trade_date: str = "",
    start_date: str = "",
    end_date: str = "",
    timeout: int = 30,
) -> None:
    """获取 Tushare stk_factor_pro 股票技术面因子数据，并写入 ClickHouse。

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
        module_runtime("tushare_stk_factor_pro", "clickhouse")
    )
    _ensure_stk_factor_pro_table(write_config)
    fields = _stk_factor_pro_fields_from_clickhouse(write_config)
    trade_date = _default_trade_date(trade_date)
    params = _clean_stk_factor_pro_params(
        ts_code=ts_code,
        trade_date=trade_date,
        start_date=start_date,
        end_date=end_date,
    )
    df = _query_stk_factor_pro(
        pro,
        ts_code=ts_code,
        trade_date=trade_date,
        start_date=start_date,
        end_date=end_date,
        fields=fields,
    )

    logger.info(
        "已获取 %s 条 Tushare stk_factor_pro 数据，参数=%s，字段=%r",
        len(df),
        params,
        list(df.columns),
    )
    written = write_dataframe(df, write_config)
    if write_config.enabled:
        logger.info(
            "已写入 %s 条 stk_factor_pro 数据到 ClickHouse 表 %s，模式=%s",
            written,
            write_config.table,
            write_config.mode,
        )
