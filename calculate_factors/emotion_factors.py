import calculate_factors.feature as feature
import pymysql
from calculate_factors.sql_interact import Factor
import calculate_factors.sql_interact as si
import pandas as pd

"""
def get_emotion_factors(tsCode,startDate,endDate):
    df = js.get_daily(tsCode,startDate,endDate)
    vol_series=pd.Series(df['volume'])

    return
"""


def update_emotion_factor(code_date_serise):
    con = pymysql.connect(host="localhost", user="root", passwd="mysql", db="stockvision")
    table_name = "emotion_factor"
    pool = si.get_all_codes(con)

    logvol = Factor("logvol", [])
    logvol_10 = Factor("logvol_10", [])
    logvol_15 = Factor("logvol_15", [])
    logvol_20 = Factor("logvol_20", [])
    vol_change_rate = Factor("vol_change_rate", [])
    vol_change_rate_5 = Factor("vol_change_rate_5", [])
    vol_change_rate_10 = Factor("vol_change_rate_10", [])
    vol_change_rate_20 = Factor("vol_change_rate_20", [])
    vol_relative_ratio_0 = Factor("vol_relative_ratio_0", [])
    vol_relative_ratio_1 = Factor("vol_relative_ratio_1", [])
    ar = Factor("ar", [])
    ar_14 = Factor("ar_14", [])
    br = Factor("br", [])
    br_14 = Factor("br_14", [])
    obv_0 = Factor("obv_0", [])
    obv_1 = Factor("obv_1", [])
    vma = Factor("vma", [])
    pvt_0 = Factor("pvt_0", [])
    pvt_1 = Factor("pvt_1", [])
    mfi = Factor("mfi", [])

    # 更新因子数据
    for code in pool:
        # 获取基础数据
        vol_series = si.get_data_series(con, "daily", "vol", code, code_date_serise[code])
        open_series = si.get_data_series(con, "daily", "open", code, code_date_serise[code])
        high_series = si.get_data_series(con, "daily", "high", code, code_date_serise[code])
        low_series = si.get_data_series(con, "daily", "high", code, code_date_serise[code])
        close_series = si.get_data_series(con, "daily", "close", code, code_date_serise[code])


        # 避免时间序列不够长无法进行计算/避免输入空值
        if len(vol_series) > 0:
            vol_change_rate_list = cal_vol_change_rate_n(vol_series, 1)
            vol_change_rate.append_value(si.get_column_tuple_list(code, code_date_serise[code], vol_change_rate_list))

            obv_0_list = cal_obv(close_series, vol_series, 0)
            obv_0.append_value(si.get_column_tuple_list(code, code_date_serise[code], obv_0_list))
            obv_1_list = cal_obv(close_series, vol_series, 1)
            obv_1.append_value(si.get_column_tuple_list(code, code_date_serise[code], obv_1_list))

            pvt_0_list = cal_pvt(close_series, vol_series, 0)
            pvt_0.append_value(si.get_column_tuple_list(code, code_date_serise[code], pvt_0_list))
            pvt_1_list = cal_pvt(close_series, vol_series, 1)
            pvt_1.append_value(si.get_column_tuple_list(code, code_date_serise[code], pvt_1_list))

            vma_list = cal_vma_n(vol_series, 6)
            vma.append_value(si.get_column_tuple_list(code, code_date_serise[code], vma_list))

        if len(vol_series) >= 5:
            # 得到因子
            logvol_list = cal_logvol_n(vol_series, 5)
            vol_change_rate_5_list = cal_vol_change_rate_n(vol_series, 5)
            vol_relative_ratio_0_list = cal_vol_relative_ratio(vol_series, 5, 5, 0)
            vol_relative_ratio_1_list = cal_vol_relative_ratio(vol_series, 5, 5, 1)

            logvol.append_value(si.get_column_tuple_list(code, code_date_serise[code], logvol_list))
            vol_change_rate_5.append_value(
                si.get_column_tuple_list(code, code_date_serise[code], vol_change_rate_5_list))
            vol_relative_ratio_0.append_value(
                si.get_column_tuple_list(code, code_date_serise[code], vol_relative_ratio_0_list))
            vol_relative_ratio_1.append_value(
                si.get_column_tuple_list(code, code_date_serise[code], vol_relative_ratio_1_list))

            mfi_list = cal_mfi(high_series, low_series, close_series, vol_series, 5)
            mfi.append_value(si.get_column_tuple_list(code, code_date_serise[code], mfi_list))

        if len(vol_series) >= 10:
            logvol_10_list = cal_logvol_n(vol_series, 10)
            vol_change_rate_10_list = cal_vol_change_rate_n(vol_series, 10)

            logvol_10.append_value(si.get_column_tuple_list(code, code_date_serise[code], logvol_10_list))
            vol_change_rate_10.append_value(
                si.get_column_tuple_list(code, code_date_serise[code], vol_change_rate_10_list))

        if len(vol_series) >= 14:
            ar_14_list = cal_ar_n(open_series, high_series, low_series, 14)
            br_14_list = cal_br_n(close_series, high_series, low_series, 14)

            ar_14.append_value(si.get_column_tuple_list(code, code_date_serise[code], ar_14_list))
            br_14.append_value(si.get_column_tuple_list(code, code_date_serise[code], br_14_list))

        if len(vol_series) >= 15:
            logvol_15_list = cal_logvol_n(vol_series, 15)

            logvol_15.append_value(si.get_column_tuple_list(code, code_date_serise[code], logvol_15_list))

        if len(vol_series) >= 20:
            logvol_20_list = cal_logvol_n(vol_series, 20)
            vol_change_rate_20_list = cal_vol_change_rate_n(vol_series, 20)

            logvol_20.append_value(si.get_column_tuple_list(code, code_date_serise[code], logvol_20_list))
            vol_change_rate_20.append_value(
                si.get_column_tuple_list(code, code_date_serise[code], vol_change_rate_20_list))

        if len(vol_series) >= 26:
            ar_list = cal_ar_n(open_series, high_series, low_series, 26)
            br_list = cal_br_n(close_series, high_series, low_series, 26)

            ar.append_value(si.get_column_tuple_list(code, code_date_serise[code], ar_list))
            br.append_value(si.get_column_tuple_list(code, code_date_serise[code], br_list))

    # 更新数据库中因子
    si.update_factor_column(con, table_name, logvol.get_name(), logvol.get_value())
    si.update_factor_column(con, table_name, logvol_10.get_name(), logvol_10.get_value())
    si.update_factor_column(con, table_name, logvol_15.get_name(), logvol_15.get_value())
    si.update_factor_column(con, table_name, logvol_20.get_name(), logvol_20.get_value())
    si.update_factor_column(con, table_name, vol_change_rate.get_name(), vol_change_rate.get_value())
    si.update_factor_column(con, table_name, vol_change_rate_5.get_name(), vol_change_rate_5.get_value())
    si.update_factor_column(con, table_name, vol_change_rate_10.get_name(), vol_change_rate_10.get_value())
    si.update_factor_column(con, table_name, vol_change_rate_20.get_name(), vol_change_rate_20.get_value())
    si.update_factor_column(con, table_name, ar.get_name(), ar.get_value())
    si.update_factor_column(con, table_name, br.get_name(), br.get_value())
    si.update_factor_column(con, table_name, ar_14.get_name(), ar_14.get_value())
    si.update_factor_column(con, table_name, br_14.get_name(), br_14.get_value())
    si.update_factor_column(con, table_name, vol_relative_ratio_0.get_name(), vol_relative_ratio_0.get_value())
    si.update_factor_column(con, table_name, vol_relative_ratio_1.get_name(), vol_relative_ratio_1.get_value())
    si.update_factor_column(con, table_name, obv_0.get_name(), obv_0.get_value())
    si.update_factor_column(con, table_name, obv_1.get_name(), obv_1.get_value())
    si.update_factor_column(con, table_name, vma.get_name(), vma.get_value())
    si.update_factor_column(con, table_name, pvt_0.get_name(), pvt_0.get_value())
    si.update_factor_column(con, table_name, pvt_1.get_name(), pvt_1.get_value())
    si.update_factor_column(con, table_name, mfi.get_name(), mfi.get_value())


# n日对数成交量变化率
def cal_logvol_n(vol_series, n):
    return feature.get_logvol(vol_series, period=n, max_period=n)


# n日成交量变化率
def cal_vol_change_rate_n(vol_series, n):
    return feature.get_volume_change_rate(vol_series, period=n, max_period=n)


# OBV 能量潮
def cal_obv(close, vol, mode):
    return feature.get_obv(close, vol, max_period=mode, mode=mode)


# n日成交量量比
def cal_vol_relative_ratio(vol_series, n, p, mode):
    return feature.get_volume_relative_ratio(vol_series, n, period=p, max_period=p, mode=mode)


# 量均线
def cal_vma_n(vol, n):
    return feature.get_vma(vol, period=n, max_period=n)


# 量价趋势
def cal_pvt(close, vol, mode):
    return feature.get_pvt(close, vol, max_period=1, mode=mode)


def cal_mfi(high, low, close, vol, n):
    return feature.get_mfi(high, low, close, vol, period=n, max_period=n)


# AR人气指标
def cal_ar_n(open, high, low, n):
    return feature.get_ar(open, high, low, period=n, max_period=n)


# BR意愿指标
def cal_br_n(close, high, low, n):
    return feature.get_br(close, high, low, period=n, max_period=n)
