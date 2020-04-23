import calculate_factors.feature as feature
from calculate_factors.sql_interact import Factor
import pymysql
import calculate_factors.sql_interact as si


def update_motive_factors(code_date_series):
    con = pymysql.connect(host="localhost", user="root", passwd="mysql", db="stockvision")
    table_name = "motive_factor"
    pool = si.get_all_codes(con)

    macd = Factor("macd", [])
    cci = Factor("cci", [])
    roc = Factor("roc", [])
    trix = Factor("trix", [])
    rsi = Factor("rsi", [])
    dpo = Factor("dpo", [])
    ma_close_dis = Factor("ma_close_dis", [])
    ma_ma_dis = Factor("ma_ma_dis", [])
    bias = Factor("bias", [])
    for code in pool:
        close_series = si.get_data_series(con, "daily", "close", code, code_date_series[code])
        high_series = si.get_data_series(con, "daily", "high", code, code_date_series[code])
        low_series = si.get_data_series(con, "daily", "low", code, code_date_series[code])

        # 保证输入不为空
        if len(close_series) > 0:
            macd_list = cal_macd(close_series)
            print(macd_list, end=',')
            print(code + " " + macd.get_name())
            macd.append_value(si.get_column_tuple_list(code, code_date_series[code], macd_list))

        if len(close_series) >= 5:
            ma_close_dis_list = cal_ma_close_dis(close_series, 5)
            print(macd_list, end=',')
            print(code + " " + ma_close_dis.get_name())
            ma_close_dis.append_value(si.get_column_tuple_list(code, code_date_series[code], ma_close_dis_list))

        if len(close_series) >= 6:
            bias_list = cal_bias_n(close_series, 6)
            bias.append_value(si.get_column_tuple_list(code, code_date_series[code], bias_list))

        if len(close_series) >= 10:
            # dpo_list = cal_dpo(close_series, 10)
            # dpo.append_value(si.get_column_tuple_list(code, code_date_series[code], dpo_list))

            ma_ma_dis_list = cal_ma_ma_dis(close_series, 5, 10)
            ma_ma_dis.append_value(si.get_column_tuple_list(code, code_date_series[code], ma_ma_dis_list))

        if len(close_series) >= 12:
            trix_list = cal_trix(close_series, 12)
            trix.append_value(si.get_column_tuple_list(code, code_date_series[code], trix_list))

            roc_list = cal_roc_n(close_series, 12)
            roc.append_value(si.get_column_tuple_list(code, code_date_series[code], roc_list))

        if len(close_series) >= 14:
            rsi_list = cal_rsi(close_series, 14)
            rsi.append_value(si.get_column_tuple_list(code, code_date_series[code], rsi_list))

            cci_list = cal_cci_n(close_series, high_series, low_series, 14)
            cci.append_value(si.get_column_tuple_list(code, code_date_series[code], cci_list))

    si.update_factor_column(con, table_name, macd.get_name(), macd.get_value())
    si.update_factor_column(con, table_name, cci.get_name(), cci.get_value())
    si.update_factor_column(con, table_name, roc.get_name(), roc.get_value())
    si.update_factor_column(con, table_name, trix.get_name(), trix.get_value())
    si.update_factor_column(con, table_name, rsi.get_name(), rsi.get_value())
    # si.update_factor_column(con, table_name, dpo.get_name(), dpo.get_value())
    si.update_factor_column(con, table_name, ma_close_dis.get_name(), ma_close_dis.get_value())
    si.update_factor_column(con, table_name, ma_ma_dis.get_name(), ma_ma_dis.get_value())
    si.update_factor_column(con, table_name, bias.get_name(), bias.get_value())


# macd指数平滑异动移动平均线
def cal_macd(close, fastperiod=12, slowperiod=26, signalperiod=9):
    return feature.get_macd(close, max_period=0, fp=fastperiod, sp=slowperiod, s_p=signalperiod)


# ROC变动率
def cal_roc_n(close, n):
    return feature.get_roc(close, period=n, max_period=0)


# trix
def cal_trix(close, n):
    return feature.get_trix(close, period=n, max_period=0)


# RSI相对强弱指数
def cal_rsi(close, n):
    return feature.get_rsi(close, period=n, max_period=0)


# CCI顺势指标
def cal_cci_n(close, high, low, n):
    return feature.get_cci(close, high, low, period=n, max_period=0)


# DPO区间震荡线
def cal_dpo(close, ma):
    return feature.get_dpo(close, ma, max_period=0)


# 均线与收盘价之间的距离
def cal_ma_close_dis(close, n):
    return feature.ma_close_dis(close, period=n, max_period=0)


# 计算均线之间的距离
def cal_ma_ma_dis(close, n1, n2):
    return feature.ma_ma_dis(close, n1, n2, max_period=0)


# BIAS乖离率
def cal_bias_n(close, n):
    return feature.get_bias(close, period=n, max_period=0)
