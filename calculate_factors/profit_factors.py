import calculate_factors.feature as feature
import calculate_factors.sql_interact as si
from calculate_factors.sql_interact import Factor

import pymysql

"""
def get_profit_factors(tsCode, startDate, endDate):
    df = js.get_daily(tsCode, startDate, endDate)
    close_series = df['close']
    high_series = df['high']
    low_series = df['low']

    df['logreturn']

    return

"""


def update_profit_factor(code_date_serise):
    con = pymysql.connect(host="localhost", user="root", passwd="mysql", db="stockvision")
    table_name = "profit_factor"
    pool = si.get_all_codes(con)

    logreturn_5 = Factor("logreturn_5", [])
    logreturn_10 = Factor("logreturn_10", [])
    logreturn_15 = Factor("logreturn_15", [])
    logreturn_20 = Factor("logreturn_20", [])
    return_5 = Factor("return_5", [])
    return_10 = Factor("return_10", [])
    return_15 = Factor("return_15", [])
    return_20 = Factor("return_20", [])

    # 更新因子数据
    for code in pool:
        # 获取基础数据
        close_serise = si.get_data_series(con, 'daily', 'close', code, code_date_serise[code])

        # 避免时间序列不够长无法进行计算
        if len(close_serise) >= 5:
            # 得到因子
            logreturn_5_list = cal_logreturn_n(close_serise, 5)
            return_5_list = cal_return_n(close_serise, 5)

            logreturn_5.append_value(si.get_column_tuple_list(code, code_date_serise[code], logreturn_5_list))
            return_5.append_value(si.get_column_tuple_list(code, code_date_serise[code], return_5_list))

        if len(close_serise) >= 10:
            logreturn_10_list = cal_logreturn_n(close_serise, 10)
            return_10_list = cal_return_n(close_serise, 10)

            logreturn_10.append_value(si.get_column_tuple_list(code, code_date_serise[code], logreturn_10_list))
            return_10.append_value(si.get_column_tuple_list(code, code_date_serise[code], return_10_list))

        if len(close_serise) >= 15:
            logreturn_15_list = cal_logreturn_n(close_serise, 15)
            return_15_list = cal_return_n(close_serise, 15)

            logreturn_15.append_value(si.get_column_tuple_list(code, code_date_serise[code], logreturn_15_list))
            return_15.append_value(si.get_column_tuple_list(code, code_date_serise[code], return_15_list))

        if len(close_serise) >= 20:
            logreturn_20_list = cal_logreturn_n(close_serise, 20)
            return_20_list = cal_return_n(close_serise, 20)

            logreturn_20.append_value(si.get_column_tuple_list(code, code_date_serise[code], logreturn_20_list))
            return_20.append_value(si.get_column_tuple_list(code, code_date_serise[code], return_20_list))

    # 更新数据库中因子
    si.update_factor_column(con, table_name, logreturn_5.get_name(), logreturn_5.get_value())
    si.update_factor_column(con, table_name, logreturn_10.get_name(), logreturn_10.get_value())
    si.update_factor_column(con, table_name, logreturn_15.get_name(), logreturn_15.get_value())
    si.update_factor_column(con, table_name, logreturn_20.get_name(), logreturn_20.get_value())
    si.update_factor_column(con, table_name, return_5.get_name(), return_5.get_value())
    si.update_factor_column(con, table_name, return_10.get_name(), return_10.get_value())
    si.update_factor_column(con, table_name, return_15.get_name(), return_15.get_value())
    si.update_factor_column(con, table_name, return_20.get_name(), return_20.get_value())


# n日对数收益率
def cal_logreturn_n(prices, n):
    return feature.get_logreturn(prices, period=n, max_period=n)


# n日绝对收益率
def cal_return_n(prices, n):
    return feature.get_return(prices, period=n, max_period=n)
