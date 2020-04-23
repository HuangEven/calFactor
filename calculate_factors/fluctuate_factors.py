import calculate_factors.feature as feature
import calculate_factors.sql_interact as si
from calculate_factors.sql_interact import Factor
import pymysql

"""
def get_fluctuate_factors(tsCode, startDate, endDate):
    df = js.get_daily(tsCode, startDate, endDate)
    close_series = df['close']
    high_series = df['high']
    low_series = df['low']

    ret = df[['tsCode', 'tradeDate']]
    ret['amplitude']= feature.get_amplitude(close_series, high_series, low_series)
    ret['priceEfficiency']= feature.get_price_efficiency(close_series)
    ret['priceCenter']= feature.get_price_center(close_series)
    ret['avgDailyAmplitude']= feature.get_avg_daily_amplitude(close_series, high_series, low_series)
    ret['macd']= feature.get_macd(close_series)


    return
"""


def update_fluctuate_factor(code_date_series):
    con = pymysql.connect(host="localhost", user="root", passwd="mysql", db="stockvision")
    table_name = "fluctuate_factor"
    pool = si.get_all_codes(con)

    amplitude = Factor("amplitude", [])
    amplitude_10 = Factor("amplitude_10", [])
    price_efficiency = Factor("price_efficiency", [])
    price_efficiency_10 = Factor("price_efficiency_10", [])
    price_center = Factor("price_center", [])
    avg_daily_amplitude = Factor("avg_daily_amplitude", [])
    atr = Factor("atr", [])
    boll_upperband = Factor("boll_upperband", [])
    boll_middleband = Factor("boll_middleband", [])
    boll_lowerband = Factor("boll_lowerband", [])

    # 更新因子数据
    for code in pool:
        # 获取基础数据
        close_series = si.get_data_series(con, "daily", "close", code, code_date_series[code])
        high_series = si.get_data_series(con, "daily", "high", code, code_date_series[code])
        low_series = si.get_data_series(con, "daily", "low", code, code_date_series[code])

        # 避免时间序列不够长无法进行计算
        if len(close_series) >= 5:
            # 得到因子
            amplitude_list = cal_amplitude_n(close_series, high_series, low_series, 5)
            amplitude.append_value(si.get_column_tuple_list(code, code_date_series[code], amplitude_list))

            price_efficiency_list = cal_price_efficiency_n(close_series, 5)
            price_efficiency.append_value(si.get_column_tuple_list(code, code_date_series[code], price_efficiency_list))

            price_center_list = cal_price_center_n_ma(close_series, 3, 5)
            price_center.append_value(si.get_column_tuple_list(code, code_date_series[code], price_center_list))

            avg_daily_amplitude_list = cal_avg_daily_amplitude_n(close_series, high_series, low_series, 5)
            avg_daily_amplitude.append_value(
                si.get_column_tuple_list(code, code_date_series[code], avg_daily_amplitude_list))

            atr_list = cal_atr(high_series, low_series, close_series, 5)
            atr.append_value(si.get_column_tuple_list(code, code_date_series[code], atr_list))

        if len(close_series) >= 10:
            amplitude_10_list = cal_amplitude_n(close_series, high_series, low_series, 10)
            amplitude_10.append_value(si.get_column_tuple_list(code, code_date_series[code], amplitude_10_list))

            price_efficiency_10_list = cal_price_efficiency_n(close_series, 10)
            price_efficiency_10.append_value(
                si.get_column_tuple_list(code, code_date_series[code], price_efficiency_10_list))

        if len(close_series) >= 20:
            boll_upperband_list, boll_middleband_list, boll_lowerband_list = cal_boll(close_series, 20)
            boll_upperband.append_value(si.get_column_tuple_list(code, code_date_series[code], boll_upperband_list))
            boll_middleband.append_value(si.get_column_tuple_list(code, code_date_series[code], boll_middleband_list))
            boll_lowerband.append_value(si.get_column_tuple_list(code, code_date_series[code], boll_lowerband_list))

    # 更新数据库中因子
    si.update_factor_column(con, table_name, amplitude.get_name(), amplitude.get_value())
    si.update_factor_column(con, table_name, amplitude_10.get_name(), amplitude_10.get_value())
    si.update_factor_column(con, table_name, price_efficiency.get_name(), price_efficiency.get_value())
    si.update_factor_column(con, table_name, price_efficiency_10.get_name(), price_efficiency_10.get_value())
    si.update_factor_column(con, table_name, price_center.get_name(), price_center.get_value())
    si.update_factor_column(con, table_name, avg_daily_amplitude.get_name(), avg_daily_amplitude.get_value())
    si.update_factor_column(con, table_name, atr.get_name(), atr.get_value())
    si.update_factor_column(con, table_name, boll_upperband.get_name(), boll_upperband.get_value())
    si.update_factor_column(con, table_name, boll_middleband.get_name(), boll_middleband.get_value())
    si.update_factor_column(con, table_name, boll_lowerband.get_name(), boll_lowerband.get_value())


# n日振幅
def cal_amplitude_n(close, high, low, n):
    return feature.get_amplitude(close, high, low, period=n, max_period=n)


# n日价格轨迹效率
def cal_price_efficiency_n(price_series, n):
    return feature.get_price_efficiency(price_series, period=n, max_period=n)


# n日价格重心
def cal_price_center_n_ma(price_series, n, ma):
    return feature.get_price_center(price_series, period=n, ma_period=ma, max_period=n)


# n日平均振幅
def cal_avg_daily_amplitude_n(close, high, low, n):
    return feature.get_avg_daily_amplitude(close, high, low, period=n, max_period=0)


# ATR平均真实波动幅度
def cal_atr(high, low, close, n):
    return feature.get_atr(high, low, close, period=n, max_period=0)


# boll
def cal_boll(close, n):
    return feature.get_boll(close, n, max_period=0)
