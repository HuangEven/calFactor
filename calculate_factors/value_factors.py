import pandas as pd

"""
def get_value_factors(tsCode, startDate,endDate):
    df = js.get_daily(tsCode, startDate, endDate)
    close_series = df['close']
    high_series = df['high']
    low_series = df['low']

    df['logreturn']


    return value_factors


# 市现率（Price Cash Flow Ratio）
# 市现率是股票价格与每股现金流量的比率，可用于评价股票的价格水平和风险水平
# 每股现金流量是公司经营活动所产生的净现金流量减去优先股股利与流通在外的普通股股数的比率
# 每股现金流量简化——公司经营活动所产生的净现金流量/总股本
def cal_pcf(data):
    daily_basic_df = js.get_daily_basic_by_date(data)
    cashflow_df = js.get_cashflow_by_date(data)
    daily_df = js.get_daily_by_date(data)

    ret = pd.merge(daily_basic_df, cashflow_df, on='tsCode')
    ret = pd.merge(daily_df, ret, on='tsCode')
    ret['pcf'] = ret['close'] / (ret['nCashflowInvAct'] / ret['totalShare'])

    return ret['pcf']
"""

