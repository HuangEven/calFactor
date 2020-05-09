import calculate_factors.feature as feature


# macd指数平滑异动移动平均线
def macd(close, fastperiod=12, slowperiod=26, signalperiod=9):
    return feature.get_macd(close, max_period=0, fp=fastperiod, sp=slowperiod, s_p=signalperiod)


# ROC变动率
def roc(close, n):
    return feature.get_roc(close, period=n, max_period=0)


# trix
def trix(close, n):
    return feature.get_trix(close, period=n, max_period=0)


# RSI相对强弱指数
def rsi(close, n):
    return feature.get_rsi(close, period=n, max_period=0)


# CCI顺势指标
def cci(close, high, low, n):
    return feature.get_cci(close, high, low, period=n, max_period=0)


# DPO区间震荡线
def dpo(close, ma):
    return feature.get_dpo(close, ma, max_period=0)


# 均线与收盘价之间的距离
def ma_close_dis(close, n):
    return feature.ma_close_dis(close, period=n, max_period=0)


# 计算均线之间的距离
def ma_ma_dis(close, n1, n2):
    return feature.ma_ma_dis(close, n1, n2, max_period=0)


# BIAS乖离率
def cal_bias_n(close, n):
    return feature.get_bias(close, period=n, max_period=0)