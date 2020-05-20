import calculate_factors.feature as feature


# n日振幅
def amplitude(params):
    try:
        close=params[0]
        high=params[1]
        low=params[2]
        n=params[3]
    except Exception as e:
        print(e)
        return None

    return feature.get_amplitude(close, high, low, period=n, max_period=n)


# n日价格轨迹效率
def price_efficiency(params):
    try:
        price_series=params[0]
        n=params[1]
    except Exception as e:
        print(e)
        return None

    return feature.get_price_efficiency(price_series, period=n, max_period=n)


# n日价格重心
def price_center(params):
    try:
        price_series=params[0]
        n=params[1]
        ma=params[2]
    except Exception as e:
        print(e)
        return None

    return feature.get_price_center(price_series, period=n, ma_period=ma, max_period=n)


# n日平均振幅
def avg_daily_amplitude(params):
    try:
        close=params[0]
        high=params[1]
        low=params[2]
        n=params[3]
    except Exception as e:
        print(e)
        return None

    return feature.get_avg_daily_amplitude(close, high, low, period=n, max_period=0)


# ATR平均真实波动幅度
def atr(params):
    try:
        high=params[0]
        low=params[1]
        close=params[2]
        n=params[3]
    except Exception as e:
        print(e)
        return None

    return feature.get_atr(high, low, close, period=n, max_period=0)


# boll
def boll(params):
    try:
        close=params[0]
        n=params[1]
    except Exception as e:
        print(e)
        return None

    return feature.get_boll(close, n, max_period=0)

