import calculate_factors.feature as feature

# n日对数成交量变化率
def logvol(params):
    try:
        vol_series=params[0]
        n=params[1]
    except Exception as e:
        print(e)
        return None     # 参数个数可能错误
    return feature.get_logvol(vol_series, period=n, max_period=n)


# n日成交量变化率
def vol_change_rate(params):
    try:
        vol_series=params[0]
        n=params[1]
    except Exception as e:
        print(e)
        return None
    return feature.get_volume_change_rate(vol_series, period=n, max_period=n)


# OBV 能量潮
def obv(params):
    try:
        close=params[0]
        vol=params[1]
        mode=params[2]
    except Exception as e:
        print(e)
        return None
    return feature.get_obv(close, vol, max_period=mode, mode=mode)


# n日成交量量比
def vol_relative_ratio(params):
    try:
        vol_series=params[0]
        n=params[1]
        p=params[2]
        mode=params[3]
    except Exception as e:
        print(e)
        return None
    return feature.get_volume_relative_ratio(vol_series, n, period=p, max_period=p, mode=mode)


# 量均线
def vma(params):
    try:
        vol=params[0]
        n=params[1]
    except Exception as e:
        print(e)
        return None
    return feature.get_vma(vol, period=n, max_period=n)


# 量价趋势
def pvt(params):
    try:
        close=params[0]
        vol=params[1]
        mode=params[2]
    except Exception as e:
        print(e)
        return None
    return feature.get_pvt(close, vol, max_period=1, mode=mode)


def mfi(params):
    try:
        high=params[0]
        low=params[1]
        close=params[2]
        vol=params[3]
        n=params[4]
    except Exception as e:
        print(e)
        return None
    return feature.get_mfi(high, low, close, vol, period=n, max_period=n)


# AR人气指标
def ar(params):
    try:
        open=params[0]
        high=params[1]
        low=params[2]
        n=params[3]
    except Exception as e:
        print(e)
        return None
    return feature.get_ar(open, high, low, period=n, max_period=n)


# BR意愿指标
def br(close, high, low, n,params):
    try:
        close=params[0]
        high=params[1]
        low=params[2]
        n=params[3]
    except Exception as e:
        print(e)
        return None
    return feature.get_br(close, high, low, period=n, max_period=n)
