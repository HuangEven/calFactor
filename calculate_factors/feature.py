# -*- coding:utf-8 -*-

import numpy as np
import talib
import pandas as pd
from scipy.stats import boxcox
import matplotlib.pyplot as plt
from decimal import *


###############################################################################################################
# Market Indicators
# N日对数收益率
# @param: price_series 价格序列
#         period 计算周期，n日对数收益率
#         max_period 最大计算周期，所有属性中最大的计算周期
def get_logreturn(price_series, period=5, max_period=10):
    return (np.log(np.array(price_series[period:])) - np.log(np.array(price_series[:-period])))[(max_period - period):]


# N日绝对收益率
def get_return(price_series, period=1, max_period=10):
    return ((np.array(price_series[period:]) - np.array(price_series[:-period])) /
            np.array(price_series[:-period]))[(max_period - period):]


# N日对数成交量变化率
# @param: vol_series 成交量序列
#         period 计算周期，n日对数收益率
#         max_period 最大计算周期，所有属性中最大的计算周期
def get_logvol(vol_series, period=5, max_period=10):
    return (np.log(np.array(vol_series[period:])) - np.log(np.array(vol_series[:-period])))[(max_period - period):]


# N日成交量变化率
def get_volume_change_rate(vol_series, period=1, max_period=10):
    return ((np.array(vol_series[period:]) - np.array(vol_series[:-period])) / np.array(vol_series[:-period]))[
           (max_period - period):]


# 计算成交量量比
# @param: n 成交量计算天数
#         period 对比间隔天数
#         mode 模式，0时间段成交量与历史比; 1，单日成交量与历史比 默认为0
def get_volume_relative_ratio(vol_series, n=5, period=5, max_period=10, mode=0):
    #     # N日成交量/N日成交量
    if mode == 0:
        # 采用N日平均成交量作为替代进行简便计算
        vol_ma = talib.MA(np.array(vol_series, dtype=np.double), timeperiod=n)[(n - 1):]  # 成交量移动平均
        return (vol_ma[period:] / np.array(vol_ma)[:-period])[max_period - n + 1 - period:]
    # 今日成交量/过去N日平均每日成交量
    elif mode == 1:
        vol_ma = talib.MA(np.array(vol_series, dtype=np.double), timeperiod=n)  # 成交量移动平均
        print(len(np.array(vol_series)[n:] / vol_ma[n - 1:-1]))
        return np.array(vol_series)[n:] / vol_ma[n - 1:-1]


# 计算n日振幅
# 根据n日最高最低价格计算n日振幅(标准振幅定义)
# @date: 11.14
# @version: 1.0 checked
# @param: high 最高价序列
#         low 最低价序列
#         period 计算周期，n日对数收益率
#         max_period 最大计算周期，所有属性中最大的计算周期
def get_amplitude(close, high, low, period=5, max_period=10):
    high_period = np.array(high)  # 每日最高价
    low_period = np.array(low)  # 每日最低价
    for i in range(1, period):
        high_period = np.maximum(high_period[1:], np.array(high)[:-i])  # 计算N日的最高价
    for i in range(1, period):
        low_period = np.minimum(low_period[1:], np.array(low)[:-i])  # 计算N日的最低价
    amplitude = (high_period[1:] - low_period[1:]) / np.array(close[:-period])
    return amplitude[(max_period - period):]


# 计算N日价格轨迹效率
# @date: 11.13
# @version: 1.0 checked
# @param: price_series 价格序列
#         period 计算周期，N日价格轨迹效率
#         max_period 最大计算周期，所有属性中最大的计算周期
def get_price_efficiency(price_series, period=5, max_period=10):
    # 绝对位移长度
    abs_dis = np.abs(np.array(price_series[period:]) - np.array(price_series[:-period]))
    # 价格路程长度
    path_dis = 0
    for i in range(1, period + 1):
        if i == period:
            path_dis += np.abs(np.array(price_series[i:]) - np.array(price_series[(i - 1):-1]))
        else:
            path_dis += np.abs(np.array(price_series[i:]) - np.array(price_series[(i - 1):-1]))[:(i - period)]
    e = (abs_dis / path_dis)[(max_period - period):]
    return e


# 计算价格重心
# 使用5日移动平均线作为价格重心
# @param: period 重心高低对比间隔，例如：period=3，则将今日5日平均-3日前的5日平均
def get_price_center(close, period=3, ma_period=5, max_period=10):
    ma = talib.MA(np.array(close), timeperiod=ma_period)
    # ma = ma[(ma_period-1):]   # 去除NaN
    return ((ma[period:] - ma[:-period]) / ma[:-period])[max_period - period:]


# 计算n日平均振幅
# 计算每日振幅，计算n日平均振幅
# @date: 11.14
# @param: high 最高价序列
#         low 最低价序列
#         period 计算周期，n日对数收益率
#         max_period 最大计算周期，所有属性中最大的计算周期
def get_avg_daily_amplitude(close, high, low, period=5, max_period=10):
    daily_amplitude = (np.array(high[1:]) - np.array(low[1:])) / np.array(close[:-1])  # 每日振幅
    avg_amplitude = daily_amplitude[(period - 1):]
    for i in range(0, period - 1):
        avg_amplitude += daily_amplitude[i:-(period - i - 1)]
    return avg_amplitude[(max_period - period):] / period


###############################################################################################################
# Technical Indicators
# MACD 指数平滑异动移动平均线
def get_macd(close, fp=12, sp=26, s_p=9, max_period=10):
    macd, signal, hist = talib.MACD(np.array(close), fastperiod=fp, slowperiod=sp, signalperiod=s_p)
    hist *= 2
    return hist[max_period:]


# ATR 平均真实波动幅度
def get_atr(high, low, close, period, max_period=10):
    atr = talib.ATR(np.array(high), np.array(low), np.array(close), timeperiod=period)
    atr, maxlog = boxcox(atr)  # 进行Box-Cox变换使得更符合正态分布
    return atr[max_period:]


# RSI 相对强弱指数
def get_rsi(close, period=14, max_period=10):
    rsi = talib.RSI(np.array(close), timeperiod=period)
    rsi, maxlog = boxcox(rsi)
    return rsi[max_period:]


# CCI 顺势指标
def get_cci(close, high, low, period=14, max_period=10):
    cci = talib.CCI(np.array(high), np.array(low), np.array(close), period)
    return cci[max_period:]


# OBV 能量潮
# 变体: 返回OBV每日变化量
# @param: mode计算模式: 0 常规OBV定义; 1 变体OBV
def get_obv(close, volume, max_period=10, mode=0):
    obv = talib.OBV(np.array(close), np.array(volume, dtype=np.double))
    if mode == 0:
        return obv[max_period:]
    if mode == 1:
        obv = obv[1:] - obv[:-1]
        return obv[max_period - 1:]


# ROC 变动率指标
def get_roc(close, period=12, max_period=10):
    roc = talib.ROC(np.array(close), timeperiod=period)
    return roc[max_period:]


# BIAS 乖离率
def get_bias(close, period=6, max_period=10):
    ma = talib.MA(np.array(close), timeperiod=period)
    bias = ((np.array(close) - ma) / np.array(ma))[max_period:]
    return bias


# VMA 量均线
def get_vma(volume, period=6, max_period=10):
    vma = talib.MA(np.array(volume, dtype=np.double), timeperiod=period)
    return vma[max_period:]


# PVT 量价趋势
# mode: 0 原指标 1 每天变化值
def get_pvt(close, volume, max_period=10, mode=0):
    close = np.array(close)
    pvt = (close[1:] - close[:-1]) / close[:-1] * volume[1:]
    if mode == 1:
        return pvt[max_period - 1:]
    elif mode == 0:
        pvt = pvt.cumsum()
        return pvt[max_period - 1:]


def get_mfi(high, low, close, volume, period, max_period):
    mfi = talib.MFI(np.array(high), np.array(low), np.array(close),
                    np.array(volume, dtype=np.double), timeperiod=period)
    return mfi[max_period:]


# DPO 区间震荡线
def get_dpo(close, ma_period=10, max_period=10):
    close = np.array(close)
    ma = talib.MA(close, timeperiod=ma_period)
    x=int(ma_period * 1.5)
    dpo = close[x:] - ma[ma_period - 1:]
    return dpo


# 均线与收盘价之间的距离
# @update: 12.4 计算绝对距离意义不大，更新为相对距离
def ma_close_dis(close, period, max_period=10):
    ma = talib.MA(np.array(close), timeperiod=period)
    dis = ((np.array(close) - ma) / np.array(close))[max_period:]
    return dis


# 计算均线之间的距离
# @update: 12.4 计算绝对距离意义不大，更新为相对距离
def ma_ma_dis(close, period1, period2, max_period=10):
    ma1 = talib.MA(np.array(close), timeperiod=period1)
    ma2 = talib.MA(np.array(close), timeperiod=period2)
    return ((ma1 - ma2) / np.array(close))[max_period:]


def get_trix(close, period=12, max_period=10):
    trix = talib.TRIX(np.array(close), timeperiod=period)
    return trix[max_period:]


def get_boll(close, period=20, max_period=10):
    upperband, middleband, lowerband = talib.BBANDS(np.array(close), timeperiod=period, nbdevup=2, nbdevdn=2, matype=0)
    return upperband[max_period:], middleband[max_period:], lowerband[max_period:]


# AR 人气指标
def get_ar(open_p, high, low, period=14, max_period=10):
    ar_result = []  # AR值数组
    df_len = len(open_p)  # 获取数据长度
    if df_len < period:  # 数据长度小于计算周期
        return None
    high_open = np.array(high) - np.array(open_p)  # 当天最高价-当天开盘价
    open_low = np.array(open_p) - np.array(low)  # 当天开盘价-当天最低价
    for i in range(1, df_len + 1):
        if i <= period:  # 当i小于数据长度(无法计算AR)
            ar_result.append(0)  # 将0赋值给AR数组的头n天
            continue
        h_o = high_open[(i - period):i]  # 获取从第(i-period)天至第i天的最高价-开盘价
        o_l = open_low[(i - period):i]  # 获取从第(i-period)天至第i天的开盘价-最低价
        h_o_sum = sum(h_o)  # 计算N天内的最高价-开盘价的和
        o_l_sum = sum(o_l)  # 计算N天内的开盘价-最低价的和
        if o_l_sum == 0:  # 保证分母不为0
            o_l_sum = 0.01
        # 人气指数（AR）
        ar = 100 * h_o_sum / o_l_sum
        ar_result.append(ar)
    # 返回AR结果
    return np.array(ar_result)[max_period:]


# BR 意愿指标
# 由于需要使用间隔天的数据，因此实际BR所需的数据为period+1
def get_br(close, high, low, period=14, max_period=10):
    br_result = []  # BR值数组
    df_len = len(close)  # 获取数据长度
    if df_len < period:  # 数据长度小于计算周期
        return None
    high_close = np.array(high)[1:] - np.array(close)[:-1]  # 当天最高价-前一日收盘价
    close_low = np.array(close)[:-1] - np.array(low)[1:]  # 前一日收盘价-当天最低价
    high_close = np.append(0, high_close)
    close_low = np.append(0, close_low)
    for i in range(1, df_len + 1):
        if i <= period:  # 当i小于数据长度(无法计算AR)
            br_result.append(0)  # 将0赋值给BR数组的头n天
            continue
        h_c = high_close[(i - period):i]  # 获取从第(i-period)天至第i天的最高价-前一日收盘价
        c_l = close_low[(i - period):i]  # 获取从第(i-period)天至第i天的前一日收盘价-最低价
        h_c_sum = sum(h_c)  # 计算N天内的最高价-前一日开盘价的和
        c_l_sum = sum(c_l)  # 计算N天内的前一日收盘价-最低价的和
        if c_l_sum == 0:
            c_l_sum = 0.01
        # 意愿指标（BR）
        br = 100 * h_c_sum / c_l_sum
        br_result.append(br)
    # 返回BR结果
    return np.array(br_result)[max_period:]


# 生成新属性
# @param: mp 生成属性的最大周期
def get_attrs(df0, mp):
    close = df0['close']
    high = df0['high']
    low = df0['low']
    volume = df0['volume']
    open_p = df0['open']
    amt = df0['amt']
    df = df0[mp:]

    df['volume_relative_ratio5'] = get_volume_relative_ratio(volume, n=5, period=5, max_period=mp, mode=0)  # 5日成交量比
    df['volume_relative_ratio10'] = get_volume_relative_ratio(volume, n=10, period=10, max_period=mp, mode=0)  # 10日成交量比
    df['volume_relative_ratio20'] = get_volume_relative_ratio(volume, n=20, period=20, max_period=mp, mode=0)  # 20日成交量比
    df['volume_relative_ratio30'] = get_volume_relative_ratio(volume, n=30, period=30, max_period=mp, mode=0)  # 30日成交量比

    df['price_center5_5'] = get_price_center(close, period=1, ma_period=5, max_period=mp)  # 价格重心 今日5日平均-1日前的5日平均
    df['price_center5_10'] = get_price_center(close, period=10, ma_period=5, max_period=mp)  # 价格重心 今日5日平均-10日前的5日平均
    df['price_center5_20'] = get_price_center(close, period=20, ma_period=5, max_period=mp)  # 价格重心 今日5日平均-20日前的5日平均
    df['price_center10_5'] = get_price_center(close, period=5, ma_period=10, max_period=mp)  # 价格重心 今日10日平均-5日前的10日平均
    df['price_center10_10'] = get_price_center(close, period=10, ma_period=10, max_period=mp)  # 价格重心 今日10日平均-10日前的10日平均
    df['price_center10_20'] = get_price_center(close, period=20, ma_period=10, max_period=mp)  # 价格重心 今日10日平均-20日前的10日平均
    df['price_center20_20'] = get_price_center(close, period=20, ma_period=20, max_period=mp)  # 价格重心 今日20日平均-20日前的20日平均
    df['price_center30_30'] = get_price_center(close, period=30, ma_period=30, max_period=mp)  # 价格重心 今日30日平均-30日前的30日平均

    df['ma5_close_dis'] = ma_close_dis(close, period=5, max_period=mp)  # 当日收盘价与5日均线的距离
    df['ma10_close_dis'] = ma_close_dis(close, period=10, max_period=mp)  # 当日收盘价与10日均线的距离
    df['ma20_close_dis'] = ma_close_dis(close, period=20, max_period=mp)  # 当日收盘价与20日均线的距离
    df['ma30_close_dis'] = ma_close_dis(close, period=30, max_period=mp)  # 当日收盘价与30日均线的距离
    df['ma60_close_dis'] = ma_close_dis(close, period=60, max_period=mp)  # 当日收盘价与60日均线的距离

    df['bias'] = get_bias(close, period=6, max_period=mp)  # 6日乖离率

    df['ma5_ma10_dis'] = ma_ma_dis(close, period1=5, period2=10, max_period=mp)  # ma5与ma10之间的距离
    df['ma5_ma20_dis'] = ma_ma_dis(close, period1=5, period2=20, max_period=mp)  # ma5与ma20之间的距离
    df['ma10_ma20_dis'] = ma_ma_dis(close, period1=10, period2=20, max_period=mp)  # ma10与ma20之间的距离
    df['ma20_ma30_dis'] = ma_ma_dis(close, period1=20, period2=30, max_period=mp)  # ma20与ma30之间的距离
    df['ma30_ma60_dis'] = ma_ma_dis(close, period1=30, period2=60, max_period=mp)  # ma30与ma60之间的距离

    df['atr14'] = get_atr(high, low, close, period=14, max_period=mp)  # 14天ATR
    df['atr28'] = get_atr(high, low, close, period=28, max_period=mp)  # 28天ATR
    df['rsi14'] = get_rsi(close, period=14, max_period=mp)  # 14天RSI
    df['cci14'] = get_cci(high, low, close, period=14, max_period=mp)  # 14天CCI

    df['obv'] = get_obv(close, volume, max_period=mp)  # OBV
    df['obv_daily_change'] = get_obv(close, volume, max_period=mp, mode=1)  # OBV daily change
    df['roc'] = get_roc(close, period=12, max_period=mp)  # ROC
    df['trix'] = get_trix(close, period=12, max_period=mp)  # TRIX
    df['mfi'] = get_mfi(high, low, close, volume, period=14, max_period=mp)  # MFI
    df['boll_up'], df['boll_mid'], df['boll_dn'] = get_boll(close, period=20, max_period=mp)  # BOLL轨道
    df['ar13'] = get_ar(open_p, high, low, period=13, max_period=mp)  # 13天AR
    df['br13'] = get_br(close, high, low, period=13, max_period=mp)  # 13天BR
    # ar26, br26 = get_arbr(open_p, close, high, low, period=26, max_period=mp)     # 26天AR,BR
    df['vma6'] = get_vma(volume, period=6, max_period=mp)  # 6日量均线
    df['pvt'] = get_pvt(close, volume, max_period=mp, mode=1)  # PVT
    # df['dpo'] = get_dpo(close, ma_period=10, max_period=mp)                       # DPO 区间震荡线

    df['open_logreturn'] = get_logreturn(open_p, period=1, max_period=mp)  # 1日对数开盘价涨跌幅
    df['close_logreturn'] = get_logreturn(close, period=1, max_period=mp)  # 1日对数收盘价涨跌幅

    df['close_return'] = get_return(close, period=1, max_period=mp)  # 1日绝对收益率
    df['logreturn5'] = get_logreturn(close, period=5, max_period=mp)  # 5日对数收益率
    df['logreturn10'] = get_logreturn(close, period=10, max_period=mp)  # 10日对数收益率
    df['logreturn20'] = get_logreturn(close, period=20, max_period=mp)  # 20日对数收益率
    df['logreturn30'] = get_logreturn(close, period=30, max_period=mp)  # 30日对数收益率
    df['logreturn60'] = get_logreturn(close, period=60, max_period=mp)  # 60日对数收益率

    df['diffreturn'] = (np.log(np.array(high[mp:])) - np.log(np.array(low[mp:])))  # 对数高低价差

    df['logvol'] = get_logvol(volume, period=1, max_period=mp)  # 当日对数成交量增长率
    df['logvol5'] = get_logvol(volume, period=5, max_period=mp)  # 5日成交量对数变化
    df['logvol10'] = get_logvol(volume, period=10, max_period=mp)  # 10日成交量对数变化
    df['logvol20'] = get_logvol(volume, period=20, max_period=mp)  # 20日成交量对数变化
    df['logvol30'] = get_logvol(volume, period=30, max_period=mp)  # 30日成交量对数变化
    df['logvol60'] = get_logvol(volume, period=60, max_period=mp)  # 60日成交量对数变化

    df['vol_change_rate'] = get_volume_change_rate(volume, period=1, max_period=mp)  # 1日成交量变化率

    df['price_e5'] = get_price_efficiency(close, period=5, max_period=mp)  # 5日价格轨迹效率
    df['price_e10'] = get_price_efficiency(close, period=10, max_period=mp)  # 10日价格轨迹效率
    df['price_e20'] = get_price_efficiency(close, period=20, max_period=mp)  # 20日价格轨迹效率
    df['price_e30'] = get_price_efficiency(close, period=30, max_period=mp)  # 30日价格轨迹效率
    df['price_e60'] = get_price_efficiency(close, period=60, max_period=mp)  # 60日价格轨迹效率

    df['amplitude'] = get_amplitude(close, high, low, period=1, max_period=mp)  # 1日振幅
    df['amplitude5'] = get_amplitude(close, high, low, period=5, max_period=mp)  # 5日振幅
    df['amplitude10'] = get_amplitude(close, high, low, period=10, max_period=mp)  # 10日振幅
    df['amplitude20'] = get_amplitude(close, high, low, period=20, max_period=mp)  # 20日振幅
    df['amplitude30'] = get_amplitude(close, high, low, period=30, max_period=mp)  # 30日振幅
    df['amplitude60'] = get_amplitude(close, high, low, period=60, max_period=mp)  # 60日振幅

    df['macd'] = get_macd(close, max_period=mp)  # MACD

    return df
