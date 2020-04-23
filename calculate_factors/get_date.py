#!/usr/bin/env python 
# -*- coding:utf-8 -*-
import datetime
from datetime import timedelta
import time


def gen_dates(b_date, days):
    day = timedelta(days=1)
    # print(day)
    for i in range(days):
        # print(b_date + day*i)
        yield b_date + day * i


def get_date_list(start_date, end_date):  # end_date=None
    """
    获取日期列表
    :param start: 开始日期
    :param end: 结束日期
    :return:
    """
    if start_date is not None:
        start = datetime.datetime.strptime(start_date, "%Y%m%d")
    if end_date is None:
        end = datetime.datetime.now()
    else:
        end = datetime.datetime.strptime(end_date, "%Y%m%d")
    data = []
    for d in gen_dates(start, ((end - start).days + 1)):  # 29 + 1
        # print(d)   # datetime.datetime  类型
        data.append(d.strftime("%Y%m%d"))
    return data


def get_month_list(start_date, end_date):
    dates = get_date_list(start_date, end_date)
    months = []
    for i in dates:
        if i[:6] not in months:
            months.append(i[:6])
    return months


def date_to_integer(date):
    # 以上海证券交易所正式成立1990年12月19日开始计算
    start = time.strptime("19901219", "%Y%m%d")
    current = time.strptime(date, "%Y%m%d")

    start = datetime.datetime(start[0], start[1], start[2])
    current = datetime.datetime(current[0], current[1], current[2])

    return (current - start).days



"""    
    print(get_date_list(start_date, end_date))    # 两个日期之间的所有日期，包括开始日期， 包括 结束日期
    print(get_month_list(start_date, end_date)) # 两个日期之间的所有月份，包括开始月份， 包括 结束月份
"""
