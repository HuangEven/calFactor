import calculate_factors.value_factors as value_factors
import calculate_factors.analyst_factors as analyst_factors
import calculate_factors.develop_factors as develop_factors
import calculate_factors.fluctuate_factors as fluctuate_factors
import calculate_factors.motive_factors as motive_factors
import calculate_factors.profit_factors as profit_factors
import calculate_factors.emotion_factors as emotion_factors
import calculate_factors.sql_interact as si
import calculate_factors.get_date as gd

import pandas as pd
import numpy as np
import pymysql
import datetime


def init(codes):
    db = pymysql.connect(host="localhost", user="root", passwd="mysql", db="stockvision")

    value_table = "value_factor"
    profit_table = "profit_factor"
    motive_table = "motive_factor"
    fluctuate_table = "fluctuate_factor"
    emotion_table = "emotion_factor"
    develop_table = "develop_factor"
    analyst_table = "analyst_factor"

    #init_table(db, value_table, codes)
    # init_table(db, profit_table, codes)
    init_table(db, motive_table, codes)
    #init_table(db, fluctuate_table, codes)
    #init_table(db, emotion_table, codes)
    #init_table(db, develop_table, codes)
    #init_table(db, analyst_table, codes)


def init_table(con, table_name, codes):
    cursor = con.cursor()
    # 如果数据库中没有该表，创建
    if (si.select_table(cursor, table_name) == 0):
        print("创建" + table_name)
        si.create_table(con, table_name)

    insert_basic_column_val(con, table_name, codes)


def insert_basic_column_val(con, table_name, codes):
    code_list = si.get_all_codes(con)
    data_list = []
    cursor = con.cursor()
    for code in code_list:
        date_list = codes[code]
        for date in date_list:
            # date_int = gd.date_to_integer(date)
            data_list.append((code, date))
    # 考虑到executemany性能远优于execute，当有不满足主码唯一性时忽略
    try:
        sql_insert = "INSERT IGNORE INTO " + table_name + "(ts_code,trade_date) VALUES(%s,%s)"
        cursor.executemany(sql_insert, data_list)
        con.commit()
        print("更新" + table_name)
    except:
        print(table_name+"更新失败")
        con.rollback()


def get_date_list_by_codes(start_date, end_date):
    con = pymysql.connect(host="localhost", user="root", passwd="mysql", db="stockvision")
    raw_date_list = gd.get_date_list(start_date, end_date)
    raw_codes = si.get_all_codes(con)
    init_list = [0] * len(raw_codes)
    codes = pd.Series(init_list, index=raw_codes)
    count = 0
    for code in raw_codes:
        date_list = []
        count += 1
        for date in raw_date_list:
            df = pd.read_sql("select ts_code from daily where trade_date=%s", con, params=(date,))
            if code in df['ts_code'].values:
                date_list.append(date)
        codes[code] = date_list
        print(date_list)
        print(count)

    return codes


"""
# 获取单只股票因子计算值，其中每日指标中的因子值直接从数据库表中获取
def get_factor_by_code(all_factor_list, tsCode, start, end):
    factors = pd.DataFrame()
    # 每日指标因子
    if all_factor_list[0]:
        # factors[all_factor_list[0]] = get_daily_basic_factor(all_factor_list[0], tsCode, start, end)

    # 价值因子
    if all_factor_list[1]:
        factors[all_factor_list[1]] = value_factors.get_value_factors(all_factor_list[1], tsCode, start, end)

    # 利润因子
    if all_factor_list[2]:
        factors[all_factor_list[2]] = profit_factors.get_profit_factors(all_factor_list[2], tsCode, start, end)

    # 成长因子
    if all_factor_list[3]:
        factors[all_factor_list[3]] = develop_factors.get_develop_factors(all_factor_list[3], tsCode, start, end)

    # 波动因子
    if all_factor_list[4]:
        factors[all_factor_list[4]] = fluctuate_factors.get_fluctuate_factors(all_factor_list[4], tsCode, start, end)

    # 情感因子
    if all_factor_list[5]:
        factors[all_factor_list[5]] = emotion_factors.get_emotion_factors(all_factor_list[5], tsCode, start, end)

    # 动量因子
    if all_factor_list[6]:
        factors[all_factor_list[6]] = motive_factors.get_motive_factors(all_factor_list[6], tsCode, start, end)

    # 分析师预测因子
    if all_factor_list[7]:
        factors[all_factor_list[7]] = analyst_factors.get_analyst_factors(all_factor_list[7], tsCode, start, end)

    return factors

# 获取股票池中所有股票的因子
def get_factor_in_codes():
    code_list = []
    codes = get_codes()
    for code in codes:
        code_list.append(get_factor_by_code(code))

    return code_list

# 股票池代码集
def get_codes():
    url = "http://localhost:8001/java-service/python/category"
    rsp = requests.get(url)
    codes = json.loads(rsp.text)

    return codes
"""

if __name__ == "__main__":
    start_date = '20171201'
    end_date = '20180101'
    #codes = get_date_list_by_codes(start_date, end_date)
    #np.save('data.npy', codes)

    con = pymysql.connect(host="localhost", user="root", passwd="mysql", db="stockvision")
    pool = si.get_all_codes(con)
    print(pool)
    #codes = np.load('data.npy')
    #codes = pd.Series(codes, index=pool)

    #init(codes)
    # profit_factors.update_profit_factor(codes)
    # emotion_factors.update_emotion_factor(codes)
    # fluctuate_factors.update_fluctuate_factor(codes)
    # motive_factors.update_motive_factors(codes)
