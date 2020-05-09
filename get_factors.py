import calculate_factors.value_factors as value_factors
import calculate_factors.analyst_factors as analyst_factors
import calculate_factors.develop_factors as develop_factors
import calculate_factors.fluctuate_factors as fluctuate_factors
import calculate_factors.motive_factors as motive_factors
import calculate_factors.profit_factors as profit_factors
import calculate_factors.emotion_factors as emotion_factors
import calculate_factors.sql_interact as si
import calculate_factors.get_date as gd
import calculate_factors.update_factor as uf

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

    si.insert_basic_column_val(con, table_name, codes)


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


if __name__ == "__main__":
    start_date = '20171201'
    end_date = '20180101'
    #codes = get_date_list_by_codes(start_date, end_date)
    #np.save('data.npy', codes)

    con = pymysql.connect(host="localhost", user="root", passwd="mysql", db="stockvision")
    pool = si.get_all_codes(con)
    #print(pool)
    codes = np.load('data.npy')
    codes = pd.Series(codes, index=pool)
    uf.add_factor(con,"profit_factor","log_return#daily.close#5",codes)
    #uf.add_factor_category(con,"profit_factor",codes)

    #init(codes)
    # profit_factors.update_profit_factor(codes)
    # emotion_factors.update_emotion_factor(codes)
    # fluctuate_factors.update_fluctuate_factor(codes)
    # motive_factors.update_motive_factors(codes)

    """
    cursor=con.cursor()
    for factor in range(100):
        row=[['000001.SH','20180101',factor+1,0.1]]*1000000
        print(len(row))
        sql_insert="INSERT IGNORE INTO factor (ts_code,trade_date,factor_id,val) VALUES(%s,%s,%s,%s)"
        cursor.executemany(sql_insert, row)
        con.commit()
        print(factor)
    """



