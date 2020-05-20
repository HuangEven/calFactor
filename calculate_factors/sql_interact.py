import calculate_factors.get_date as gd
import pandas as pd
import math


# table_name="daily"
def create_table(db, table_name):
    cursor = db.cursor()
    try:
        sql = "CREATE TABLE " + table_name + "(ts_code VARCHAR(10),trade_date VARCHAR(8),PRIMARY KEY (ts_code,trade_date))"
        cursor.execute(sql)
        db.commit()
    except:
        db.rollback()


# 新建因子类表后更新更新股票-日期数据
def insert_basic_column_val(con, table_name, codes):
    code_list = get_all_codes(con)
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


# 在trade_date字段添加索引
def add_trade_date_index(con,table_name):
    cursor=con.cursor()
    index_name="ix_trade_date_"+table_name
    try:
        sql_index="ALTER TABLE "+ table_name + " ADD INDEX "+index_name+"(trade_date)"
        cursor.execute(sql_index)
        con.commit()
    except:
        print("建立索引失败！")
        con.rollback()


# table_name="'daily'"
def select_table(cursor, table_name):
    return cursor.execute("SELECT table_name FROM information_schema.TABLES WHERE table_name =%s", table_name)


# 查询表中是否存在某个字段
def select_field(cursor, table_name, column_name):
    cursor.execute("select count(*) from information_schema.columns where table_name = %s and column_name = %s",
                   (table_name, column_name))
    ret = cursor.fetchone()
    return ret[0]


# 获取表中所有因子字段以及他们的顺序信息
def get_table_fields(con,table_name):
    cursor=con.cursor()
    cursor.execute("select column_name from information_schema.columns where table_name = %s",
                   (table_name,))
    ret = cursor.fetchall()

    factor_fields=[]
    for ele in ret:
        if ele[0]=='ts_code' or ele[0]=='trade_date':
            continue
        factor_fields.append(ele[0])

    return factor_fields


# 获取所有股票代码
def get_all_codes(con):
    sql = "SELECT ts_code from stock_basic"
    codes = pd.read_sql(sql, con)
    return list(codes.ts_code)


# 获取单只股票因子数据
def get_column_tuple_list(code, date_list, data_list):
    ret = []
    # 将时间序列与因子数据序列对齐
    move_dist = len(date_list) - len(data_list)
    real_date_list = date_list[move_dist:]
    for date, data in zip(real_date_list, data_list):
        # date_int = gd.date_to_integer(date)
        #print(data,end=",")
        data = float("%.5f" % data)
        ret.append((data, code, date))
    #print(code)
    return ret


# 新增因子字段
def add_factor_column(con,table_name,column_name):
    cursor=con.cursor()
    try:
        sql = "ALTER TABLE " + table_name + " ADD COLUMN " + column_name + " float"
        print(sql)
        cursor.execute(sql)
        con.commit()
        print("新建" + column_name + "字段")
    except Exception as e:
        print(e)
        con.rollback()


# 更新某个因子所有股票的数据
def update_factor_column(con, table_name, column_name, data_list):
    cursor = con.cursor()
    # print(data_list)
    # 更新记录，若已有数据相当于再更新一次
    # sql = "INSERT INTO " + table_name + "(ts_code,trade_date," + column_name + ") VALUES(%s,%s,1) on duplicate key "+column_name+"=%s"
    # for data in data_list:
    # print(len(data_list))
    try:
        sql = "update " + table_name + " set " + column_name + "=%s where ts_code=%s and trade_date=%s"
        cursor.executemany(sql, data_list)
        con.commit()
        print(table_name + ":更新因子" + column_name + "数据")
    except Exception as e:
        print(e)
        print(table_name + ":更新因子" + column_name + "数据失败")
        con.rollback()


def get_data_series(con, table, column, code, date_list):
    data_list = []
    cursor = con.cursor()
    for d in date_list:
        try:
            sql = "select " + column + " from " + table + " where ts_code=%s and trade_date=%s"
            cursor.execute(sql, (code, d))
            ret = cursor.fetchone()
            data_list.append(ret[0])
        except:
            print("Fetch close data of " + code + d + column + table + " Error.")
    # print(column+" "+table,end=":")
    # print(data_list)
    return pd.Series(data_list)


# 向因子表中插入一条记录
def insert_factor_data(con,table,data_list):
    cursor=con.cursor()
    data_len=len(data_list[0])
    str_list=['%s']*data_len
    str=','.join(str_list)
    str='(' + str + ')'
    sql="insert into "+ table +" values"+str
    try:
        cursor.executemany(sql,data_list)
        con.commit()
    except Exception as e:
        print(e)
        con.rollback()

def get_str_from_list(str_list):
    str = ''
    for i in range(len(str_list)):
        if i != len(str_list) - 1:
            temp = str_list[i] + ','
        else:
            temp = str_list[i]
        str += temp

    return str

# 根据股票的日线行情得到股票的连续的交易日序列
def get_date_list_by_codes(con,start_date, end_date):
    raw_date_list = gd.get_date_list(start_date, end_date)
    raw_codes = get_all_codes(con)
    init_list = [0] * len(raw_codes)
    codes = pd.Series(init_list, index=raw_codes)
    dates_str=str(raw_date_list)[1:-1]

    for code in raw_codes:
        sql = "select trade_date from daily where trade_date in (" + dates_str + ") and ts_code=%s"
        df = pd.read_sql(sql, con, params=(code,))
        dates=list(df['trade_date'])
        print(dates)
        codes[code] = dates

    return codes


class Factor:
    name = ''
    value = []

    def __init__(self, n, v):
        self.name = n
        self.value = v

    def set_value(self, v):
        self.value = v

    def get_name(self):
        return self.name

    def get_value(self):
        return self.value

    def append_value(self, v):
        self.value += v
