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


# table_name="'daily'"
def select_table(cursor, table_name):
    return cursor.execute("SELECT table_name FROM information_schema.TABLES WHERE table_name =%s", table_name)


# 查询表中是否存在某个字段
def select_field(cursor, table_name, column_name):
    cursor.execute("select count(*) from information_schema.columns where table_name = %s and column_name = %s",
                   (table_name, column_name))
    ret = cursor.fetchone()
    return ret[0]


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


# 更新某个因子所有股票的数据
def update_factor_column(con, table_name, column_name, data_list):
    cursor = con.cursor()
    # 若数据库中没有该列,新增字段
    if (select_field(cursor, table_name, column_name) == 0):
        try:
            sql = "ALTER TABLE " + table_name + " ADD COLUMN " + column_name + " float"
            cursor.execute(sql)
            con.commit()
            print("新建" + column_name + "字段")
        except:
            con.rollback()

    # print(data_list)
    # 更新记录，若已有数据相当于再更新一次
    # sql = "INSERT INTO " + table_name + "(ts_code,trade_date," + column_name + ") VALUES(%s,%s,1) on duplicate key "+column_name+"=%s"
    # for data in data_list:
    print(len(data_list))
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
            print("Fetch close data of " + code + d + " Error.")
    print(column+" "+table,end=":")
    print(data_list)
    return pd.Series(data_list)


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
