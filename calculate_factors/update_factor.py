import calculate_factors.sql_interact as si
import calculate_factors.profit_factor as profit_factor


# 全局变量，将模块信息保存下来
module_dict={"profit_factor":profit_factor}


# 新增因子类
def add_factor_category(con,category,code_date_series):
    # 创建表
    si.create_table(con, category)

    # 更新表中的数据
    si.insert_basic_column_val(con,category,code_date_series)

    # 更新完表中数据之后在trade_date字段添加索引
    si.add_trade_date_index(con,category)


# 新增因子
def add_factor(con,category,factor,code_date_series):
    # 解析因子
    basic_data_info,factor_dict,factor_filed=resolve_factor(factor)
    print(basic_data_info)

    # 增加字段
    si.add_factor_column(con,category,factor_filed)

    codes=si.get_all_codes(con)
    values=[]
    for code in codes:
        # 根据收集的信息从数据库中去获取基础数据
        for table in basic_data_info:
            dict={}
            for field in basic_data_info[table]:
                dict[field]=si.get_data_series(con, table, field, code, code_date_series[code])
            # 更新基础数据
            basic_data_info[table]=dict

        func = getattr(module_dict[category], factor_dict["name"])  # 通过反射拿到相应的函数对象
        param_list=[]
        # 向参数列表中计算因子要用到的基本数据
        for table in factor_dict["params0"]:
            for field in factor_dict["params0"][table]:
                param_list.append(basic_data_info[table][field])
        # 向参数列表中增加常数参数
        for val in factor_dict["params1"]:
            param_list.append(val)
        ret_data=func(param_list)   # 计算因子
        values += si.get_column_tuple_list(code,code_date_series[code],ret_data)    # 增加该股票下的因子数据

    # 更新数据
    si.update_factor_column(con, category, factor_filed, values)


# 一个因子类一个因子类地解析,并更新数据
def update_factor_in_category(con,category,factors,cur_date,code_date_series):
    # 解析
    basic_data_info,factor_list=resolve_factors(factors)
    print(basic_data_info)

    # 获取该因子类中因子字段及它们的排列情况
    fields=si.get_table_fields(con,category)
    print(fields)

    codes=si.get_all_codes(con)
    data_list=[]    # 存储所有因子数据
    for code in codes:
        # 根据收集的信息从数据库中去获取基础数据
        for table in basic_data_info:
            dict={}
            for field in basic_data_info[table]:
                dict[field]=si.get_data_series(con, table, field, code, code_date_series[code])
            # 更新基础数据
            basic_data_info[table]=dict

        code_list=[code,cur_date]
        row={}
        # 计算因子,通过反射找到方法并传入参数计算
        # 注意之后要进行数据对齐，为了避免不同时间窗口下计算的因子数据结果长度不同，只取最后一天数据
        for factor in factor_list:
            func = getattr(module_dict[category], factor["name"])
            param_list=[]   # 存放参数列表
            # 获取参数
            for table in factor["params0"]:
                for field in factor["params0"][table]:
                    param_list.append(basic_data_info[table][field])
            for val in factor["params1"]:
                param_list.append(val)

            ret_data=func(param_list)   # 调用函数计算因子
            if len(ret_data)>0:
                data=ret_data[len(ret_data)-1]  # 只取最后一天（当日的数据）
                row[factor["field"]]=float("%.5f" % data)

        if row:     # 字典对象不为空
            # 重新排序使得结果与表中字段顺序一致
            for field in fields:
                code_list.append(row[field])
            print(code_list)
            data_list.append(code_list)     # 添加该股票下的因子数据

    si.insert_factor_data(con,category,data_list)   # 批量更新当天的因子数据


# key#daily.close_daily.open#n_m
# 对需要哪些数据的信息做一个集中的收集
def resolve_factors(factors):
    basic_data_info={}
    factor_list=[]
    for str in factors:
        str_list=str.split('#')
        name=str_list[0]
        params0_list=str_list[1].split('_')
        params0_dict={}
        for p_str in params0_list:
            params=p_str.split('.')
            if params[0] not in params0_dict:      # 若字典中没有该项，新增
                params0_dict[params[0]]=[]
            if params[0] not in basic_data_info:    # 收集信息中没有该项，增加
                basic_data_info[params[0]]=[]
            params0_dict[params[0]].append(params[1])   # 不可能重复，所以不需要判断字段是否已存在
            if params[1] not in basic_data_info[params[0]]: # params[0]对应基本表信息，params[1]对应字段信息
                basic_data_info[params[0]].append(params[1])        # 出现基本表中新的字段

        params1_list=[]  # 计算该因子的一些常量参数
        for val in str_list[2].split('_'):
            params1_list.append(int(val))

        factor_field=str_list[0]+"_"+str_list[2]

        str_dict={'name':name,'params0':params0_dict,'params1':params1_list,'field':factor_field}
        factor_list.append(str_dict)

    return basic_data_info,factor_list


# key#daily.close_daily.open#n_m
# 通过因子key解析要计算该因子所需要用到的基础数据以及其他参数
def resolve_factor(factor):
    basic_data_info={}
    str_list=factor.split("#")
    name=str_list[0]    # 计算该因子的函数名称
    params0_list=str_list[1].split("_")
    params0_dict={}  # 计算该因子所需要的基础数据信息
    for p_str in params0_list:
        params=p_str.split(".")
        if params[0] not in params0_dict:      # 若字典中没有该项，新增
            params0_dict[params[0]]=[]
        if params[0] not in basic_data_info:    # 收集信息中没有该项，增加
            basic_data_info[params[0]]=[]
        params0_dict[params[0]].append(params[1])   # 不可能重复，所以不需要判断字段是否已存在
        if params[1] not in basic_data_info[params[0]]: # params[0]对应基本表信息，params[1]对应字段信息
            basic_data_info[params[0]].append(params[1])        # 出现基本表中新的字段

    params1_list=[]  # 计算该因子的一些常量参数
    for val in str_list[2].split('_'):
        params1_list.append(int(val))

    # 将计算该因子的函数名称、计算该因子所需要的基础数据信息、计算该因子的一些常量参数封装
    str_dict={'name':name,'params0':params0_dict,'params1':params1_list}

    # mysql的字段不能进行复杂的标点符号处理
    # 将因子名转化为mysql可接受的字段
    factor_field=str_list[0]+"_"+str_list[2]

    return basic_data_info,str_dict,factor_field


