import calculate_factors.sql_interact as si
import calculate_factors.profit_factor as profit_factor


# 新增因子类
def add_factor_category(con,category,code_date_series):
    # 创建表
    si.create_table(con, category)

    # 更新表中的数据
    si.insert_basic_column_val(con,category,code_date_series)

    # 更新完表中数据之后在trade_date字段添加索引
    si.add_trade_date_index(con,category)


# 新增因子
def add_factor(con,category,factor,codes,date_list,code_date_series):

    # 解析因子
    basic_data_info,factor_list=resolve_factor(factor)

    values=[]
    for code in codes:
        # 根据收集的信息从数据库中去获取基础数据
        for table in basic_data_info:
            dict={}
            for field in basic_data_info[table]:
                dict[field]=si.get_data_series(con, table, field, code, code_date_series[code])
            # 更新基础数据
            basic_data_info[table]=dict

        func = getattr(category, factor["name"])
        param_list=[]
        for table in factor["params0"]:
            for field in factor["params0"][table]:
                param_list.append(basic_data_info[table][field])

        ret_data=func(param_list)
        values.append(si.get_column_tuple_list(code,date_list,ret_data))

    si.update_factor_column(con, category, factor, values)


# 一个因子类一个因子类地解析,并更新数据
def update_factor_in_category(con,category,factors,codes,cur_date,code_date_series):

    module_dict={"profit_factor":profit_factor}
    # 解析
    basic_data_info,factor_list=resolve_factors(factors)

    data_list=[]    # 存储新增数据集
    for code in codes:
        # 根据收集的信息从数据库中去获取基础数据
        for table in basic_data_info:
            dict={}
            for field in basic_data_info[table]:
                dict[field]=si.get_data_series(con, table, field, code, code_date_series[code])
            # 更新基础数据
            basic_data_info[table]=dict

        code_list=[code,cur_date]
        # 计算因子,通过反射找到方法并传入参数计算
        # 注意之后要进行数据对齐
        # 为了避免不同时间窗口下计算的因子数据结果长度不同，只取最后一天数据
        for factor in factor_list:
            func = getattr(module_dict[category], factor["name"])
            param_list=[]
            for table in factor["params0"]:
                for field in factor["params0"][table]:
                    param_list.append(basic_data_info[table][field])

            ret_data=func(param_list)
            code_list.append(ret_data[len(ret_data)-1])     # 只取最后一天（当日的数据）

        data_list.append(code_list)

    si.insert_factor_data(con,category,data_list)


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
                basic_data_info[params[0]]=params[1]        # 出现基本表中新的字段
        params1_list=int(str_list[2].split('_'))

        str_dict={'name':name,'params0':params0_dict,'params1':params1_list}
        factor_list.append(str_dict)

    return basic_data_info,factor_list

# 通过因子key解析要计算该因子所需要用到的基础数据以及其他参数
def resolve_factor(factor):
    basic_data_info={}
    str_list=str.split('#')
    name=str_list[0]
    params0_list=str_list[1].split('_')
    params0_dict={}
    for p_str in params0_list:
        params=p_str.split('.')
        if params[0] not in params0_dict:      # 若字典中没有该项，新增
            params0_dict[params[0]]=[]
        params0_dict[params[0]].append(params[1])   # 不可能重复，所以不需要判断字段是否已存在
        if params[1] not in basic_data_info[params[0]]: # params[0]对应基本表信息，params[1]对应字段信息
            basic_data_info[params[0]]=params[1]        # 出现基本表中新的字段
    params1_list=int(str_list[2].split('_'))
    str_dict={'name':name,'params0':params0_dict,'params1':params1_list}

    return basic_data_info,str_dict
