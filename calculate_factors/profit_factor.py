import calculate_factors.feature as feature


# n日对数收益率
def log_return(params):
    prices=params[0]
    n=params[1]
    return feature.get_logreturn(prices, period=n, max_period=n)


# n日绝对收益率
def absolute_return(params):
    prices=params[0]
    n=params[1]
    return feature.get_return(prices, period=n, max_period=n)

