import datetime as dtm
import pandas as pd
import statsmodels.api as sm_api
import statsmodels.tsa.api as ts_api
import numpy as np
import logging

from data_api import hkab_server
from common.chrono import tenor as tenor_lib
from common.chrono import daycount
from common.numeric.interpolator import Linear

logger = logging.Logger(__name__)

DAYCOUNT = daycount.DayCount.ACT365
RATES_CURVE = {}
MIN_POINTS = 5

def load_rates_curve(value_date: dtm.date):
    tenors_rates = hkab_server.get_rates(value_date)
    rates_curve = []
    for tenor, rate in tenors_rates.items():
        tenor_date = tenor_lib.Tenor(tenor).get_date(value_date)
        tenor_dcf = DAYCOUNT.get_dcf(value_date, tenor_date)
        rates_curve.append((tenor_dcf, rate / 100))
    return Linear(rates_curve)

def get_hedge_ratio(value_date: dtm.date, expiry_date: dtm.date) -> float:
    if value_date not in RATES_CURVE:
        RATES_CURVE[value_date] = load_rates_curve(value_date)
    contract_dcf = DAYCOUNT.get_dcf(value_date, expiry_date)
    return 1 + RATES_CURVE[value_date].get_value(contract_dcf) * contract_dcf

def get_beta_matrix(stock_prices: dict[str, pd.Series], index_prices: dict[str, pd.Series]) -> dict[str, dict[str, float]]:
    stock_returns = {k: stk_p.dropna().pct_change() for k, stk_p in stock_prices.items()}
    index_returns = {k: idx_p.dropna().pct_change() for k, idx_p in index_prices.items()}
    betas = {}
    for idx_n, idx_r in index_returns.items():
        betas[idx_n] = {}
        for sn, stk_r in stock_returns.items():
            r_df = pd.concat([idx_r, stk_r], axis=1)
            r_v = list(zip(*r_df.dropna().values))
            if len(r_v) != 2 or len(r_v[0]) < MIN_POINTS:
                logger.error(f'{sn} data points are not valid')
                continue
            x_in = sm_api.add_constant(r_v[0], prepend=False)
            res = sm_api.OLS(r_v[1], x_in).fit()
            logger.info(f"{idx_n}, {sn}, {res.params}")
            betas[idx_n][sn] = res.params
    return betas

def get_autocorrelation(stock_prices: dict[str, pd.Series]) -> dict[str, tuple[int, float]]:
    stock_returns = {k: stk_p.dropna().pct_change() for k, stk_p in stock_prices.items()}
    res = {}
    for sn, stk_r in stock_returns.items():
        stk_r_v = stk_r.dropna().values
        if len(stk_r_v) < MIN_POINTS:
            logger.info(f'{sn} data points are not valid')
            continue
        pac_v = ts_api.pacf(stk_r_v)
        pac_i = np.argmax([abs(x) for x in pac_v[1:]]) + 1
        res[sn] = (pac_i, pac_v[pac_i])
    return res
