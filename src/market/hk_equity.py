import pandas as pd
import datetime as dtm
import logging

from common.models.market_data import MarketDataType, SessionType
from data_api import hkex_client
from lib import analytics
from instruments.stock import Stock, BaseInstrument
from instruments.equity_index import EquityIndex

logger = logging.Logger(__name__)

BASE_PRICE = 100

def get_data_slice(instrument: BaseInstrument, from_date: dtm.date, to_date: dtm.date = None):
    return pd.Series({k: instrument.data[k] for k in instrument.data.irange(from_date, to_date)})

def get_return(instrument: BaseInstrument, from_date: dtm.date, to_date: dtm.date = None):
    try:
        from_price = instrument.data.get_latest_value(from_date)
    except IndexError:
        # returns calculation does not support partial period
        logger.info(f'{instrument.name} prices not available from {from_date}')
        return None
    if to_date:
        to_price = instrument.data.get_latest_value(to_date)
    else:
        to_date, to_price = instrument.data.get_last_point()
    return (to_price / from_price - 1)

def get_returns(instruments: list[BaseInstrument], from_date: dtm.date, to_date: dtm.date = None):
    return {inst.name: get_return(inst, from_date, to_date) for inst in instruments}

def get_stocks_beta(stocks: list[Stock], benchmarks: list[EquityIndex],
                    from_date: dtm.date, to_date: dtm.date = None):
    index_data = {}
    stocks_data = {}
    for bi in benchmarks:
        index_data[bi.name] = get_data_slice(bi, from_date, to_date)
    for si in stocks:
        stocks_data[si.name] = get_data_slice(si, from_date, to_date)
    betas = analytics.get_beta_matrix(pd.DataFrame(stocks_data), pd.DataFrame(index_data))
    return betas


def get_lag_correlations(stocks: list[Stock], from_date: dtm.date, to_date: dtm.date = None):
    stocks_data = {}
    for si in stocks:
        stocks_data[si.name] = get_data_slice(si, from_date, to_date)
    correls = analytics.get_autocorrelation(stocks_data)
    return correls


def get_stock_intraday_data(stocks: list[Stock], benchmarks: list[EquityIndex],
                            beta_matrix: dict[str, dict[str, dict[str, float]]]):
    stocks_data = {}
    for stk_i in stocks:
        price_series = pd.Series(hkex_client.get_intraday_data(stk_i.data_id))
        price_factor = BASE_PRICE / stk_i[MarketDataType.PREV_CLOSE]
        prices_norm = {k: v * price_factor for k, v in price_series.items()}
        stocks_data[stk_i.name] = {'Market': pd.Series(prices_norm)}
    for idx_i in benchmarks:
        price_series = pd.Series(hkex_client.get_intraday_data(idx_i.data_id))
        index_close = idx_i[MarketDataType.PREV_CLOSE]
        for stk_n, (b0, b1) in beta_matrix[idx_i.name].items():
            prices_norm = {k: (1 + (v/index_close-1) * b0 + b1) * BASE_PRICE for k, v in price_series.items()}
            stocks_data[stk_n][idx_i.name] = pd.Series(prices_norm)
    return stocks_data


def get_index_futures_spread(idx: EquityIndex):
    futures_data = {}
    spots_data = {}
    spreads_data = {}
    index_ticks = pd.Series(hkex_client.get_intraday_data(idx.data_id))
    if index_ticks.empty:
        logger.error('No underlying spot ticks to calculate spread')
    trade_date = index_ticks.index[-1].date()
    for session_type in SessionType:
        futures_list = hkex_client.get_futures_contracts(idx.derivatives_id, session_type=session_type)
        num_futures_loaded = 0
        for future in futures_list:
            if future.expiry < trade_date or num_futures_loaded >= 2:
                continue
            fut_key = future.name
            future_ticks = pd.Series(hkex_client.get_intraday_data(future.data_id))
            if future_ticks.empty:
                continue
            num_futures_loaded += 1
            if fut_key in futures_data:
                futures_data[fut_key] = pd.concat([futures_data[fut_key], future_ticks])
                continue
            else:
                futures_data[fut_key] = future_ticks
            hedge_ratio = analytics.get_hedge_ratio(trade_date, future.expiry)
            prices_spot = {k: v / hedge_ratio for k, v in future_ticks.items()}
            spots_data[f'{fut_key} Spot'] = pd.Series(prices_spot)
            spread_ticks = future_ticks.combine(index_ticks, lambda x, y : x/y-1).dropna()
            spreads_data[fut_key] = spread_ticks
    spots_data[idx.name] = index_ticks
    return futures_data | spots_data, spreads_data
