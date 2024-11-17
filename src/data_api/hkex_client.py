import datetime as dtm

from common import sql
from common.models.data_series import DataSeries
from common.models.market_data import SessionType

from data_api.db_config import META_DB, PRICES_DB
from data_api.hkex_config import *
from data_api import hkex_server
from instruments.stock import Stock
from instruments.equity_index import EquityIndex, EquityIndexFuture

def get_components(id: str):
    select_query = f"SELECT component_id FROM {INDEX_COMPOSITION_TABLE} WHERE index_id='{id}'"
    select_res = sql.fetch(select_query, META_DB)
    index_components = [row[0] for row in select_res]
    return index_components

def get_stocks(load_data: bool = False) -> list[Stock]:
    select_query = f"SELECT ric, name FROM {EQUITY_TABLE}"
    select_res = sql.fetch(select_query, META_DB)
    instruments = []
    for row in select_res:
        stock = Stock(data_id=row[0], name=row[1])
        if load_data:
            stock._data_series = get_history(stock.data_id)
        instruments.append(stock)
    return instruments

def get_indices(load_data: bool = False) -> list[EquityIndex]:
    index_query = f"SELECT ric, name FROM {INDEX_TABLE}"
    index_res = sql.fetch(index_query, META_DB)
    instruments = []
    for row in index_res:
        index = EquityIndex(data_id=row[0], name=row[1])
        if load_data:
            index._data_series = get_history(index.data_id)
        instruments.append(index)
    return instruments

def get_index_derivatives(load_data: bool = False) -> list[EquityIndex]:
    select_query = f"SELECT t1.ric, t1.name, t2.future_id FROM {INDEX_TABLE} AS t1 "\
    f"LEFT OUTER JOIN {FUTURE_TABLE} AS t2 ON t1.index_id=t2.underlier_id ORDER BY t2.lot_size DESC"
    select_res = sql.fetch(select_query, META_DB)
    instruments = []
    for row in select_res:
        index = EquityIndex(data_id=row[0], derivatives_id=row[2], name=row[1])
        if load_data:
            index._data_series = get_history(index.data_id)
        instruments.append(index)
    return instruments

def get_stock_derivatives(load_data: bool = False) -> list[Stock]:
    select_query = f"SELECT t1.ric, t1.name, t2.future_id FROM {EQUITY_TABLE} AS t1 "\
    f"LEFT OUTER JOIN {FUTURE_TABLE} AS t2 ON t1.stock_id=t2.underlier_id ORDER BY t2.lot_size DESC"
    select_res = sql.fetch(select_query, META_DB)
    instruments = []
    for row in select_res:
        stock = Stock(data_id=row[0], derivatives_id=row[2], name=row[1])
        if load_data:
            stock._data_series = get_history(stock.data_id)
        instruments.append(stock)
    return instruments

def get_underlier(id: str):
    underlier_query = f"SELECT underlier_id FROM {FUTURE_TABLE} WHERE future_id='{id}'"
    underlier_id, = sql.fetch(underlier_query, META_DB, count=1)
    index_query = f"SELECT ric, name FROM {INDEX_TABLE} WHERE index_id='{underlier_id}'"
    index_res = sql.fetch(index_query, META_DB, count=1)
    return EquityIndex(data_id=index_res[0], derivatives_id=underlier_id, name=index_res[1])

def get_futures_contracts(series: str, session_type: SessionType, load_data: bool = False) -> list[EquityIndexFuture]:
    underlier = get_underlier(series)
    contracts_query = "SELECT contract_id, contract_month, last_trade_date, extended_session_id "\
    f"FROM {FUT_CONTRACT_TABLE} WHERE series_id='{series}'"
    select_res = sql.fetch(contracts_query, META_DB)
    instruments = []
    for row in select_res:
        expiry = dtm.datetime.strptime(row[2], sql.DATE_FORMAT).date()
        match hkex_server.get_session_default(session_type):
            case SessionType.REGULAR:
                data_id = row[0]
            case SessionType.EXTENDED:
                data_id = row[3]
        future = EquityIndexFuture(underlier, expiry, data_id=data_id, name=f'{series} {row[1]}')
        if load_data:
            future._data_series = get_history(row[0])
        instruments.append(future)
    return instruments

def get_history(ric: str):
    select_query = f"SELECT date, close FROM {HISTORY_TABLE} WHERE instrument_id='{ric}' ORDER BY date"
    select_res = sql.fetch(select_query, PRICES_DB)
    date_series = [(dtm.datetime.strptime(row[0], sql.DATE_FORMAT).date(), row[1]) for row in select_res]
    return DataSeries(date_series)

def get_last_date(ric: str):
    select_query = f"SELECT date FROM {HISTORY_TABLE} WHERE instrument_id='{ric}' ORDER BY date DESC"
    last_date, = sql.fetch(select_query, PRICES_DB, count=1)
    return dtm.datetime.strptime(last_date, sql.DATE_FORMAT).date()

def get_intraday_data(ric: str, **kwargs) -> dict[dtm.datetime, float]:
    chart_data = hkex_server.get_chart_data(ric, **kwargs)
    return {row[0]: row[4] for row in chart_data}

if __name__ == '__main__':
    hkex_server.set_token()
    # stocks = set()
    # for c in SERIES_CODES:
    #     stocks.update(get_components(c))
    # for s in stocks:
    #     hkex_server.load_stock_details(s)
    for s in get_stocks() + get_indices():
        from_date = get_last_date(s.data_id) + dtm.timedelta(days=1)
        hkex_server.update_history_daily(s.data_id, '1m', from_date)
    pass
