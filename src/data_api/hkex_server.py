import datetime as dtm
from bs4 import BeautifulSoup
import regex
import urllib
import logging

from common import request_web, sql
from common.models.market_data import InstrumentDataField, MarketDataType, InstrumentDataModel, SessionType, OptionDataFlag
from data_api.db_config import META_DB, PRICES_DB
from data_api.hkex_config import *

logger = logging.Logger(__name__)

COMPONENTS_URL = "https://www.hsi.com.hk/data/eng/rt/index-series/{code}/constituents.do"
def update_components(series: str):
    from_date = dtm.date(2024, 6, 11)
    series_json = request_web.get_json(request_web.url_get(COMPONENTS_URL.format(code=series.lower())))
    series_info = series_json['indexSeriesList']
    assert len(series_info) == 1, 'Invalid size for index series list'
    logger.info(series_info[0]['seriesName'])
    insert_rows = []
    for index in series_info[0]['indexList']:
        # index_name = index['indexName'].strip()
        for constituent in index['constituentContent']:
            stock_code = constituent['code'].rjust(4, '0')
            # constituentName = constituent['constituentName'].strip()
            # all_stocks[constituentCode] = {'name': constituentName}
            insert_rows.append(f"\n('{series}', '{stock_code}', '{from_date.strftime(sql.DATE_FORMAT)}')")
    if insert_rows:
        insert_query = f"INSERT INTO {INDEX_COMPOSITION_TABLE} VALUES {','.join(insert_rows)};"
        return sql.modify(insert_query, META_DB)
    return False

CALENDAR_URL = "https://www.hkex.com.hk/Services/Trading/Derivatives/Overview/Trading-Calendar-and-Holiday-Schedule?sc_lang=en"
# CALENDAR_COLS = ['Contract', 'Expiry', 'Settle']
def cell_to_date(cell: str):
    return dtm.datetime.strptime(cell, '%d-%b-%y').date()
def get_expiry_dates() -> dict[str, tuple[dtm.date, dtm.date]]:
    calendar_text = request_web.url_get(CALENDAR_URL)
    calendar_soup = BeautifulSoup(calendar_text, 'html.parser')
    for s_table in calendar_soup.find_all('table'):
        s_thead = s_table.find_all('thead')
        s_tbody = s_table.find_all('tbody')
        if s_thead and s_tbody:
            # colnames = [td.text for td in s_thead[0].find('tr').find_all('th')]
            table = [[td.text for td in s_tr.find_all('td')] for s_tr in s_tbody[0].find_all('tr')]
            cal_map = {}
            for row in table:
                cal_map[row[0]] = (cell_to_date(row[1]), cell_to_date(row[2]))
            return cal_map
    raise RuntimeError('No calendar expiry data found')


def str_to_num(input: str, num_type = float) -> float:
    if input:
        return num_type(input.replace(',', ''))
    else:
        return 0

def get_field(data_dict: dict[str, any], datapoint_type: MarketDataType):
    match datapoint_type:
        case InstrumentDataField.NAME:
            return data_dict['nm']
        case InstrumentDataField.RIC:
            return data_dict['ric']
        case InstrumentDataField.CONTRACT:
            return data_dict['con']
        case InstrumentDataField.CCY:
            return data_dict['ccy']
        case MarketDataType.LAST:
            return str_to_num(data_dict['ls']) if data_dict['ls'] else None
        case MarketDataType.BID:
            return str_to_num(data_dict['bd']) if data_dict['bd'] else None
        case MarketDataType.ASK:
            return str_to_num(data_dict['as']) if data_dict['as'] else None
        case MarketDataType.PREV_CLOSE:
            return str_to_num(data_dict['hc'])
        case MarketDataType.SETTLE:
            return str_to_num(data_dict['se']) if data_dict['se'] else None
        case MarketDataType.OPEN:
            return str_to_num(data_dict['op']) if data_dict['op'] else None
        case MarketDataType.VOLUME:
            return str_to_num(data_dict['vo'], int) if data_dict['vo'] else None
        case MarketDataType.PREV_OI:
            return str_to_num(data_dict['oi'], int) if data_dict['oi'] else None
        case InstrumentDataField.LOT_SIZE:
            return str_to_num(data_dict['lot'], int)
        case InstrumentDataField.TICK_SIZE:
            return float(data_dict['tck'])
        case MarketDataType.UPDATE_TIME:
            return data_dict['updatetime']
        case _:
            logger.error(f'Unhandled {datapoint_type}')

def get_fields(data_dict: dict[str, any], datapoint_types: list[MarketDataType]):
    res = InstrumentDataModel()
    for dtp in datapoint_types:
        res[dtp] = get_field(data_dict, dtp)
    return res


SESSION_TOKEN = None
TOKEN_HOME_URL = "https://www.hkex.com.hk/Market-Data/Securities-Prices/Equities/Equities-Quote?sc_lang=en"
def set_token():
    token_home_text = request_web.url_get(TOKEN_HOME_URL)
    token_home_soup = BeautifulSoup(token_home_text, 'html.parser')
    token_func = token_home_soup.find(string=regex.compile('getToken'))
    token_return = regex.search("return \"Base64-AES-Encrypted-Token\";[\r\n]+\s*return \"([^\";\r\n]+)", token_func)
    global SESSION_TOKEN
    SESSION_TOKEN = token_return.group(1)
    logger.info(SESSION_TOKEN)

def is_valid_token():
    return SESSION_TOKEN is not None


DATA_URL = "https://www1.hkex.com.hk/hkexwidget/data/"
def request_get_json_data(endpoint: str, params: dict[str, any] = {}):
    if not is_valid_token():
        set_token()
    params.update({
        'token': SESSION_TOKEN,
        'lang': 'eng',
        'qid': 0,
        'callback': 'jQuery0_0',
    })
    params_str = urllib.parse.urlencode(params, safe='%') if params else None
    response_text = request_web.url_get(DATA_URL + endpoint, params=params_str)
    text_json = regex.search("jQuery0_0\((.*)\)", response_text).group(1)
    return request_web.get_json(text_json)['data']

STOCK_EP = "getequityquote"
#?sym={code}&token={token}&lang=eng&qid=0&callback=jQuery0_0"
def update_stock_details(code: str):
    stock_data = request_get_json_data(STOCK_EP, params={'sym': str_to_num(code, int)})['quote']
    # get_fields(stock_data, [
    #     InstrumentDataField.RIC, InstrumentDataField.CCY, 
    #     InstrumentDataField.LOT_SIZE, InstrumentDataField.TICK_SIZE,
    #     MarketDataType.LAST, MarketDataType.PREV_CLOSE, MarketDataType.OPEN, MarketDataType.UPDATE_TIME]) | {
    #     'name': stock_data['nm_s'],
    #     'issued_shares': str_to_num(stock_data['amt_os'], int),
    #     'close_date': dtm.datetime.strptime(stock_data['hist_closedate'], "%d %b %Y").date(),
    #     'index_classification': stock_data['hsic_ind_classification'],
    # }
    insert_query = f"INSERT OR IGNORE INTO {EQUITY_TABLE} VALUES ("\
        f"'{code}', '{get_field(stock_data, InstrumentDataField.RIC)}', '{get_field(stock_data, InstrumentDataField.CCY)}', "\
        f"\"{stock_data['nm_s']}\", \"{stock_data['hsic_ind_classification']}\", "\
        f"{get_field(stock_data, InstrumentDataField.LOT_SIZE)}, {get_field(stock_data, InstrumentDataField.TICK_SIZE)}, "\
        f"{str_to_num(stock_data['amt_os'], int)}, {str_to_num(stock_data['div_yield'])})"
    return sql.modify(insert_query, META_DB)

STOCKS_UNI_EP = "getequityfilter"
def update_stocks():
    url_params = {
        'subcat': 1, 'market': 'MAIN',
        'sort': 4, 'order': 0
    }
    stocks_list = request_get_json_data(STOCK_EP, params=url_params)['stocklist']
    for row in stocks_list:
        print(row)
    return False

INDEX_EP = "getderivativesindex"
def update_index_details(code: str):
    index_data = request_get_json_data(INDEX_EP, params={'ats': code})['info']
    insert_query = f"INSERT OR IGNORE INTO {INDEX_TABLE} VALUES ("\
        f"'{code}', '{get_field(index_data, InstrumentDataField.RIC)}', 'HKD', \"{index_data['nm']}\")"
    return sql.modify(insert_query, META_DB)

STOCK_DERIVS_EP = "getstockderivativeslist"
STOCK_DERIVS_URL = 'https://www.hkex.com.hk/Products/Listed-Derivatives/Single-Stock/Stock-Futures?sc_lang=en'
def update_stock_derivatives():
    stock_list = request_get_json_data(STOCK_DERIVS_EP)['stocklist']
    lot_sizes = {
    }
    insert_rows = []
    for row in stock_list:
        fut_volume, opt_volume = str_to_num(row['fut'], int), str_to_num(row['opt'], int)
        if not (fut_volume and opt_volume and (fut_volume > 100 or opt_volume > 10000)):
            continue
        symbol = row['sym'].rjust(4, '0')
        contract_id = row['cd']
        insert_rows.append(f"\n('{contract_id}', '{symbol}', {lot_sizes[contract_id]})")
    if insert_rows:
        insert_query = f"INSERT OR IGNORE INTO {FUTURE_TABLE} VALUES {','.join(insert_rows)};"
        return sql.modify(insert_query, META_DB)
    return False


REGULAR_OPEN_TIME = dtm.time(9, 30)
REGULAR_CLOSE_TIME = dtm.time(16, 30)
EXTENDED_OPEN_TIME = dtm.time(17, 30)
# EXTENDED_CLOSE_TIME = dtm.time(3, 0)
def get_session_default(session_type: SessionType = None) -> SessionType:
    if session_type in list(SessionType):
        return session_type
    current_dtm = dtm.datetime.now()
    current_date, current_time = current_dtm.date(), current_dtm.time()
    if current_date.weekday() > 4:
        return SessionType.REGULAR
    elif REGULAR_OPEN_TIME <= current_time <= REGULAR_CLOSE_TIME:
        return SessionType.REGULAR
    elif current_time >= EXTENDED_OPEN_TIME or current_time <= REGULAR_OPEN_TIME:
        return SessionType.EXTENDED
    return SessionType.REGULAR

# DERIVS_EP = "getderivativesinfo"
# def getDerivativesInfo(code) -> dict[str, any]:
#     deriv_data = request_get_json_data(DERIVS_EP, params={'ats': code})['info']
#     return {
#         'type': deriv_data['sc'] or deriv_data['idx'],
#         'futures': deriv_data['fut']['d'] or deriv_data['fut']['n'],
#         'options': deriv_data['opt']
#     }

FUTURES_EP = "getderivativesfutures"
def request_futures_details(series: str, session_type: SessionType):
    url_params = {
        'ats': series,
        'type': get_session_default(session_type),
    }
    return request_get_json_data(FUTURES_EP, params=url_params)

def load_futures_quotes(code: str, session_type: SessionType = None) -> tuple[dtm.datetime, dict[str, float]]:
    futs_data = request_futures_details(code, session_type)
    last_update = dtm.datetime.strptime(futs_data['lastupd'], "%d/%m/%Y %H:%M")
    res = {}
    fields = [InstrumentDataField.CONTRACT, InstrumentDataField.RIC]
    points = [MarketDataType.LAST, MarketDataType.ASK, MarketDataType.BID,
              MarketDataType.SETTLE, MarketDataType.PREV_CLOSE,
              MarketDataType.VOLUME, MarketDataType.PREV_OI]
    for fut_data in futs_data['futureslist']:
        data_fields = get_fields(fut_data, fields)
        data_points = get_fields(fut_data, points)
        if data_points[MarketDataType.PREV_OI] and data_points[MarketDataType.VOLUME]:
            res[data_fields[InstrumentDataField.RIC]] = data_points
        else:
            logger.info(f"Skipping inactive {data_fields[InstrumentDataField.CONTRACT]} contract for {code}")
    return last_update, res

def update_futures_details(series: str, has_extended: bool = True) -> tuple[dtm.datetime, dict[str, any]]:
    insert_rows = []
    fields = [InstrumentDataField.CONTRACT, InstrumentDataField.RIC]
    expiry_map = get_expiry_dates()
    futs_data = request_futures_details(series, SessionType.REGULAR)
    if has_extended:
        extended_ids = {}
        for fut_data in request_futures_details(series, SessionType.EXTENDED)['futureslist']:
            fields_data = get_fields(fut_data, fields)
            extended_ids[fields_data[InstrumentDataField.CONTRACT]] = fields_data[InstrumentDataField.RIC]
    for fut_data in futs_data['futureslist']:
        fields_data = get_fields(fut_data, fields)
        contract_month = fields_data[InstrumentDataField.CONTRACT]
        if contract_month not in expiry_map:
            continue
        (expiry_date, settle_date) = expiry_map[contract_month]
        if has_extended:
            extended_id = f'\'{extended_ids[contract_month]}\''
        else:
            extended_id = 'NULL'
        insert_rows.append("\n("\
    f"'{fields_data[InstrumentDataField.RIC]}', '{series}', '{contract_month}', "\
    f"'{expiry_date.strftime(sql.DATE_FORMAT)}', '{settle_date.strftime(sql.DATE_FORMAT)}', {extended_id})")
    if insert_rows:
        insert_query = f"INSERT OR IGNORE INTO {FUT_CONTRACT_TABLE} VALUES {','.join(insert_rows)};"
        return sql.modify(insert_query, META_DB)
    return False

def is_valid_live(data_fields: dict[str, any]) -> bool:
    if (data_fields[MarketDataType.VOLUME]) or \
        (data_fields[MarketDataType.BID] and data_fields[MarketDataType.ASK]):
        return True
    return False

OPTION_CHAIN_EP = "getderivativesoption"
def get_options_chain(code: str, contract_id: str, session_type: SessionType = None) -> dict[float, dict[str, InstrumentDataModel]]:
    url_params = {
        'ats': code,
        'con': contract_id,
        'type': get_session_default(session_type),
        'fr': 'null',
        'to': 'null',
    }
    options_data = request_get_json_data(OPTION_CHAIN_EP, params=url_params)
    # last_update = dtm.datetime.strptime(options_data['lastupd'], "%d/%m/%Y %H:%M")
    fields = [MarketDataType.LAST, MarketDataType.BID, MarketDataType.ASK, MarketDataType.VOLUME, MarketDataType.PREV_OI]
    res = {}
    for row in options_data['optionlist']:
        strike = str_to_num(row['strike'])
        res[strike] = {}
        put_fields_data = get_fields(row['p'], fields)
        if is_valid_live(put_fields_data):
            res[strike][OptionDataFlag.PUT] = put_fields_data
        call_fields_data = get_fields(row['c'], fields)
        if is_valid_live(call_fields_data):
            res[strike][OptionDataFlag.CALL] = call_fields_data
    return res


SPAN_MAP = {
    '1min': '0',
    '5min': '1',
    '15min': '2',
    '1h': '3',
    '1d': '6',
    '1w': '7',
    '1m': '8',
    '1q': '9'
}
INTERVAL_MAP = {
    '1d': '0',
    '5d': '1',
    '1m': '2',
    '3m': '3',
    '6m': '4',
    '1y': '5',
    '2y': '6',
    '5y': '7',
    '10y': '8',
    'ytd': '9',
}
# HIST_COLS = ['Open', 'High', 'Low', 'Close', 'Volume', 'Turnover']
HISTORY_EP = "getchartdata2"
#?span={frequency}&int={lookback}&ric={ric}&token={token}&lang=eng&qid=0&callback=jQuery0_0"
def get_chart_data(ric: str, frequency: str='1min', lookback: str='1d'):
    url_params = {
        'span': SPAN_MAP[frequency],
        'int': INTERVAL_MAP[lookback],
        'ric': ric,
    }
    chart_data = request_get_json_data(HISTORY_EP, params=url_params)['datalist']
    res = [(dtm.datetime.fromtimestamp(row[0]/1000), *row[1:]) for row in chart_data[1:-1]]
    return res

def update_history_daily(ric: str, lookback: str, first_date: dtm.date = None):
    history = get_chart_data(ric, frequency='1d', lookback=lookback)
    insert_rows = []
    for row in history:
        date = row[0].strftime(sql.DATE_FORMAT)
        if first_date and row[0].date() < first_date:
            continue
        insert_rows.append(f"\n("\
            f"'{ric}', '{date}', '{float(row[1])}', {float(row[2])}, {float(row[3])}, {float(row[4])}, "\
            f"{int(row[5])}, {int(row[6])})")
    if insert_rows:
        insert_query = f"INSERT OR IGNORE INTO {HISTORY_TABLE} VALUES {','.join(insert_rows)};"
        return sql.modify(insert_query, PRICES_DB)
    return False


if __name__ == '__main__':
    # get_expiry_dates()
    # index_ids = sql.fetch(f'SELECT index_id FROM {INDEX_TABLE}', META_DB)
    # for code, in index_ids:
    #     update_components(code)
    set_token()
    # update_stock_derivatives()
    future_ids = sql.fetch(f'SELECT future_id FROM {FUTURE_TABLE}', META_DB)
    for code, in future_ids:
        contracts_info = sql.fetch(f"SELECT * FROM {FUT_CONTRACT_TABLE} WHERE series_id='{code}'", META_DB)
        if contracts_info:
            continue
        update_futures_details(code)

# create_query = f"CREATE TABLE {EQUITY_TABLE} ("\
#     "stock_id TEXT, ric TEXT, currency TEXT, name TEXT, industry TEXT, "\
#     "lot_size INTEGER, tick_size REAL, issued_shares INTEGER, dividend_yield REAL, "\
#     f"CONSTRAINT {EQUITY_TABLE}_pk PRIMARY KEY (stock_id))"
# create_query = f"CREATE TABLE {INDEX_TABLE} ("\
#     "index_id TEXT, ric TEXT, currency TEXT, name TEXT, "\
#     f"CONSTRAINT {INDEX_TABLE}_pk PRIMARY KEY (index_id))"

# create_query = f"CREATE TABLE {INDEX_COMPOSITION_TABLE} ("\
#     "index_id TEXT, component_id TEXT, from_date TEXT, "\
#     f"CONSTRAINT {INDEX_COMPOSITION_TABLE}_pk PRIMARY KEY (index_id, component_id, from_date))"

# create_query = f"CREATE TABLE {FUTURE_TABLE} ("\
#     "future_id TEXT, underlier_id TEXT, lot_size INTEGER, "\
#     f"CONSTRAINT {FUTURE_TABLE}_pk PRIMARY KEY (future_id))"
# for id, code in [('HSI', 'hsi'), ('HTI', 'hstech'), ('HHI', 'hscei')]:
#     insert_query = f"INSERT INTO {FUTURE_TABLE} VALUES ('{id}', '{code}', 50)"
#     sql.modify(insert_query, META_DB)

# create_query = f"CREATE TABLE {FUT_CONTRACT_TABLE} ("\
#     "contract_id TEXT, series_id TEXT, contract_month TEXT, "\
#     "last_trade_date TEXT, settlement_date TEXT, extended_session_id TEXT, "\
#     f"CONSTRAINT {FUT_CONTRACT_TABLE}_pk PRIMARY KEY (contract_id))"
# sql.modify(create_query, META_DB)

# create_query = f"CREATE TABLE {HISTORY_TABLE} ("\
#     "instrument_id TEXT, date TEXT, open REAL, high REAL, low REAL, close REAL, volume INTEGER, turnover INTEGER, "\
#     f"CONSTRAINT {HISTORY_TABLE}_pk PRIMARY KEY (instrument_id, date))"
# sql.modify(create_query, PRICES_DB)

        # 'KST': 500,
        # 'AIA': 1000,
        # 'MIU': 1000,
        # 'PHT': 500,
        # 'LAU': 200,
        # 'MET': 500,
        # 'JDC': 500,
        # 'BLI': 60,
        # 'PEN': 200,
        # 'BIU': 150,
        # 'ALB': 500,
        # 'HKB': 400,
        # 'HEX': 100,
        # 'TCH': 100,
        # 'CTC': 2000,
        # 'PEC': 2000,
        # 'CHT': 500,
        # 'PAI': 500,
