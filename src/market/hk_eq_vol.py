from common.models.market_data import MarketDataType, OptionDataFlag, SessionType
from volatility.instruments.option import CallOption, PutOption
from volatility.models.listed_options_construct import ListedOptionsConstruct, ModelStrikeSlice, ModelStrikeLine

from data_api import hkex_client
from data_api import hkex_server

def get_vol_model(code: str, session_type: SessionType = None):
    price_type, weight_type = MarketDataType.MID, MarketDataType.SPREAD
    futures_list = hkex_client.get_futures_contracts(code, session_type=session_type)
    update_dtm, quotes = hkex_server.load_futures_quotes(code, session_type=session_type)
    value_date = update_dtm.date()
    option_chain = []
    for future in futures_list:
        expiry = future.expiry
        if future.data_id not in quotes:
            continue
        fut_price = quotes[future.data_id][price_type]
        if not fut_price:
            continue
        future.data[value_date] = fut_price
        option_data = hkex_server.get_options_chain(code, expiry.strftime('%m%Y'), session_type=session_type)
        strike_lines = []
        for strike, strike_info in option_data.items():
            call_option = CallOption(future, expiry, strike)
            put_option = PutOption(future, expiry, strike)
            call_weight, put_weight = 0, 0
            if OptionDataFlag.CALL in strike_info:
                price = strike_info[OptionDataFlag.CALL][price_type]
                if price:
                    call_option.data[value_date] = price
                    call_weight = 1 / (strike_info[OptionDataFlag.CALL][weight_type])
            if OptionDataFlag.PUT in strike_info:
                price = strike_info[OptionDataFlag.PUT][price_type]
                if price:
                    put_option.data[value_date] = price
                    put_weight = 1 / (strike_info[OptionDataFlag.PUT][weight_type])
            strike_lines.append(ModelStrikeLine(strike, call_option, put_option, call_weight, put_weight))
        df = 1/(1 + 0.05 * future.get_expiry_dcf(value_date))
        option_chain.append(ModelStrikeSlice(expiry, df, strike_lines))
    return ListedOptionsConstruct(value_date, option_chain, name=f'{code}-Vol')

def construct(instrument_ids: list[str]):
    # discount_curve = CurveContext().get_rate_curve('USD-SOFR', value_date)
    return [get_vol_model(id) for id in instrument_ids]

def get_vol_surface_data(code: str, model_type: str):
    vol_model = get_vol_model(code)
    vol_surface = vol_model.build(model_type)
    # err_list = vol_model.get_calibration_errors(vol_surface)
    return vol_model.get_vols_graph(vol_surface)
