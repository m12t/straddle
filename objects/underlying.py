import math
import logging
from bisect import insort
from datetime import datetime, timedelta

import utils
from exceptions.exceptions import InitError

import numpy as np
from ib_insync import Stock, Index, Option


class Underlying:
    def __init__(self, app: object, underlying: object):
        self._logger = logging.getLogger(__name__)
        self._logger.info(f'initializing underlying: {underlying["symbol"]}')
        self.is_alive = True
        self.app = app  # the running app instance
        self.underlying = underlying  # sqlite3.Row object
        self.dbid = None  # id taken from DB. used for FK
        self.con_id = None
        self.symbol = None
        self.sec_type = None  # STK (includes ETF), IND
        self.currency = None
        self.exchange = None
        self.primary_exchange = None
        self.option_exchange = None
        self.option_trading_class = None
        self.option_multiplier = None
        self.option_settlement = None
        self.option_style = None  # AMERICAN or EUROPEAN
        self._unpack_underlying()  # populate the above underlying attributes
        self.contract = None  # is an ib-insync contract object
        self.data_line = None  # live market data line for the underlying
        self.chain = []  # contracts for all strikes for the nearest 2 tenors
        self.strikes = []  # compute unique strikes once, instead of every iter
        self.straddle_options = []  # live data for nearest-strike options
        self.strangle_options = []  # live data for next 2 nearest-strikes
        self.options_expiration: datetime = None
        self.open_time, self.close_time = utils.get_schedule(self.exchange)
        self.HOLDING_PERIOD = 29  # minutes
        self.t1, self.t2 = self._build_permissible_times()
        self.iv = 0.0  # avg of askGreeks.IV of all contracts in data line
        self.real_vol_last = 0.0  # involves pulling data from db
        self.real_vol_ma = 0.0  # involves pulling data from db
        self.vol_ma_gap = 0.0  # a calculation of above attributes
        self.vol_gap = 0.0  # a calculation of above attributes
        self.features = []  # feature vector to feed into model
        self._post_init()

    def _post_init(self) -> None:
        self._build_underlying_contract()
        self._req_underlying_data()
        self._build_option_chain()
        self._grab_strikes()
        self._manage_option_data_lines()
        self._run_validation_suite()

    def _unpack_underlying(self) -> None:
        row = self.underlying  # sqlite3.Row instance
        try:
            self.dbid: int = row['ID']  # int
            self.con_id: int = row['ConID']
            self.symbol: str = row['Symbol']
            self.sec_type: str = row['SecType']
            self.currency: str = row['Currency']
            self.exchange: str = row['Exchange'] or 'SMART'
            self.primary_exchange: str = row['PrimaryExchange'] or ''
            self.option_exchange: str = row['OptionExchange'] or 'SMART'
            self.option_trading_class: str = row['OptionTradingClass'] or ''
            self.option_multiplier: str = row['OptionMultiplier']
            self.option_settlement: str = row['OptionSettlement']
            self.option_style: str = row['OptionStyle']
        except AttributeError as e:
            # TODO: is there another error type that can result here?
            #       IndexError? TypeError?
            self._logger.exception(e)
            self.shutdown(on_init=True)

    def _run_validation_suite(self) -> None:
        """check that all values are as expected, if not, shutdown"""
        self._validate_datetimes()
        self._validate_db_data()
        self._validate_contract()
        self._validate_chain()

    def _validate_datetimes(self):
        try:
            assert isinstance(self.t1, datetime)
            assert isinstance(self.t2, datetime)
            assert isinstance(self.open_time, datetime)
            assert isinstance(self.close_time, datetime)
            assert isinstance(self.options_expiration, datetime)
            today = self.app.session_start_time.date()
            assert self.open_time.date() == today
            assert self.close_time.date() == today
            assert self.t1.date() == today
            assert self.t2.date() == today
        except AssertionError as e:
            self._logger.exception(e)
            self.shutdown(on_init=True)

    def _validate_db_data(self):
        """assert that database data are of the correct type"""
        sec_types = {'STK', 'IND'}
        try:
            assert isinstance(self.dbid, int)
            assert isinstance(self.con_id, int)
            assert isinstance(self.symbol, str)
            assert isinstance(self.sec_type, str)
            assert isinstance(self.currency, str)
            assert isinstance(self.exchange, str)
            assert isinstance(self.primary_exchange, str)
            assert isinstance(self.option_trading_class, str)
            assert isinstance(self.option_multiplier, str)
            assert isinstance(self.option_settlement, str)
            assert isinstance(self.option_style, str)
            assert self.sec_type in sec_types
        except AssertionError as e:
            self._logger.exception(e)
            self.shutdown(on_init=True)

    def _validate_contract(self) -> None:
        """check that the conract has a conId and symbol and that they match
           the expected values as found in the respective class attributes"""
        contract = self.contract
        try:
            conid = contract.conId
            symbol = contract.symbol
            assert isinstance(conid, int)
            assert isinstance(symbol, str)
            assert conid == self.con_id
            assert symbol.upper() == self.symbol.upper()
        except (AssertionError, AttributeError) as e:
            self._logger.exception(e)
            self.shutdown(on_init=True)

    def _validate_chain(self):
        # alternative architecture that removes invalid options from the chain,
        # preserving valid ones and not failing the entire underlying on
        # a few bad contracts. If any valid ones exist, the Underlying
        # is kept alive.
        rights = {'C', 'CALL', 'P', 'PUT'}
        options = self.chain.copy()
        self.chain.clear()
        for option in options:
            try:
                # NOTE: at this stage these values should be populated since
                #       the call to ib.qualifyContracts is blocking.
                conid = option.conId
                symbol = option.symbol
                exchange = option.exchange
                strike = option.strike
                right = option.right
                expiration = option.lastTradeDateOrContractMonth
                assert isinstance(conid, int)
                assert isinstance(symbol, str)
                assert isinstance(exchange, str)
                assert isinstance(strike, float)
                assert isinstance(right, str)
                assert isinstance(expiration, str)
                assert symbol.upper() == self.symbol.upper()
                assert right in rights
                self.chain.append(option)
            except (AssertionError, AttributeError):
                continue
        if not self.chain:
            self._logger.error('chain of length 0 encountered.')
            self.shutdown(on_init=True)

    def _build_permissible_times(self) -> tuple[datetime]:
        """create datetimes representing the earliest and latest permissible
           times that a position can be opened for the day. Because it builds
           on open and close times already pulled from pandas market calendars,
           it tracks early closes and other schedule irregularities."""
        try:
            t1 = self.open_time + timedelta(minutes=15)  # 15 mins after open
            t2 = self.close_time - timedelta(hours=4)  # 4 hours before close
            latest_possible = t2 + timedelta(minutes=self.HOLDING_PERIOD)
            assert latest_possible < self.close_time
        except (TypeError, AttributeError) as e:
            self._logger.exception(e)
            self.shutdown(on_init=True)
        return t1, t2

    def build_feature_vector(self, time) -> None:
        """Refresh data and then populate a feature vector"""
        self._refresh_data(time)
        self.features = np.array([
            self.vol_ma_gap,
            self.vol_gap,
            self.iv,
            self.real_vol_last,
            self.real_vol_ma
        ])

    def _req_underlying_data(self) -> None:
        self.data_line = self.app.ib.reqMktData(self.contract)
        self._load_data_line()

    def _load_data_line(self, timeout: int = 12) -> None:
        """wait for the market data line to load, or a timeout of 12 seconds"""
        self._logger.info(f'loading data_line for {self.symbol}')
        loaded = False
        for _ in range(timeout*10):
            if not math.isnan(self.data_line.marketPrice()):
                loaded = True
                break
            self.app.ib.sleep(0.1)
        if not loaded:
            self._logger.error(f'failed to load {self.symbol} data_line')
            self.shutdown(on_init=True)

    def _cancel_data_line(self) -> None:
        self.app.ib.cancelMktData(self.data_line.contract)

    def _build_underlying_contract(self) -> None:
        """Build the underlying data contract to track its price"""
        if self.sec_type == 'STK':
            contract = Stock(
                symbol=self.symbol, exchange='SMART',
                currency=self.currency,
                primaryExchange=self.primary_exchange)
        elif self.sec_type == 'IND':
            contract = Index(
                symbol=self.symbol, exchange=self.exchange,
                currency=self.currency)
        else:
            self._logger.error('Invalid SecType encountered.')
            self.shutdown(on_init=True)
        try:
            self.contract = self.app.ib.qualifyContracts(contract)[0]
            self._load_contract()
        except IndexError as e:
            self._logger.exception(e)
            self.shutdown(on_init=True)

    def _load_contract(self, timeout: int = 12) -> None:
        """wait for the contract to load with a timeout of 12 seconds"""
        loaded = False
        for _ in range(timeout*10):
            # timeout * 10 so timeout is in seconds and sleep is 0.1s
            try:
                if self.contract.conId != 0:
                    loaded = True
                    break
            except AttributeError:
                continue
            self.app.ib.sleep(0.1)
        if not loaded:
            self._logger.error(f'failed to load {self.symbol} contract')
            self.shutdown(on_init=True)

    def _build_option_chain(self) -> None:
        """Request the options chain, filter down by exchange and
           trading class. Then build ib Option contract objects and
           verify them by using ib.qualifyContracts. assign the
           class attribute `chain` to this list of valid Options."""
        chain = self.app.ib.reqSecDefOptParams(
            self.symbol, '', self.sec_type, self.con_id)  # a blocking method
        chain = [c for c in chain if c.exchange == self.option_exchange]
        if self.option_trading_class:
            chain = [c for c in chain if c.tradingClass ==
                     self.option_trading_class]
        expiration = self._filter_expirations(chain)
        self.options_expiration = self._get_expiration_dt(expiration)
        strikes = self._filter_strikes(chain)
        raw = self._build_contracts(expiration, strikes)
        self.chain = [c for c in self.app.ib.qualifyContracts(*raw) if c != []]

    def _grab_strikes(self) -> list:
        """set a class attribute with all of the unique strikes
           from the options chain once, instead of calculating
           it every loop in _get_adjacent_strikes(). NOTE:
           `.tolist()` is required for `insort` to work downstream"""
        self.strikes = np.unique(sorted(
            [c.strike for c in self.chain])).tolist()

    def _refresh_data(self, time) -> None:
        self._manage_option_data_lines()
        self._refresh_iv()  # pull the newest values from data lines
        self._refresh_real_vol(time)  # real_vol_last and real_vol_ma
        self.vol_ma_gap = self.real_vol_ma - self.iv
        self.vol_gap = self.real_vol_last - self.iv

    def _calc_realized_vol(self, a: list) -> float:
        try:
            a = a[-1]
            return np.sqrt(252 * 390 * np.log(a[0] / a[1]) ** 2)
        except (IndexError, TypeError):
            return float('nan')

    def _cal_realized_vol_ma(self, a: list) -> float:
        try:
            return np.sqrt(252 * 390 * np.log(a[:, 0] / a[:, 1]) ** 2).mean()
        except (IndexError, TypeError):
            return float('nan')

    def _refresh_real_vol(self, time) -> None:
        data = self.app.db.get_price_extrema(self.dbid, time, lookback=15)
        self.real_vol_last = self._calc_realized_vol(data)
        self.real_vol_ma = self._cal_realized_vol_ma(data)

    def _refresh_iv(self) -> None:
        try:
            a = np.array([o.askGreeks.impliedVol for
                          o in self.straddle_options])
            a = a[~np.isnan(a)]  # remove nan values from array
            self.iv = np.mean(a)  # set iv == average IV
        except (AttributeError, TypeError):
            self.iv = float('nan')

    def _req_option_data(self, contracts: list) -> list:
        return [self.app.ib.reqMktData(c) for c in contracts]

    def _cancel_contracts(self, contracts: list) -> None:
        for contract in contracts:
            self.app.ib.cancelMktData(contract)

    def _build_contracts(self, expiration: str, strikes: list) -> list:
        contracts = []
        for strike in strikes:
            for right in ['CALL', 'PUT']:
                contract = Option(
                    symbol=self.symbol,
                    lastTradeDateOrContractMonth=expiration,
                    tradingClass=self.option_trading_class,
                    strike=strike,
                    right=right,
                    exchange=self.option_exchange,
                    multiplier=self.option_multiplier)
                contracts.append(contract)
        return contracts

    def _get_expiration_dt(self, exp: str) -> datetime:
        """take the string expiration and return the exact
           datetime expiration of the option based on exchange
           hours."""
        return utils.get_schedule(self.exchange, end=exp)[1]

    def _filter_expirations(self, chain: list) -> str:
        """sort the strikes by expiration and return the nearest tenor"""
        # throw a try block aroudn this?? IndexError??, other errors??
        return str(np.unique(sorted([c.expirations for c in chain]))[0])

    def _filter_strikes(self, chain: list) -> np.array:
        """get all strikes from potentially nested list of chains
           and then flatten the nested arrays to return a 1D array"""
        return np.unique([c.strikes for c in chain])

    def _validate_spot(self, spot) -> bool:
        try:
            assert isinstance(spot, float)
            assert not math.isnan(spot)
            assert spot > 0
            return True
        except AssertionError:
            return False

    def _get_spot(self) -> float:
        """try to get the spot price by first accessing the value in the
           `last` attribute of data_line, if that fails, try the ib_insync
           marketPrice() method, if that fails validation, lastly get the
           last known price from the db."""
        try:
            spot = self.data_line.last
        except AttributeError:
            spot = None
        if not self._validate_spot(spot):
            spot = self.data_line.marketPrice()
            if not self._validate_spot(spot):
                spot = self.app.db.get_spot(self.dbid)
                if not self._validate_spot(spot):
                    spot = 0.0
        return spot

    def _get_adjacent_strikes(self) -> list:
        spot = self._get_spot()
        strikes = self.strikes.copy()
        # TODO: see using lambda with min() instead of insort. test speed:
        # closest_strike = min(strikes, key=lambda value: abs(value - spot))
        insort(strikes, spot)  # add spot into the list of sorted strikes
        spot_index = strikes.index(spot)
        strikes.remove(spot)
        next_strikes = strikes[spot_index - 3:spot_index + 3]
        return next_strikes

    def _remove_data_lines(self, invalid_strikes: set) -> None:
        # are the contracts becoming [] after cancellation?
        print('straddle_options', self.straddle_options)  # DAT
        print('len odl', len(self.straddle_options))  # DAT
        self.straddle_options.remove(
            [c for c in self.straddle_options if
             c.contract.strike in invalid_strikes])
        self.strangle_options.remove(
            [c for c in self.strangle_options if
             c.contract.strike in invalid_strikes])

    def _handle_invalid_contracts(self, invalid_strikes: set) -> None:
        """Cancel ib mkt data lines for every unneeded contract"""
        self._cancel_contracts(
            [c.contract for c in self.straddle_options +
             self.strangle_options if
             c.contract.strike in invalid_strikes])

    def _handle_missing_contracts(self, missing_strikes: set) -> None:
        """Request market data for needed contracts and append
           those contracts to straddle_options and let _sort_options
           handle sorting the strikes downstream. Also log the new options'
           details to the database, letting the DB handle duplicate errors."""
        contracts = [c for c in self.chain if c.strike in missing_strikes]
        self.straddle_options.extend(self._req_option_data(contracts))
        self.app.db.log_options(self.dbid, contracts)

    def _sort_options(self, needed_strikes: list) -> None:
        """Ensure that self.straddle_options houses only the contracts
           that are immediate straddles of the underlying spot price.
           then ensure that the preloaded_data line houses the next
           further strikes out."""
        straddle_strikes = set(needed_strikes[2:4])
        del needed_strikes[2:4]
        strangle_strikes = set(needed_strikes)
        options = self.straddle_options + self.strangle_options
        # ^ NOTE: no copy() needed here because a new list is created with `+`
        self.straddle_options.clear()
        self.strangle_options.clear()
        self.straddle_options.extend(
            [o for o in options if o.contract.strike in straddle_strikes])
        self.strangle_options.extend(
            [o for o in options if o.contract.strike in strangle_strikes])

    def _manage_option_data_lines(self) -> None:
        """Method to manage option data lines to have only the necessary
           data lines live. It does this by finding the strikes of all
           live data lines and comparing them to the strikes it should
           have. Any unnecessary data lines are thusly cancelled and any
           needed but not currently live data lines are requested. This
           method is also used on initialization to populate straddle and
           preload lists with live options data."""
        options = self.straddle_options + self.strangle_options
        # TODO: make live_strikes a class attribute updated when contracts
        #       change, this way it isn't necessarily calculated every iter.
        live_strikes = np.unique([c.contract.strike for c in options]).tolist()
        needed_strikes = self._get_adjacent_strikes()
        missing_strikes = set(needed_strikes) - set(live_strikes)
        invalid_strikes = set(live_strikes) - set(needed_strikes)
        if invalid_strikes:
            self._handle_invalid_contracts(invalid_strikes)
        if missing_strikes:
            self._handle_missing_contracts(missing_strikes)
        self._sort_options(needed_strikes)

    def shutdown(self, on_init: bool = False) -> None:
        """close all market data lines, set is_alive flag to False, trigger
           a call to App to remove this underlying from its list."""
        if self.data_line is not None:
            self._cancel_data_line()
        options = self.strangle_options + self.straddle_options
        if options:
            self._cancel_contracts([c.contract for c in options])
        self.is_alive = False
        if on_init:
            raise InitError
        else:
            self.app.refresh_underlyings()
