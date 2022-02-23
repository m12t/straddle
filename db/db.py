import sqlite3
import logging
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta

import numpy as np


class DB:
    def __init__(self, path: str, tz: ZoneInfo = None) -> None:
        self._logger = logging.getLogger(__name__)
        self.tz = tz or ZoneInfo("America/New_York")
        self.con = sqlite3.connect(database=path)
        self.con.row_factory = sqlite3.Row

    def log_underlying(self, conid: int, symbol: str, sec_type: str,
                       currency: str, option_style: str,
                       option_settlement: str, option_multiplier: str,
                       option_trading_class: str, is_1256_contract: bool,
                       exchange: str = None, option_exchange: str = None,
                       primary_exchange: str = None) -> None:
        """Manual entry of underlying attributes from separate script
           `add_underlying.py` located within the /db directory is acceptable
           due to the infrequency of adding new underlyings."""
        try:
            with self.con:
                self.con.execute(
                    """INSERT INTO Underlying(
                        ConID, Symbol, SecType, Currency, OptionStyle,
                        OptionSettlement, OptionMultiplier, OptionExchange,
                        OptionTradingClass, Is1256Contract, Exchange,
                        PrimaryExchange)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (conid, symbol, sec_type, currency, option_style,
                     option_settlement, option_multiplier, option_exchange,
                     option_trading_class, is_1256_contract,
                     exchange, primary_exchange))
        except sqlite3.IntegrityError:
            pass
        except sqlite3.DatabaseError as e:
            self._logger.exception(e)

    def log_options(self, underlying_id: int, contracts: list[object]) -> None:
        """log options based on contracts being passed in instead of options.
           This improves usability as Underlying can log options with only the
           contracts before mktData lines are requested. This method follows
           a philosophy of 1 try with multiple excepts as opposed to two
           separate try blocks. This way is preferred because any 1
           exception should end the sequence. This works for log_options,
           but not log_option_data because log option can't take NULL values,
           but option_data can and should be able to take NULL values."""
        for contract in contracts:
            try:
                con_id: int = contract.conId
                exp: str = contract.lastTradeDateOrContractMonth
                strike: float = contract.strike
                right: str = contract.right
                exchange: str = contract.exchange or None
                with self.con:
                    self.con.execute(
                        """INSERT INTO Option(
                            ConID, UnderlyingID, LastTradeDateOrContractMonth,
                            Right, Strike, Exchange)
                        VALUES (?, ?, ?, ?, ?, ?)""",
                        (con_id, underlying_id, exp, right, strike, exchange))
            except sqlite3.IntegrityError:
                # unique constraint failed
                continue
            except AttributeError as e:
                self._logger.exception(e)
                # failed to extract option data
                continue
            except sqlite3.DatabaseError as e:
                self._logger.exception(e)
                continue

    def log_option(self, underlying_id: int, options: list[object]) -> None:
        print('I WAS CALLED AND SHOULD NOT HAVE BEEN CALLED!!!')
        raise RuntimeError('I WAS CALLED AND SHOULD NOT HAVE BEEN CALLED!!!')
        # DEPRECATED. DELETE AFTER TESTING
        """philosophy of 1 try with multiple excepts as opposed to two
           separate try blocks. This way is preferred because any 1
           exception should end the sequence. This works for log_option,
           but not log_option_data because log option can't take NULL values,
           but option_data can and should be able to take NULL values."""
        for option in options:
            try:
                contract: object = option.contract
                con_id: int = contract.conId
                last_trade_date: str = contract.lastTradeDateOrContractMonth
                strike: float = contract.strike
                right: str = contract.right
                exchange: str = contract.exchange or None
                with self.con:
                    self.con.execute(
                        """INSERT INTO Option(
                            ConID, UnderlyingID, LastTradeDateOrContractMonth,
                            Right, Strike, Exchange)
                        VALUES (?, ?, ?, ?, ?, ?)""",
                        (con_id, underlying_id, last_trade_date,
                         right, strike, exchange))
            except sqlite3.IntegrityError as e:
                self._logger.exception(e)
                # unique constraint failed
                continue
            except AttributeError as e:
                self._logger.exception(e)
                # failed to extract option data
                continue
            except sqlite3.DatabaseError as e:
                self._logger.exception(e)
                continue

    def _calc_missed_timestamps(self, last_logged_time: datetime,
                                num_iters_missed: int) -> list:
        """take the numer of iterations missed, the last logged timestamp,
            and output a list of valid timestamps to use when populating
            the DB with NULL values."""
        missed_timestamps = []
        for i in range(1, num_iters_missed + 1):
            missed_timestamps.append(
                last_logged_time + timedelta(microseconds=i * 250000))
        return missed_timestamps

    def log_null(self, underlyings: list[object],
                 last_logged_time: datetime, num_iters_missed: int) -> None:
        missed_timestamps = self._calc_missed_timestamps(
            last_logged_time, num_iters_missed)
        for time in missed_timestamps:
            for underlying in underlyings:
                try:
                    with self.con:
                        self.con.execute(
                            """INSERT INTO UnderlyingData(UnderlyingID, Time)
                               VALUES (?, ?)""",
                            (underlying.dbid, time))
                except sqlite3.DatabaseError:
                    pass  # `pass` to run code below, `continue` would NOT.
                for option in underlying.straddle_options:
                    try:
                        with self.con:
                            option_id = self.get_option_id_from_conid(
                                option.contract.conId)
                            self.con.execute(
                                """INSERT INTO OptionData(OptionID, Time)
                                   VALUES (?, ?)""",
                                (option_id, time))
                    except (sqlite3.DatabaseError, AttributeError):
                        continue

    def log_underlying_data(self, underlying_id: int, data_line: object,
                            time: datetime) -> None:
        try:
            price = data_line.last
        except AttributeError:
            price = None
        try:
            with self.con:
                self.con.execute(
                    """INSERT INTO UnderlyingData(UnderlyingID, Time, Price)
                       VALUES (?, ?, ?)""",
                    (underlying_id, time, price))
        except sqlite3.DatabaseError:
            pass

    def log_option_data(self, options: list[object], time: datetime) -> None:
        """this can accept a ticker object as it is only ever called after a
           ticker is present and a mktData line open."""
        for option in options:
            option_conid = option.contract.conId
            option_id = self.get_option_id_from_conid(option_conid)
            try:
                bid = option.bid
                ask = option.ask
                bid_iv = option.bidGreeks.impliedVol
                ask_iv = option.askGreeks.impliedVol
            except AttributeError:
                bid = None
                ask = None
                bid_iv = None
                ask_iv = None
            try:
                with self.con:
                    self.con.execute(
                        """INSERT INTO OptionData(
                            OptionID, Time, Ask, Bid, AskImpVol, BidImpVol)
                        VALUES (?, ?, ?, ?, ?, ?)""",
                        (option_id, time, bid, ask, bid_iv, ask_iv))
            except sqlite3.DatabaseError as e:
                self._logger.exception(e)

    def log_buy_signal(self, underlying_id: int, time: datetime) -> None:
        try:
            with self.con:
                self.con.execute(
                    """INSERT INTO BuySignal(UnderlyingID, Time)
                       VALUES (?, ?)""",
                    (underlying_id, time))
        except sqlite3.DatabaseError as e:
            self._logger.exception(e)

    def log_trade(self, trade: object, acc_num: str) -> None:
        time = datetime.now(tz=self.tz).replace(tzinfo=None)
        try:
            option_id = self.get_option_id_from_conid(trade.contract.conId)
            quantity = trade.filled()
            com = sum([f.commissionReport.commission for f in trade.fills])
            avg_price = trade.orderStatus.avgFillPrice
            with self.con:
                self.con.execute(
                    """INSERT INTO Trade(
                        AccountNum, Time, OptionID,
                        Quantity, AvgPrice, Commission)
                    VALUES (?, ?, ?, ?, ?, ?)""",
                    (acc_num, time, option_id,
                     quantity, avg_price, com))
        except (AttributeError, TypeError):
            pass
        except sqlite3.DatabaseError as e:
            self._logger.exception(e)

    def get_option_id_from_conid(self, con_id: int) -> int:
        # TESTED.
        """return the database ID of an option instance given its con_id"""
        self.con.row_factory = lambda _, row: row[0]
        return self.con.execute(
            "SELECT ID FROM Option WHERE ConID = :conid", {"conid": con_id}
            ).fetchone()

    def get_underlying_id(self, symbol: str) -> int:
        # TESTED
        self.con.row_factory = lambda _, row: row[0]
        return self.con.execute(
            """SELECT ID FROM Underlying
               WHERE Symbol = :symbol""", {"symbol": symbol}).fetchone()

    def get_all_underlyings(self) -> list[sqlite3.Row]:
        # TESTED
        self.con.row_factory = sqlite3.Row
        return self.con.execute("""SELECT * FROM Underlying""").fetchall()

    def delete_all_trades(self) -> None:
        """used for testing to clear out trade data to bypass the
           `net position exists` error."""
        self.con.execute("DELETE FROM TRADE")

    def get_all_trades(self) -> list[sqlite3.Row]:
        self.con.row_factory = sqlite3.Row
        return self.con.execute("""SELECT * FROM Trade""").fetchall()

    def get_all_options(self) -> list[sqlite3.Row]:
        self.con.row_factory = sqlite3.Row
        return self.con.execute("""SELECT * FROM Option""").fetchall()

    def get_sigma(self, underlying_id: int,
                  time: datetime, lookback: int) -> float:
        # TESTED
        """calculate the realized volatility over the lookback period."""
        a = self.get_price_extrema(underlying_id, time, lookback)
        try:
            return np.sqrt(252 * 390 * np.log(a[:, 0] / a[:, 1]) ** 2).mean()
        except IndexError:
            return float('nan')

    def get_price_extrema(self, underlying_id: int, time: datetime,
                          lookback: int) -> np.array:
        # TESTED
        """Return the min and max realized prices on a minute period basis for
           the number of periods, in minutes, given by `lookback`. The output
           array is used to calculate realized volatility and realvolma"""
        self.con.row_factory = lambda _, row: row[0:2]
        t1 = time - timedelta(minutes=lookback)
        data = self.con.execute(
            """SELECT MIN(Price), MAX(Price)
               FROM UnderlyingData
               WHERE UnderlyingID = :id
                    AND Time > :t1
                    AND Price IS NOT NULL
               GROUP BY strftime('%d%H%M', Time)
               ORDER BY Time ASC""",
            {"id": underlying_id, "t1": t1}).fetchall()
        return np.array(data)

    def get_spot(self, id: int) -> float:
        # TESTED
        """return the last not null price for a given underlying"""
        self.con.row_factory = lambda _, row: row[0]
        try:
            return self.con.execute(
                    """SELECT Price
                       FROM UnderlyingData
                       WHERE UnderlyingID = :id
                            AND Price IS NOT NULL
                       ORDER BY Time DESC
                       LIMIT 1""", {"id": id}).fetchone()
        except sqlite3.DatabaseError:
            return 0.0

    def get_position_size(self, symbol: str, time: datetime) -> int:
        # TESTED
        """return net position for an underlying since session start time"""
        # NOTE: (untested) this uses native SQL sum instead of python...
        self.con.row_factory = lambda _, row: row[0]
        return self.con.execute(
            """SELECT SUM(Quantity)
               FROM Trade
               JOIN Option
               ON Trade.OptionID = Option.ID
               JOIN Underlying
               ON Option.UnderlyingID = Underlying.ID
               WHERE underlying.Symbol = :symbol
               AND Trade.Time > :time""",
            {"symbol": symbol, "time": time}).fetchone()

    def get_positions(self, symbol: str, time: datetime) -> list[sqlite3.Row]:
        # TESTED
        """Return details on the positions opened after a
           given time that were saved to this db. this is used in monitor.py
           NOTE: this may not include all trades in IB's records"""
        self.con.row_factory = sqlite3.Row
        return self.con.execute(
            """SELECT SUM(t.Quantity) AS quantity,
                      t.AvgPrice AS avg_price,
                      o.Strike AS strike,
                      o.Right AS right,
                      o.Exchange AS exchange,
                      o.LastTradeDateOrContractMonth AS expiration,
                      u.OptionMultiplier AS multiplier,
                      u.OptionTradingClass AS trading_class
               FROM Trade AS t
               JOIN Option AS o
                    ON t.OptionID = o.ID
               JOIN Underlying AS u
                    ON o.UnderlyingID = u.ID
               WHERE u.Symbol = :symbol
                    AND t.Time > :time
               GROUP BY strike, right, expiration""",
            {"symbol": symbol, "time": time}).fetchall()

    def get_all_positions(self, time: datetime) -> list[sqlite3.Row]:
        """Return details on all trades placed since session start time,
           for all symbols. Used to check for any unclosed positions on
           shutdown of the algorithm."""
        self.con.row_factory = sqlite3.Row
        return self.con.execute(
            """SELECT SUM(t.Quantity) AS quantity,
                      t.AvgPrice AS avg_price,
                      o.Strike AS strike,
                      o.Right AS right,
                      o.Exchange AS exchange,
                      o.LastTradeDateOrContractMonth AS expiration,
                      u.symbol AS symbol,
                      u.OptionMultiplier AS multiplier,
                      u.OptionTradingClass AS trading_class
               FROM Trade AS t
               JOIN Option AS o
                    ON t.OptionID = o.ID
               JOIN Underlying AS u
                    ON o.UnderlyingID = u.ID
               WHERE t.Time > :time
               GROUP BY strike, right, expiration""",
            {"time": time}).fetchall()

    def close(self) -> None:
        """commit changes and close the connection with the db"""
        self.con.commit()  # commit any unsaved changes
        self.con.close()
