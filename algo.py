#!/usr/bin/env python3
import math
import time
import logging
import threading
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
# from configparser import ConfigParser

from logs.setup import config_logger
import utils
import transact
import monitor
from db.db import DB
from objects.model import Model
from objects.account import Account
from objects.underlying import Underlying
from exceptions.exceptions import ValidationError, InitError

from ib_insync import IB


class App:
    def __init__(self, account_num: str, port: int, timeout: int = 120,
                 testing: bool = True) -> None:
        self.account_num = account_num
        self.port = port
        self.timeout = timeout
        self.mode = 'LIVE' if self.port in {4001, 7496} else 'PAPER'
        self.mode = 'TESTING' if testing else self.mode  # DAT
        self.tz = ZoneInfo("America/New_York")
        self.session_start_time = utils.get_now(tz=self.tz)
        config_logger(self.mode, self.session_start_time, __name__)
        self._logger = logging.getLogger(__name__)
        self.ib = IB()
        self.ibc = utils.get_controller()  # tool to control IBGateway client
        self.watchdog = utils.get_watchdog(self.ib, self.ibc, self.port)
        self.db = DB(path='./db/alpha.db', tz=self.tz)
        self.model = Model()
        self.account = Account(self)
        self.underlying_attributes = None
        self.untracked = []
        self.underlyings = []  # is gradually populated as exchanges open
        self.next_open_time = None
        self.first_open_time = None
        self.next_close_time = None
        self.buffer = None
        self.last_close_time = None
        self._post_init()

    def _post_init(self):
        self.underlying_attributes = self.db.get_all_underlyings()
        self.preliminary_market_check()
        utils.start(self.ib, self.watchdog, timeout=self.timeout)
        self.account.refresh_account()
        self.instantiate_underlyings()
        self._validate_underlyings()
        self.next_open_time = self.get_next_open_time()
        self.first_open_time = self.next_open_time
        self.next_close_time = self.get_next_close_time()
        self.buffer = self.next_close_time - timedelta(minutes=15)
        self.last_close_time = self.get_last_close_time()
        self._validate_datetimes()

    def preliminary_market_check(self) -> None:
        """Perform a preliminary check that at least one exchange is open
           before launching the API client and spinning up the entire algo."""
        sched = [utils.get_schedule(u['Exchange']) for u in
                 self.underlying_attributes]
        sched = [(o, c) for o, c in sched if o is not None and c is not None]
        valid = [True for o, c in sched if self.session_start_time < c]
        if not valid:
            self._logger.critical('All exchanges are closed for the day.')
            self.shutdown(preliminary=True)

    def _validate_underlyings(self):
        """check for underlyings (which will be in self.untracked now)"""
        try:
            valid = []
            for underlying in self.untracked:
                try:
                    assert isinstance(underlying, Underlying)
                    valid.append(underlying)
                except AssertionError as e:
                    self._logger.exception(e)
            self.untracked = valid
            assert self.untracked
        except AssertionError as e:
            self._logger.exception(e)
            self.shutdown()

    def _validate_datetimes(self):
        try:
            assert isinstance(self.next_open_time, datetime)
            assert isinstance(self.first_open_time, datetime)
            assert isinstance(self.next_close_time, datetime)
            assert isinstance(self.buffer, datetime)
            assert isinstance(self.last_close_time, datetime)
            today = self.session_start_time.date()
            assert self.next_open_time.date() == today
            assert self.first_open_time.date() == today
            assert self.next_close_time.date() == today
            assert self.buffer.date() == today
            assert self.last_close_time.date() == today
            assert self.first_open_time < self.last_close_time
        except AssertionError as e:
            self._logger.exception(e)
            self.shutdown()

    def instantiate_underlyings(self) -> None:
        for row in self.underlying_attributes:
            try:
                self.untracked.append(Underlying(self, row))
            except InitError:
                continue

    def refresh_underlyings(self) -> None:
        """cull any underlyings where is_alive == False. This method is called
           by Underlying instances directly when the instance shuts down."""
        self.untracked = [u for u in self.untracked if u.is_alive]
        self.underlyings = [u for u in self.underlyings if u.is_alive]

    def get_next_open_time(self) -> datetime:
        # open time only ever looks at untracked
        if self.untracked:
            # if statement prevent `min() of empty sequence` error
            return min([u.open_time for u in self.untracked])
        else:
            return None

    def get_next_close_time(self) -> datetime:
        """get the next soonest exchange close time from a list
           of all tracked underlyings' exchange close times, prioritizing
           currently tracked underlyings."""
        if self.underlyings:
            return min([u.close_time for u in self.underlyings])
        elif self.untracked:
            return min([u.close_time for u in self.untracked])
        else:
            return None

    def get_last_close_time(self) -> datetime:
        """return the latest close of all underlyings. This
           method is only called once, and uses self.untracked
           because it gets called before any market opened and
           thus self.untracked has all underlying data in it."""
        return max([u.close_time for u in self.untracked])

    def add_open_underlyings(self, now) -> None:
        """add any and all open underlyings not currently being tracked.
           must rebuild the untracked list to exclude the underlyings
           which are now being tracked."""
        self.underlyings += [u for u in self.untracked if now >= u.open_time]
        self.untracked = [u for u in self.untracked if now < u.open_time]

    def cull_closed_underlyings(self, last_timestamp) -> None:
        """remove underlyings whose exchanges are closed from
           the underlyings list by creating a copy of the list,
           clearing the original self.underlyings list, and rebuiding
           it without the underlyings whose exchanges are close."""
        underlyings = self.underlyings.copy()
        self.underlyings.clear()
        for underlying in underlyings:
            if last_timestamp >= underlying.close_time:
                # underlying's exchange is closed.
                underlying.shutdown()  # close all mkt data lines
            else:
                # underlying's exchange is still open
                self.underlyings.append(underlying)

    def check_exchanges(self, now) -> None:
        """ensure the right exchanges are being tracked.
           1. add underlyings as their exchanges open
           2. remove underlyings as their exchanges close"""
        if self.next_open_time:
            # there is at least 1 exchange not yet open.
            if now >= self.next_open_time:
                # next exchange is open, start tracking it.
                self.add_open_underlyings(now)
                self.next_open_time = self.get_next_open_time()
                self.next_close_time = self.get_next_close_time()
                self.buffer = self.next_close_time - timedelta(minutes=15)
                # * Also call self.get_next_close_time() in case the newly
                #   added underlying's exchange closes before all the currently
                #   running underlying's exchanges.
                # * If this wasn't called here, the underlying would not neces-
                #   sarily be unloaded from the loop when its exchange closes.
        if now >= self.buffer:
            # an exchange is <= 15 minutes from closing.
            # check for and cancel existing positions.
            self.check_for_positions()
            if now >= self.next_close_time:
                # some or all underlying exchanges have closed.
                # refresh the values, cull closed underlyings.
                self.cull_closed_underlyings(now)
                self.next_close_time = self.get_next_close_time()
                self.buffer = self.next_close_time - timedelta(minutes=15)

    def run_algo_loop(self) -> None:
        last_time = db_time = utils.get_now(tz=self.tz)
        while (self.account.available_funds > 10000 and
               db_time < self.last_close_time):
            t1 = time.perf_counter()
            self.check_exchanges(db_time)
            cs = math.floor((time.time() % 1) * 100) / 100  # centiseconds
            mod = cs % 0.25
            if mod < 0.12:
                # accept any values less than half way between iterations
                # and prevent time drift by modulating sleep_time such that
                # sleep() ends right when the next iteration should begin.
                cs -= mod  # square off cs such that cs % 0.25 == 0
                db_time = datetime.now(tz=self.tz).replace(
                    tzinfo=None).replace(microsecond=int(cs * 1e6))
                elapsed = (db_time - last_time).total_seconds()
                if elapsed > 0.25:
                    num_elapsed = int(elapsed // 0.25 - 1)
                    self.db.log_null(underlyings=self.underlyings,
                                     num_iters_missed=num_elapsed,
                                     last_logged_time=last_time)
                last_time = db_time
                self.eval_sequence(time=db_time)
                sleep_time = 0.2 - mod
                compute_time = time.perf_counter() - t1
                # print('compute_time', compute_time*1000, 'ms')  # DAT
                self.ib.sleep(max(0.005, sleep_time - compute_time))

    def eval_sequence(self, time: datetime) -> None:
        for u in self.underlyings:
            u.build_feature_vector(time)
            # if self.model.eval(u.features) and u.t1 <= time <= u.t2:
            if True and utils.get_now() > datetime(2021, 11, 26, 13, 45):
                print('about to buy')
                exiting_positions = self.ib.positions(account=self.account_num)
                try:
                    trade = transact.buy(self, u, time, exiting_positions)
                except ValidationError as e:
                    self._logger.exception(e)
                    trade = False
                if trade:
                    print('trade made')  # DAT
                    self.shutdown()  # DAT
                    self.launch_monitor(u, time, exiting_positions)
                    self.account.refresh_account()  # refresh cash, etc.
                self.db.log_buy_signal(u.dbid, time)
            self.db.log_underlying_data(u.dbid, u.data_line, time)
            self.db.log_option_data(u.straddle_options, time)

    def launch_monitor(self, underlying: object,
                       time: datetime, exiting_positions: list) -> None:
        """spin up a thread to monitor the position that was just opened"""
        t = threading.Thread(
            target=monitor.monitor,
            args=(self, underlying, time, exiting_positions),
            daemon=True)
        t.start()

    def wait_for_market_open(self) -> None:
        """sleep until the first exchange opens. No buffer needed since
           contracts should already be loaded and ready to be tracked."""
        now = utils.get_now(tz=self.tz)
        has_run = False
        while now < self.first_open_time:
            time_to_open = (self.first_open_time - now).total_seconds()
            if not has_run:
                has_run = True
                self._logger.info(
                    f'waiting for market open. {time_to_open} seconds to go')
            # self.ib.sleep(time_to_open / 2)  # ensure you don't oversleep
            self.ib.sleep(max(0, time_to_open - 15))  # ensure no oversleep
            now = utils.get_now(tz=self.tz)

    def check_for_positions(self) -> None:
        """Ensure that no positions that were opened during this session
           are still open. This is critical in the event an uncaught error
           shuts the program down during exeuction, this will prevent holding
           too long."""
        positions = self.db.get_all_options(self.session_start_time)
        for row in positions:
            if row['quantity'] != 0:
                self._logger.warning(f'OPEN POSITION FOUND!! {tuple(row)}')
                transact.close_position(self, row)  # build this out

    def main(self) -> None:
        self.wait_for_market_open()
        self.run_algo_loop()
        self.shutdown()

    def run(self) -> None:
        try:
            self.main()
        except Exception as e:
            self._logger.exception(e)
            self.shutdown()

    def shutdown(self, preliminary: bool = False) -> None:
        if preliminary:
            # no need to shutdown underlyings and API client
            # as they were never spun up in the first place.
            utils.exit()
        if self.db:
            self.check_for_positions()  # don't shutdown with live positions
            self.db.close()
        for underlying in self.underlyings + self.untracked:
            # cancel mkt data lines for each underlying, if possible.
            underlying.shutdown()
        utils.stop(self.ibc, self.watchdog)


if __name__ == '__main__':
    acc_num = input('Enter the account number: ')
    port = input('Enter the port to run on: ') or 4002
    App(account_num=acc_num, port=port).run()
