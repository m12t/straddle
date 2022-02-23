"""module containing logic to monitor a position once it has been opened.
   monitor() is the entry point, and is the target of a new
   thread spooled from algo.py once a buy transaction is executed.
   PnL is calculated repetitively until either a 50% return is achieved
   or the holding_period is hit, at which point a call is made to sell()
   and the trade is then logged to the db before the thread shuts down."""
import asyncio
import logging
from datetime import timedelta

import utils
import transact
import validate
from db.db import DB

_logger = logging.getLogger(__name__)


def monitor(app: object, underlying: object, db_time: object,
            preexiting_positions: list) -> None:
    asyncio.set_event_loop(asyncio.new_event_loop())
    symbol = underlying.symbol
    holding_period = underlying.holding_period
    db = DB(path='./db/alpha.db', tz=app.tz)  # new instance for thread safety
    sell_time = db_time + timedelta(minutes=holding_period)
    print('inside monitor')  # DAT
    # db_time -= timedelta(days=500)  # DAT
    # call IB server for list of open positions
    ib_pos = validate.get_ib_positions(app, preexiting_positions)
    db_pos = db.get_positions(symbol, db_time)
    positions = validate.validate_positions(app.ib, symbol, ib_pos, db_pos)
    if positions:
        # is this if statement necessary?
        run_thread_loop(
            app.ib, app.account, db, app.tz, underlying, positions, sell_time)
    # thread is automatically terminated at this stage.


def run_thread_loop(ib: object, account: object, db: object,
                    tz: object, underlying: object,
                    positions: list[object], sell_time: object) -> None:
    """main thread loop to monitor the pnl of the positions
       and trigger a sale after the holding period has elapsed
       or predetermined level of realizable (selling to bid)
       return is achieved."""
    print('in monitor run_thread_loop')  # DAT
    y1 = sum([p.avg_price * p.quantity for p in positions])
    while utils.get_now(tz=tz) < sell_time:
        try:
            if calc_pnl(positions, y1) > 0.50:
                # break loop to force a sale at > 50% return
                break
        except AssertionError:
            pass  # `pass` instead of `continue` to catch the sleep below
        except (AttributeError, TypeError) as e:
            _logger.exception(e)
        ib.sleep(0.1)
    transact.sell(ib, db, tz, underlying, account, positions)


def calc_pnl(positions: list[object], y1: float) -> float:
    """return the current liquidation return, as a % of opening cost"""
    y2 = 0.0
    for position in positions:
        bid = position.data_line.bid
        bid_size = position.data_line.bidSize
        assert bid_size >= position.quantity
        y2 += bid * bid_size
    return (y2 - y1) / y1
