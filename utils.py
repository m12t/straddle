import sys
import logging
from zoneinfo import ZoneInfo
from datetime import datetime

import pandas_market_calendars as mcal
from ib_insync.ibcontroller import IBC, Watchdog

_logger = logging.getLogger(__name__)


def clean_exchange(exchange: str) -> str:
    """Refactor exchange inputs to match what mcal expects"""
    if exchange == 'CBOE':
        exchange = 'CBOE_Index_Options'
    return exchange


def get_schedule(exchange: str,
                 tz: ZoneInfo = ZoneInfo('America/New_York'),
                 end: datetime or str = None) -> tuple[datetime]:
    """Return the market open and close times in EST of the given exchange,
       whose tz may differ, as is the case of CBOE given natively in CST.
       This function is used for both the open and close times for today,
       as well as the close time on day of expiration for options passed
       into this function as `expiration` which is used to calculate tenor."""
    start = datetime.now(tz=tz).date()
    if end is None:
        # no expiration was passed in, use today for both start and end date
        end = start
    exchange = clean_exchange(exchange)
    try:
        schedule = mcal.get_calendar(exchange).schedule(
            start_date=start, end_date=end, tz=str(tz))
    except ValueError as e:
        _logger.exception(e)
        return None, None
    if schedule.empty:
        _logger.critical('Invalid schedule encountered.')
        o, c = None, None
    else:
        o = schedule['market_open'][0].to_pydatetime().replace(tzinfo=None)
        c = schedule['market_close'][-1].to_pydatetime().replace(tzinfo=None)
    return o, c


def get_now(tz: ZoneInfo = ZoneInfo('America/New_York')):
    return datetime.now(tz=tz).replace(tzinfo=None)


# def get_controller(v: int, gateway: bool, mode: str = 'paper') -> IBC:
def get_controller() -> IBC:
    return IBC(twsVersion=981, gateway=True, tradingMode='paper')


def get_watchdog(ib, ibc, port) -> Watchdog:
    return Watchdog(ibc, ib, port=port)


def start(ib: object, watchdog: object, timeout: int = 120) -> None:
    _logger.debug('Running utils.start')
    watchdog.start()
    i = 0
    while not(ib.isConnected() and ib.client.isReady()) and i < timeout:
        i += 1
        ib.sleep(1)


def stop(ibc: object, watchdog: object) -> None:
    watchdog.stop()  # watchdog calls ib.disconnect()
    ibc.terminate()  # kill the running IBGateway/TWS API client
    exit()           # halt execution of the program


def exit() -> None:
    sys.exit()
