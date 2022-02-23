"""module containing logic to validate data for various functions
   throughout the algorithm, particularly transact and monitor."""

import math
from operator import attrgetter

import bsm
from objects.position import build_positions
from exceptions.exceptions import ValidationError


""" ------------------------ BEGIN UNIVERSAL LOGIC ------------------------ """


def validate_spot(underlying: object) -> float:
    try:
        spot = underlying.data_line.last
        assert isinstance(spot, float)
        assert spot > 0
    except (AttributeError, AssertionError):
        raise ValidationError('Failed to validate spot price.')


def validate_attributes(options: list[object],
                        underlying: object, action: str) -> None:
    """check that all required attributes exist and are valid"""
    # uses architecture to try on each option, instead of one try for all.
    validate_spot(underlying)
    rights = {'C', 'CALL', 'P', 'PUT'}
    call_found, put_found = False, False
    for i, option in enumerate(options):
        try:
            # locked values are the trading prices used in orders.
            # locking them here prevents them from updating during
            # sleep while orders execute and placing an order at an
            # unvalidated and unfavorable price.
            option.locked_ask = float('nan')
            option.locked_bid = float('nan')
            option.bsm_price = float('nan')
            contract = option.contract
            conid = contract.conId
            symbol = contract.symbol
            exchange = contract.exchange
            strike = contract.strike
            right = contract.right
            expiration = contract.lastTradeDateOrContractMonth
            assert right in rights
            assert isinstance(conid, int)
            assert isinstance(symbol, str)
            assert isinstance(exchange, str)
            assert isinstance(strike, float)
            assert isinstance(right, str)
            assert isinstance(expiration, str)
            if action == 'BUY':
                ask = option.ask
                ask_size = option.askSize
                assert not math.isnan(ask)
                assert not math.isnan(ask_size)
                assert isinstance(ask, float)
                assert isinstance(ask_size, int)
                assert 0 < ask < 30  # assert bid isn't absurdly high
                assert ask_size > 0
                option.locked_ask = ask
            else:
                bid = option.bid
                bid_size = option.bidSize
                assert not math.isnan(ask)
                assert not math.isnan(ask_size)
                assert isinstance(bid, float)
                assert isinstance(bid_size, int)
                assert bid > 0
                assert bid_size > 0
                option.locked_bid = option.bid
            if right in {'C', 'CALL'}:
                call_found = True
            else:
                put_found = True
        except (AttributeError, AssertionError) as e:
            print(e)
            continue
    if not (call_found and put_found):
        raise ValidationError('No valid calls or no valids puts found')


"""     <><><><><><><   BEGIN POSITION VALIDATION LOGIC   ><><><><><><>     """
"""This block is used in both transact and monitor modules to most accurately
   decipher which positions were opened by the algorithm in a time period.
   The issue is that local DB should not be trusted entirely due to errors
   where trades are logged locally but rejected at IB and aren't really open.
   However, IB returns all positions for the account, with no timestamps. To
   try and solve this, a call is made to ib.positions() always before an algo
   transaction to get the most recent copy of open positions before a trade,
   then another call is made to ib.positions() immediately after a trade, and
   the difference between the two is assumed to be what was actually traded.
   There is a nonzero chance that another algo elsewhere could have opened
   or closed positions during the milliseconds this algo transacts, but the
   changes of this are minimal. The net difference from IB's records are then
   compared with local db data, favoring IB's records."""


def get_ib_positions(app: object, preexisting: list[object]) -> list[object]:
    """Returns ib-insync position objects."""
    ib_pos = app.ib.positions(account=app.account_num)
    return remove_preexisting(app.account_num, ib_pos, preexisting)


def remove_preexisting(account_num: str, ib_positions: list[object],
                       preexisting: list[object]) -> list:
    # used by transact and monitor
    """Returns ib-insync position objects.
       position data pulled from IB's records is imperfect for 2 reasons:
       1. it doesn't contain timestamps. this makes it extremely difficult
       to uncover which positions were opened by this algorithm and those
       that were preexisting.
       2. all positions for the same contract are aggregated into 1 position.
       this means that if another user or algorithm holds an options position
       of 10 contracts in and this algorithm buys 5 more contracts of the same
       security, the returned value will be 15, again making it difficult to
       solve for the postiion that was just opened.
       this function seeks to solve this issue by receiving list of positions
       created by a call to ib.positions() placed just before the BUY order(s)
       were placed, and another call to ib.positions() just after the BUY
       order(s). The two lists are then compared to find positions that were
       completely unchanged (and are ignored), and those that changed which
       are backsolved as best as possible (there is a non-zero chance that
       other external orders could have been filled in the microseconds between
       the two ib.positions() calls) for the positions that were actually added
       by this trading algorithm. Downstream filtering will compare local DB
       data with this validated ib data to further increase accuracy."""
    new_positions = []
    preexisting_contracts = set([p.contract for p in preexisting])
    for position in ib_positions:
        if position.contract not in preexisting_contracts:
            new_positions.append(position)
        else:
            existing_position = next(p for p in preexisting if
                                     p.contract == position.contract)
            if position.position == existing_position.position:
                # the position hasn't changed, it was placed before.
                # ignore it.
                continue
            else:
                # quantity has changed, create a new *ib-insync* position
                # object and populate it with the *newly added* quantity
                if position.position > existing_position.position:
                    from ib_insync.objects import Position as Pos
                    size = position.position - existing_position.position
                    avg_cost = extract_avg_cost(position, existing_position)
                    validated = Pos(
                        account_num, position.contract, size, avg_cost)
                else:
                    # position size decreased, assume that 100% of
                    # the remaining position belongs to this algo.
                    validated = position
                new_positions.append(validated)
    return new_positions


def extract_avg_cost(position: object, existing_position: object) -> float:
    # used by transact and monitor
    """return the avg_cost of the latest purchased lot.
       Do NOT divide by 100 to give a avg_cost per share of the contract
       since all other position objects are quoted in terms of 100 shares."""
    existing_avg = existing_position.avgCost
    existing_size = existing_position.position
    position_avg = position.avgCost
    position_size = position.position
    return position_avg * position_size - existing_avg * existing_size


def validate_positions(ib: object, symbol: str,
                       ib_positions: list[object],
                       db_positions: list[object]) -> list[object]:
    """ensure that the positions pulled from local DB match
       those in IBKR's DB. This is a further validation to solve for
       the positions that were opened by this algo. Returns straddle
       Position class instnaces, not ib-insync position NamedTuples."""
    return build_positions(ib, symbol, ib_positions, db_positions)[0]


"""     <><><><><><><   END POSITION VALIDATION LOGIC   ><><><><><><>     """


""" ------------------------ END UNIVERSAL LOGIC ------------------------ """


""" ------------------------ BEGIN BUY-SIDE LOGIC ------------------------ """


def optimize_pair(puts: list[object], calls: list[object]) -> list[object]:
    """Filter the 4 contract down to 2 contracts, one call and one put.
       Filter by lowest spread/gap from BSM price, otherwise by distance OTM"""
    return [optimal_option(puts), optimal_option(calls)]


def optimal_option(options: list[object]) -> object:
    """return the option with the smallest ask
       relative to its computed Black-Scholes value"""
    # NOTE: for future versions of the algo, this optimization
    #       is a prime candidate for improvement.
    return min(options, key=attrgetter('bsm_margin'))


def validate_buy(app: object, underlying: object, time) -> None:
    """returns the optimal pair of 1 call and
       1 put to open the straddle/strangle,
       with the put being first in the order."""
    check_for_position(app, underlying.symbol)  # DONE
    options = underlying.straddle_options + underlying.strangle_options
    validate_attributes(options, underlying, action='BUY')
    bsm.price_options(app.db, options, underlying, time)
    puts, calls = validate_ask_prices(options)
    return optimize_pair(puts, calls)


def check_for_position(app: object, symbol: str) -> None:
    """Ensure that no position exists for this
       underlying that was opened during this session"""
    if app.db.get_position_size(symbol, app.session_start_time):
        # don't allow multiple positions in the same underlying at once.
        raise ValidationError(f'Net position exists for {symbol}')


def validate_ask_prices(options: list[object]) -> tuple:
    """cross reference the ask prices with the output price
       from the Black-Scholes Model and assert the disparity < 20%"""
    valids = []
    for i, option in enumerate(options):  # `for option in options` AT
        try:
            print('right:', option.contract.right, 'strike:', option.contract.strike)
            print('\ti:', i, 'ask:', option.ask, 'bsm:', option.bsm_price)  # DAT
            bsm_price = option.bsm_price
            assert bsm_price > 0  # for ZeroDivisionError safety
            bsm_margin = (option.ask - bsm_price) / bsm_price
            assert bsm_margin < 0.20  # ask < 20% over bsm calculated price
            option.bsm_margin = bsm_margin
            valids.append(option)
        except (AssertionError, ZeroDivisionError) as e:
            print(e)  # DAT
            continue
    puts = [o for o in valids if o.contract.right in {'P', 'PUT'}]
    calls = [o for o in valids if o.contract.right in {'C', 'CALL'}]
    if not (puts and calls):
        raise ValidationError('One or both legs have no valid ask prices')
    return puts, calls


""" ------------------------  END BUY-SIDE LOGIC  ------------------------ """


""" -----------------------  BEGIN SELL-SIDE LOGIC  ----------------------- """


def validate_sell(db: object, position: object,
                  underlying: object, tz: object) -> None:
    """perform validation on the price of each option in which
       a position exists. Valdiate if the spread is within range,
       the bid is reasonable, etc. use bsm model to roughly gauge
       the validity of bid prices. Modify the position attribute
       with an acceptable sell price"""
    # TODO: move this to validate module
    position.bsm_price = bsm.price_option(
        db, position.data_line, underlying, tz)
    try:
        option = position.data_line
        spread = option.ask / option.bid
    except (ZeroDivisionError, AttributeError, TypeError):
        spread = 0.0
    if 0 < spread < 1.1:
        position.sell_price = option.bid
    else:
        position.sell_price = position.bsm_price
    if option.bid / position.bsm_price > 1.25:
        position.sell_price = position.bsm_price


""" -----------------------   END SELL-SIDE LOGIC   ----------------------- """
