"""module for buy and sell logic. perform pre-validation and execution"""

from exceptions.exceptions import ValidationError, OrderError

import validate
import bsm

from ib_insync import LimitOrder, Option


def buy(app: object, underlying: object, time,
        exiting_positions: list[object]) -> bool:
    """Main entry point into the sequence of opening a long straddle"""
    options = validate.validate_buy(app, underlying, time)
    quantity = get_quantity(options, app.account)
    call_quantity, put_quantity = 0, 0
    # TODO: make sure puts execute before calls. Currently call fill first.
    #       do this in validate_buy.
    for option in options:
        if not quantity:
            break
        order = build_order('BUY', quantity, option, tif='DAY')
        filled = execute(app, option.contract, order)
        if option.contract.right in {'C', 'CALL'}:
            call_quantity += filled
        else:
            put_quantity += filled
        quantity = filled  # set the next options quantity to what was filled
    audit(app, exiting_positions, underlying,
          time, options, call_quantity, put_quantity)


def get_quantity(options: list[object], account: object) -> int:
    """get the order quantity depending on ask_size and funds available."""
    max_deployable = account.available_funds * 0.25
    min_ask_size = min([o.askSize for o in options])
    position_price = sum([o.ask for o in options])
    if min_ask_size * position_price >= max_deployable:
        quantity = max_deployable / position_price
    else:
        quantity = min_ask_size
    return int(quantity)


def build_order(action: str, quantity: int, option: object,
                tif: str = 'IOC') -> list:
    """build ib-insync LimitOrder objects populated with inputted
       values, defaulting to time-in-force of immediate-or-cancel"""
    if action.upper() not in {'BUY', 'SELL'}:
        raise ValidationError('Invalid action. Must be `BUY` or `SELL`')
    if tif.upper() not in {'IOC', 'FOK', 'DTC', 'DAY'}:
        raise ValidationError(
            'Invalid time-in-force. Must be in {`IOC`, `DTC`, `FOK`, `DAY`}')
    if action == 'BUY':
        price = option.locked_ask
    else:
        # TODO: fix this for abort_trade to not use locked_bid but instead a custom price...
        price = option.locked_bid
    return LimitOrder(
        action,
        quantity,
        price,
        sweepToFill=True,
        tif=tif,
        outsideRth=False)


def execute(app: object, contract: object, order: object) -> int:
    unfilled = order.totalQuantity
    failed_trades = 0
    total_filled = 0
    while unfilled and failed_trades < 12:
        order.totalQuantity = unfilled
        trade = app.ib.placeOrder(contract, order)
        try:
            unfilled = handle_order(app.ib, trade)
        except OrderError:
            failed_trades += 1
            continue
        if trade.filled():
            total_filled += trade.filled()
            app.db.log_trade(trade, app.account_num)
        else:
            failed_trades += 1
    return total_filled


def handle_order(ib: object, trade: object) -> int:
    # should db logs be done here?? probably...
    terminal_states = {'Cancelled', 'ApiCancelled', 'Filled'}
    while trade.orderStatus.status not in terminal_states:
        if trade.orderStatus.status == 'Inactive':
            if trade.filled() > 0:
                break
            else:
                raise OrderError
        ib.sleep(0.01)  # wait on trade to fill or be cancelled.
        # NOTE: above sleep allows the ask to be
        #       updates and therefore potentially invalid. Need to lock prices.
    return trade.remaining()


def audit(app: object, exiting_positions: list[object], underlying: object,
          time, options: list[object], calls: int, puts: int) -> None:
    """Ensure that both straddle legs have the same position size,
       else call balance_positions() to balance them out."""
    if calls != puts:
        quantity = abs(calls - puts)  # disparity between calls and puts
        right = 'PUT' if puts < calls else 'CALL'
        quantity, _ = balance_position(app, options, 'BUY', right, quantity)
        if quantity:
            # means that balance_position failed.
            # abort the trade as a last resort.
            abort_trade(app, exiting_positions, underlying, time)


def balance_position(app: object, options: list[object],
                     action: str, right: str, quantity: int,
                     depth: int = 0, alive: bool = True) -> tuple[int, bool]:
    """Use recursion to balance out positions by alternating
       between buying the lesser quantity option and selling
       the greater quantity option. Execution ends when either
       max recursion depth is reached, or quantities are balanced."""
    rights = {'C', 'CALL'} if right == 'CALL' else {'P', 'PUT'}
    depth += 1
    if depth > 4:
        # limit max recursion depth to 4
        alive = False
    if alive and quantity:
        try:
            option = [o for o in options if o.contract.right in rights][0]
        except (IndexError, TypeError) as e:
            app._logger.exception(e)
            alive = False
            return quantity, alive
        order = build_order(action, quantity, option, tif='IOC')
        filled = execute(app, option.contract, order)
        if filled:
            if filled < quantity:
                quantity -= filled
                quantity, alive = balance_position(
                    app, options, right, quantity, depth, alive)
        else:
            # no fills occured, instead of trying to buying
            # the lesser right again, sell the greater right.
            right = 'PUT' if right == 'CALL' else 'CALL'  # opposite right
            action = 'SELL' if action == 'BUY' else 'BUY'  # opposite action
            quantity, alive = balance_position(
                app, options, action, right, quantity, depth, alive)
    return quantity, alive


def abort_trade(app: object, existing_positions: list[object],
                underlying, time) -> None:
    """positions have been unsuccessful in filling, abort the trade and close
       all associated position"""
    positions = validate.get_ib_positions(app, existing_positions)
    for position in positions:
        quantity = position.position // 100  # is // 100 needed??
        option = position.contract
        price = calc_abort_price(app, position, underlying)
        order = LimitOrder(
            'SELL',
            quantity,
            price,
            sweepToFill=True,
            tif='DAY',
            outsideRth=False)
        app.ib.placeOrder(position.contract, order)
    app._logger.info('aborted trade')


def calc_abort_price(app: object, position: object,
                     underlying: object) -> float:
    avg_cost = position.avgCost
    # midpoint of bid-ask spread?
    bsm_price = bsm.price_option(app.db, option, underlying)
    pass


def sell(ib: object, db: object, tz: object, underlying: object,
         account: object, positions: list[object]) -> None:
    """Entry point to liquidation of a straddle position. Iterate
       through positions, validate each position, then place orders
       until the position is liquidated."""
    for position in positions:
        validate.validate_sell(db, position, underlying, tz)
        # TODO: modify position.sell_price to be option.locked_bid
        #       option to have locked_bid in the same was as BUY's locked_ask
        order = build_order('SELL', position.quantity, position.sell_price)
        contract = position.contract or position.data_line.contract
        filled = execute(ib, db, account, contract, order)
        while filled < position.quantity:
            # continue to execute until the position is closed.
            order.totalQuantity = position.quantity - filled
            filled += execute(ib, db, account, contract, order)


def parse_row(row: object) -> Option:
    """Parse the sqlite3.Row object and return an ib-insync Option object"""
    symbol = row['symbol']
    strike = row['strike']
    right = row['right']
    exchange = row['exchange']
    exp = row['expiration']
    multiplier = row['multiplier']
    return Option(symbol, exp, strike, right, exchange, multiplier, 'USD')


def find_option(app: object, row: object) -> Option:
    """try to find the option from an already initialized underlying"""
    pass


def close_position(app: object, row: object) -> None:
    # first try to grab the Option object from App, if that fails, then
    # and only then, create a new Option object, request data, load that.
    print('close position. build this out...')
    option = find_option(app, row)
    if option is None:
        option = parse_row(row)
    quantity = row['quantity']
    data = app.ib.reqMktData(option)
    while not data.marketPrice():
        app.ib.sleep(0.1)
