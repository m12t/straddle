"""Command line script to insert a new Underlying into algo DB using input
   validated by data automatically validated by IB."""
import sys

from db import DB

from ib_insync import IB, Stock, Index
import pandas_market_calendars as mcal


exchange_list = set(mcal.calendar_registry.get_calendar_names())
print('valid pandas market calendars exchanges', exchange_list)


def print_for_confirmation(args: dict) -> None:
    """neatly print the arg dictionary by printing the
       key, value, and value type evenly spaced."""
    longest_argname = max([len(str(arg)) for arg, _ in args.items()]) + 4
    longest_argvalue = max(
        [len(str(argvalue)) for _, argvalue in args.items()]) + 4
    for argname, arg in args.items():
        space1 = f'{" "*(longest_argname - len(argname))}'
        space2 = f'{" "*(longest_argvalue - len(str(arg)))}'
        print(f"{argname}{space1}{arg}{space2}{type(arg)}")


def validate_contract_data(contract, args):
    use_exch = int(input('Use returned exchange of '
                         f'`{contract.exchange}`? [1 (yes), 0 (no)]: '))
    if use_exch:
        args['exchange'] = contract.exchange
    use_pe = int(input('Use returned primary exchange of '
                       f'`{contract.primaryExchange}`? [1 (yes), 0 (no)]: '))
    if use_pe:
        args['primary_exchange'] = contract.primaryExchange
    use_pe_as_e = int(input(
        f'Use returned primary exchange of `{contract.primaryExchange}`'
        ' as exchange? [1 (yes), 0 (no)]: '))
    if use_pe_as_e:
        args['exchange'] = contract.primaryExchange
    assert contract.symbol == args['symbol']
    assert contract.currency == args['currency']
    assert contract.conId != 0
    args['conid'] = contract.conId


def main() -> None:
    args = get_inputs()
    port = int(input('Enter a live IB port: '))
    client_id = int(input('Enter a clientId for IB connection: '))
    ib.connect('127.0.0.1', port, client_id)
    ib.sleep(1)
    if not ib.isConnected() or not ib.client.isReady():
        raise ConnectionError('FATAL: Failed to connect.')
    if args['sec_type'] == 0:
        args['sec_type'] = 'STK'
        contract = Stock(
            symbol=args['symbol'],
            exchange='SMART',
            currency=args['currency'])
    elif args['sec_type'] == 1:
        args['sec_type'] = 'IND'
        contract = Index(
            symbol=args['symbol'],
            exchange=args['exchange'],
            currency=args['currency'])
    else:
        raise ValueError('Unexpected sec_type encountered.')
    ib.sleep(1)
    ib.qualifyContracts(contract)
    ib.sleep(5)
    validate_contract_data(contract, args)
    # translate input into valid strings
    if args['opt_style'] == 0:
        args['opt_style'] = 'AMERICAN'
    elif args['opt_style'] == 1:
        args['opt_style'] = 'EUROPEAN'
    else:
        raise ValueError('Unexpected opt_style encountered.')
    if args['opt_settlement'] == 0:
        args['opt_settlement'] = 'PHYSICAL'
    elif args['opt_settlement'] == 1:
        args['opt_settlement'] = 'CASH'
    else:
        raise ValueError('Unexpected opt_settlement encountered.')
    print_for_confirmation(args)
    proceed = input('Confirm that all the above are correct [y/n]: ').lower()
    if proceed == 'y':
        if args['exchange'] not in exchange_list:
            """corroborate entered exchange and ensure it's in
            pandas_market_calendars otherwise need to add an
            if statement to utils.get_schedule() such as
            is the case for `CBOE` -> `CBOE_Index_Options`."""
            print(f"{'-'*80}")
            print(f"WARNING: {args['exchange']} is not in exchange_list!")
            print(f"{'-'*80}")
            msg = ("Modification must be made to utils.get_schedule() "
                   "to map the exchange to the corresponding value expected "
                   "by pandas_market_calendars. "
                   "eg. `CBOE` -> `CBOE_Index_Options`")
            print(msg)
    # print values and ask for verification of values before logging to DB.
        db = DB(path='./alpha.db')  # will differ
        db.log_underlying(conid=args['conid'],
                          symbol=args['symbol'],
                          sec_type=args['sec_type'],
                          currency=args['currency'],
                          option_style=args['opt_style'],
                          option_settlement=args['opt_settlement'],
                          option_multiplier=args['opt_multiplier'],
                          option_trading_class=args['opt_trad_class'],
                          is_1256_contract=args['is_1256_contract'],
                          exchange=args['exchange'],
                          option_exchange=args['opt_exchange'],
                          primary_exchange=args['primary_exchange'])
        db.close()
    else:
        print('log aborted.')
    ib.disconnect()


def get_inputs() -> dict:
    print('----------REQUIRED INPUTS----------')
    symbol = str(input('Symbol: ')).upper()
    exchange = str(input('Exchange: ')).upper()
    primary_exchange = str(input('Primary exchange: ')).upper()
    sec_type = int(input('SecType [0 for `STK`, 1 for `IND`]: '))
    opt_style = int(input(
        'Option Style [0 for `AMERICAN` / 1 for `EUROPEAN`]: '))
    is_1256_contract = bool(input(
        'Options are 1256 contracts [0 for False / 1 for True]: '))
    opt_settlement = int(input(
        'Option Settlement [0 for `PHYSICAL` / 1 for `CASH`]: '))
    opt_multiplier = str(input('Option Multiplier (default `100`): ')) or '100'
    currency = str(input('Currency: (default `USD`): ')).upper() or 'USD'
    print('----------OPTIONAL INPUTS----------')
    opt_trad_class = str(input('Option Trading Class: ')).upper() or None
    opt_exchange = str(input('Option exchange: ')).upper() or None

    data = {
        'symbol': symbol,
        'exchange': exchange,
        'primary_exchange': primary_exchange,
        'sec_type': sec_type,
        'opt_style': opt_style,
        'is_1256_contract': is_1256_contract,
        'opt_settlement': opt_settlement,
        'opt_multiplier': opt_multiplier,
        'currency': currency,
        'opt_trad_class': opt_trad_class,
        'opt_exchange': opt_exchange
    }
    return data


if __name__ == '__main__':
    ib = IB()
    main()
