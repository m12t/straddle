"""Script to insert a new Underlying into algo DB via argparse"""
import sys
import argparse
from db import DB

"""
[add SPX]
       python add_underlying.py --symbol SPX --mode auto --secType IND --optionStyle EUROPEan --is1256Contract 1 --optionSettlement CASH --currency USD --optionMultiplier 100 --exchange CBOE --optionTradingClass SPXW --optionExchange CBOE
[add AAPL]
        python add_underlying.py --symbol AAPL --mode auto --secType STK --optionStyle american --is1256Contract 0 --optionSettlement physical --currency USD --optionMultiplier 100 --exchange NASDAQ
[add TSLA]
        python add_underlying.py --symbol TSLA --mode auto --secType STK --optionStyle american --is1256Contract 0 --optionSettlement physical --currency USD --optionMultiplier 100 --exchange NASDAQ
"""


valid_sectypes = set(['STK', 'IND'])
valid_option_styles = set(['EUROPEAN', 'AMERICAN'])
valid_option_settlements = set(['CASH', 'PHYSICAL'])


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


def clean_args(args: argparse.Namespace) -> dict:
    """clean the inputs (capitalize, empty strings -> None)
       and return a cleaned dictionary."""
    return {
        'mode': args.mode.upper(),
        'conid': args.conid,
        'symbol': args.symbol.upper(),
        'sec_type': args.secType.upper(),
        'currency': args.currency.upper(),
        'option_style': args.optionStyle.upper(),
        'option_settlement': args.optionSettlement.upper(),
        'option_multiplier': args.optionMultiplier,
        'option_trading_class': args.optionTradingClass.upper() or None,
        'is_1256_contract': bool(args.is1256Contract),
        'exchange': args.exchange.upper() or None,
        'option_exchange': args.optionExchange.upper() or None,
        'primary_exchange': args.primaryExchange.upper() or None,
    }


def main(args: argparse.Namespace) -> None:
    args = clean_args(args)

    assert args['sec_type'] in valid_sectypes
    assert args['option_style'] in valid_option_styles
    assert args['option_settlement'] in valid_option_settlements
    assert type(args['is_1256_contract']) == bool

    if args['mode'] == 'MANUAL':
        if not args['conid']:
            raise AttributeError('No conid supplied in manual mode.')
    elif args['mode'] == 'AUTO':
        from ib_insync import IB, Stock, Index
        ib = IB()
        port = int(input('Enter a live IB port: '))
        client_id = int(input('Enter a clientId for IB connection: '))
        ib.connect('127.0.0.1', port, client_id)
        ib.sleep(1)
        if not ib.isConnected() or not ib.client.isReady():
            raise ConnectionRefusedError('FATAL: Failed to connect.')
        if args['sec_type'] == 'STK':
            contract = Stock(
                symbol=args['symbol'],
                exchange='SMART',
                currency=args['currency']
            )
        else:
            # Index
            contract = Index(
                symbol=args['symbol'],
                exchange=args['exchange'],
                currency=args['currency']
            )
        ib.sleep(1)
        ib.qualifyContracts(contract)
        ib.sleep(5)
        args['conid'] = contract.conId
        assert args['conid'] != 0
        use_exch = int(input(f'Use returned exchange of '
                             f'`{contract.exchange}`? [1 (yes), 0 (no)]: '))
        if use_exch:
            args['exchange'] = contract.exchange
        else:
            args['exchange'] = args['exchange']
        use_pe = int(input(f'Use returned primary exchange of '
                           f'`{contract.primaryExchange}`? [1 (yes), 0 (no)]: '))
        if use_pe:
            args['primary_exchange'] = contract.primaryExchange
        else:
            args['primary_exchange'] = args['primary_exchange']
        assert contract.symbol == args['symbol']
        assert contract.currency == args['currency']
    else:
        raise ValueError('Invalid mode. Must be either:'
              ' `manual` or `auto` (case-insensitive)')
    # print values and ask for verification of values before logging to DB.
    del args['mode']  # `mode` is no longer needed and shouldn't be printed
    print_for_confirmation(args)
    proceed = input('Confirm that all the above are correct [y/n]: ').lower()
    if proceed == 'y':
        db = DB(path='./alpha.db')
        db.log_underlying(conid=args['conid'],
                          symbol=args['symbol'],
                          sec_type=args['sec_type'],
                          currency=args['currency'],
                          option_style=args['option_style'],
                          option_settlement=args['option_settlement'],
                          option_multiplier=args['option_multiplier'],
                          option_trading_class=args['option_trading_class'],
                          is_1256_contract=args['is_1256_contract'],
                          exchange=args['exchange'],
                          option_exchange=args['option_exchange'],
                          primary_exchange=args['primary_exchange'])
        db.close()
    else:
        print('log aborted.')
    ib.disconnect()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # required args:
    parser.add_argument('--mode', type=str, required=True,
                        help='manual or auto (use IB to get'
                             ' conid and other fields)')
    parser.add_argument('--symbol', type=str, required=True)
    parser.add_argument('--secType', type=str, required=True,
                        help='`IND` for index, `STK` for stock/ETF')
    parser.add_argument('--optionStyle', type=str, required=True,
                        help='`EUROPEAN` or `AMERICAN`')
    parser.add_argument('--is1256Contract', type=int, required=True,
                        help='[0 for False, 1 for True]'
                             ' is the contract a 1256 contract qualifying'
                             ' for 60:40 LT:ST capital gains tax rate?')
    parser.add_argument('--optionSettlement', type=str, required=True,
                        help='`CASH` or `PHYSICAL`')
    parser.add_argument('--currency', type=str, required=True)
    parser.add_argument('--optionMultiplier', type=str, required=True,
                        help='string option multiplier, eg: `100`')

    # optional args: (conid and optionMultiplier required if mode == manual)
    parser.add_argument('--conid', type=int)
    parser.add_argument('--optionTradingClass', type=str, default='',
                        help='eg. SPXW for SPX weeklys')
    parser.add_argument('--exchange', type=str, default='')  # disallow NULL
    parser.add_argument('--primaryExchange', type=str, default='')  # disallow NULL
    parser.add_argument('--optionExchange', type=str, default='')

    args = parser.parse_args()
    main(args)
