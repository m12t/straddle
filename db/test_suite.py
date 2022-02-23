import math
from datetime import datetime

from db import DB


"""------------ below is for testing querying ------------"""
"""the intent here is to ensure that invalid inputs to queries
   doesn't throw exceptions and instead just returns None"""


def test_get_spot(ids: list[int] = None):
    invalids = [-1, 0, 0.44, -0.22, 'a', 1222020, '-', '-2-2d']
    valids = [1]
    passed, failed = 0, 0
    for id in invalids:
        spot = db.get_spot(id)
        try:
            assert spot is None
            passed += 1
        except AssertionError:
            failed += 1
            print('failed with id of', id)
    for id in valids:
        spot = db.get_spot(id)
        try:
            assert isinstance(spot, float)
            passed += 1
        except AssertionError:
            failed += 1
            print('failed with id of', id)
    pass_rate = passed / (passed + failed)
    print(f'passed {pass_rate * 100}% of cases')


def test_get_sigma(ids: list[int] = None, lookbacks: list[int] = None):
    ids = [1, -1, 0, 0.44, -0.22, 1222020]
    lookbacks = [-1, 0, 0.44, -0.22, 1222020]
    passed, failed = 0, 0
    for id in ids:
        for lookback in lookbacks:
            try:
                a = db.get_sigma(id, lookback)
                print(a)
                passed += 1
            except AssertionError:
                failed += 1
            except Exception as e:
                failed += 1
                print(id, lookback, e)
    pass_rate = passed / (passed + failed)
    print(f'passed {pass_rate * 100}% of cases')


def test_get_price_extrema(ids: list[int] = None, lookbacks: list[int] = None):
    ids = [1, -1, 0, 0.44, -0.22, 1222020]
    lookbacks = [-1, 0, 0.44, -0.22, 1222020]
    passed, failed = 0, 0
    for id in ids:
        for lookback in lookbacks:
            try:
                a = db.get_price_extrema(id, lookback)
                print(a)
                passed += 1
            except AssertionError:
                failed += 1
            except Exception as e:
                failed += 1
                print(id, lookback, e)
    pass_rate = passed / (passed + failed)
    print(f'passed {pass_rate * 100}% of cases')


def test_get_underlying_id(symbols: list[str] = None):
    symbols = ['SPX', 'spx', 'sPx', '0', '-', '-2-2d', 'a', 0, 33231, -332, -3]
    passed, failed = 0, 0
    for symbol in symbols:
        try:
            c = db.get_underlying_id(symbol)
            # print(c)
            passed += 1
        except Exception as e:
            failed += 1
            print(e)
    pass_rate = passed / (passed + failed)
    print(f'passed {pass_rate * 100}% of cases')


def test_get_opt_id_from_conid(conids: list[int] = None):
    conids = [514502747, 'SPX', 'spx', 'sPx', '0',
              '-', '-2-2d', 'a', 0, 33231, -332, -3]
    passed, failed = 0, 0
    for conid in conids:
        try:
            b = db.get_option_id_from_conid(conid)
            print(b)
            passed += 1
        except AssertionError:
            failed += 1
            print('failed with conid', conid)
    pass_rate = passed / (passed + failed)
    print(f'passed {pass_rate * 100}% of cases')


def test_get_position_size(symbols: list[str] = None,
                           times: list[datetime] = None):
    symbols = ['PSX', 'SPX', 'sPx', '0', 0, 23, -2, -3.3, 3.44, 2032302, ',']
    times = [datetime.now(), '0332032', 'aa', '2342', '--']
    passed, failed = 0, 0
    for symbol in symbols:
        for time in times:
            try:
                s = db.get_position_size(symbol, time)
                # print(s)
                passed += 1
            except Exception as e:
                failed += 1
                print(e)
    pass_rate = passed / (passed + failed)
    print(f'passed {pass_rate * 100}% of cases')


def test_get_positions(symbols: list[str] = None, times: list[datetime] = None):
    symbols = ['PSX', 'SPX', 'sPx', '0', 0, 23, -2, -3.3, 3.44, 2032302, ',']
    times = [datetime.now(), '0332032', 'aa', '2342', '--']
    passed, failed = 0, 0
    for symbol in symbols:
        for time in times:
            try:
                s = db.get_positions(symbol, time)
                # print(s)
                passed += 1
            except Exception as e:
                failed += 1
                print(e)
    pass_rate = passed / (passed + failed)
    print(f'passed {pass_rate * 100}% of cases')

# function calls for the above tests:
# test_get_spot()
# test_get_sigma()
# test_get_underlying_id()
# test_get_opt_id_from_conid()
# test_get_position_size()
# test_get_positions()
# test_get_price_extrema()


def test_get_option_id():
    # opts = db.get_all_options()  # used to get some conids
    # for row in opts:
    #     print(tuple(row))
    #     option_conid 523228946 option_id None
    #     option_conid 523229578 option_id None
    #     option_conid 523228952 option_id None
    #     option_conid 523229585 option_id None
    opt = db.get_option_id_from_conid(523228946)
    assert opt is not None
    assert isinstance(opt, int)


def test_get_all_options():
    opts = db.get_all_options()
    for row in opts:
        print(tuple(row))


def test_get_all_trades():
    trades = db.get_all_trades()
    for row in trades:
        print(tuple(row))



"""------------ above is for testing querying ------------"""




if __name__ == '__main__':
    db = DB(path='./alpha.db')

    # call test function:
    test_get_sigma()

    db.close()
