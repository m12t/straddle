# from ..utils import get_schedule

# # for testing get_schedule
# expiration = '20211105'
# open, close = get_schedule('NYSE')
# # open, close = get_schedule('NYSE', date=datetime.now() - timedelta(days=1))
# # if open < datetime.strptime('20201010', '%Y%m%d'):
# # # throws a TypeError: '<' not supported between instances of 'NoneType' and 'datetime.datetime'
# if open and close:
#     print(open)
#     print(close)


"""this is the condition used in utils.start() timeout of api connection."""
def condition_test(args):
    a = args[0]
    b = args[1]
    i = args[2]
    correct = args[3]
    output = None
    if (not a or not b) and i < 120:
        output = True
        assert output == correct
    if not(a and b) and i < 120:
        output = True
        assert output == correct

def condition_tester():
    cases = [
        [False, False, 0, True],
        [False, False, 123, False],
        [True, False, 100, True],
        [True, False, 123, False],
        [False, True, 100, True],
        [False, True, 123, False],
        [True, True, 100, False],
        [True, True, 123, False]]


    failed = 0
    for i, case in enumerate(cases):
        try:
            condition_test(case)
        except:
            failed += 1
            print('failed case', i+1, case)

condition_tester()
"""this is the condition used in utils.start() timeout of api connection."""
