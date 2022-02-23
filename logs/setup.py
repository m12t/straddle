import os
import logging


def config_logger(mode: str, now: object, filename: str) -> None:
    filename = get_filename(mode, now, filename)
    logging.basicConfig(
        filename=filename,
        encoding='utf-8',
        # level=logging.DEBUG if mode.lower() == 'testing' else logging.WARNING,
        level=logging.WARNING,
        format=('%(asctime)s:%(levelname)s:%(module)s:'
                '%(funcName)s:line %(lineno)d:%(message)s'),
        datefmt='%m/%d/%Y %I:%M:%S %p %Z')


def get_filename(mode: str, now: object, filename: str) -> str:
    """Sequentially check for parent directories and create them
       as needed. Logs are sorted by mode/year/month/day/logfile.log"""
    year = str(now.year)
    month = str(now.month)
    day = str(now.day)
    return get_path(['logs', mode, year, month, day]) + filename + '.log'


def get_path(directories: list[str]) -> str:
    path = ''
    for dir in directories:
        path += f'{dir.lower()}/'
        check_or_make(path)
    return path


def check_or_make(path: str) -> None:
    if not os.path.exists(path):
        os.mkdir(path)


if __name__ == '__main__':
    # DAT
    print(os.getcwd())
    # from datetime import datetime
    # logger = config_logger('TESTING', datetime.now().date(), 'straddle')
