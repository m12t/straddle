import logging


class Position:
    def __init__(self, ib: object, symbol: str,
                 db_position: object = None, ib_position: object = None):
        self._logger = logging.getLogger(__name__)
        self.symbol: str = symbol
        if not db_position and not ib_position:
            msg = f'Need one of: {db_position, ib_position}.'
            # or... catch this in the calling function and log from there?
            self._logger.error(msg)
            raise ValueError(msg)
        if db_position:
            self.quantity: int = db_position['quantity']
            self.avg_price: float = db_position['avg_price']
            self.contract: object = self._build_contract(db_position)
        else:
            self.quantity: int = ib_position.position // 100  # test this...
            self.avg_price: float = ib_position.avgCost // 100
            self.contract: object = ib_position.contract
        self.data_line: object = ib.reqMktData(self.contract)
        self.bsm_price: float = 0.0

    def _build_contract(self, row) -> object:
        from ib_insync import Option
        return Option(
            symbol=self.symbol,
            lastTradeDateOrContractMonth=row['expiration'],
            tradingClass=row['trading_class'],
            strike=row['strike'],
            right=row['right'],
            exchange=row['exchange'] or '',
            multiplier=row['multiplier'])


def build_positions(ib: object, symbol: str,
                    ib_positions: list, db_positions: list) -> tuple[object]:
    """return Position class instances using the given DB and IB data"""
    valid_ib = []  # list of Position objs created with validated ib data
    valid_db = []  # list of Position objs created with validated db data
    for position in ib_positions:
        if position.contract.symbol == symbol:
            # only deal with positions for {symbol}
            valid_ib.append(Position(ib, symbol, ib_position=position))
    for position in db_positions:
        # this is already filtered by symbol from the db
        valid_db.append(Position(ib, symbol, db_position=position))
    return valid_ib, valid_db
