"""dealing with database"""
import sqlite3

# alpha test, beta test, then production db
con = sqlite3.connect('./alpha.db')
con.row_factory = sqlite3.Row

# NOTE: primary key has an implicit NOT NULL constraint
# NOTE: Is1256Contract is a bool stored as INT: 1 for True, 0 for False
# NOTE: reason for having ID and ConID (which is unique)
#       is for performance reasons. ID is indexed, ConID is not.
create_underlying_table = (
    """CREATE TABLE Underlying (
        ID INTEGER PRIMARY KEY,
        ConID INTEGER NOT NULL UNIQUE,
        Symbol TEXT NOT NULL UNIQUE,
        SecType TEXT NOT NULL,
        Currency TEXT NOT NULL,
        OptionStyle TEXT NOT NULL,
        OptionSettlement TEXT NOT NULL,
        OptionMultiplier TEXT NOT NULL,
        OptionExchange TEXT,
        OptionTradingClass TEXT,
        Is1256Contract INTEGER NOT NULL,
        Exchange TEXT NOT NULL,
        PrimaryExchange TEXT)"""
)

create_option_table = (
    """CREATE TABLE Option (
        ID INTEGER PRIMARY KEY,
        ConID INTEGER NOT NULL UNIQUE,
        UnderlyingID INTEGER NOT NULL REFERENCES Underlying(ID),
        LastTradeDateOrContractMonth TEXT NOT NULL,
        Right TEXT NOT NULL,
        Strike REAL NOT NULL,
        Exchange TEXT,
        UNIQUE(UnderlyingID,
               LastTradeDateOrContractMonth,
               Right,
               Strike))"""
)

# see if IB returns an order refrerence number.
# if so, include it here. UNIQUE and NOT NULL
create_trade_table = (
    """CREATE TABLE Trade (
        ID INTEGER PRIMARY KEY,
        AccountNum TEXT NOT NULL,
        Time TEXT NOT NULL,
        OptionID INTEGER NOT NULL REFERENCES Option(ID),
        Quantity INTEGER NOT NULL,
        AvgPrice REAL NOT NULL,
        Commission REAL NOT NULL,
        UNIQUE(OptionID, Time))"""
)

create_underlying_data_table = (
    """CREATE TABLE UnderlyingData (
        ID INTEGER PRIMARY KEY,
        UnderlyingID INTEGER NOT NULL REFERENCES Underlying(ID),
        Time TEXT NOT NULL,
        Price REAL,
        UNIQUE(UnderlyingID, Time))"""
)

create_option_data_table = (
    """CREATE TABLE OptionData (
        ID INTEGER PRIMARY KEY,
        OptionID INTEGER NOT NULL REFERENCES Option(ID),
        Time TEXT NOT NULL,
        Ask REAL,
        Bid REAL,
        AskImpVol REAL,
        BidImpVol REAL,
        UNIQUE(OptionID, Time))"""
)

# * all other data involved with buy signal can be queried
#   from the DB using underlying_id and time as foreign keys
create_buy_signal_data = (
    """CREATE TABLE BuySignal (
        ID INTEGER PRIMARY KEY,
        UnderlyingID INTEGER NOT NULL REFERENCES Underlying(ID),
        Time TEXT NOT NULL,
        UNIQUE(UnderlyingID, Time))"""
)

tables = [
    create_underlying_table,
    create_option_table,
    create_trade_table,
    create_underlying_data_table,
    create_option_data_table,
    create_buy_signal_data,
]


def add_underlying():
    conid = '416904'
    symbol = 'SPX'
    sec_type = 'IND'
    currency = 'USD'
    option_style = 'EUROPEAN'
    option_settlement = 'CASH'
    option_multiplier = '100'
    option_trading_class = 'SPXW'
    is_1256_contract = True
    exchange = 'CBOE'
    primary_exchange = None

    with con:
        con.execute(
            """INSERT INTO Underlying(
                ConID, Symbol, SecType, Currency, OptionStyle,
                OptionSettlement, OptionMultiplier, OptionTradingClass,
                Is1256Contract, Exchange, PrimaryExchange)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (conid, symbol, sec_type, currency, option_style,
                option_settlement, option_multiplier, option_trading_class,
                is_1256_contract, exchange, primary_exchange))


# actually create the tables:
for i, table in enumerate(tables):
    print(f"creating table {i}")
    con.execute(table)
# add_underlying()
con.close()
print('please add underlyings via `add_underlying_input.py`'
      ' or `add_underlying_cli.py` before running algo.')
