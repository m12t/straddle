from datetime import datetime
from db.db import DB
from zoneinfo import ZoneInfo

# used for testing db.get_positions() and db.get_position_size()
time = datetime(2011, 11, 4, 0, 0).replace(tzinfo=None)
db = DB(path='./db/alpha.db', tz=ZoneInfo("America/New_York"))  # DAT
pos = db.get_positions('AAPL', time)
print([tuple(p) for p in pos], type(pos))

"""------------------------------------------------------------------"""

underlying_data = db.get_all_underlyings()
print(len(underlying_data))
for row in underlying_data:
    print(tuple(row))
