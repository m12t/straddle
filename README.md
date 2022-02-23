The straddle algo is a long-only nearest-tenor option buying algorithm that seeks to buy volatility. It is for educational purposes only, it is not suggested that you run this program, much less deploy it live. It will almost surely lose you money.

components:
1. a model for triggering a buy signal based on specific parameters. Feel free to plug your own ML or non-ML model in and tweak the feature vector as needed.
1. a database to log historical ATM options and underlying data every 0.25s for use by the program as well as future model training, as well as tracking open positions, trades, and buy signals.

Functionality:
1. tracking of underlying price and straddle options data
1. model input and handling
1. order execution and handling
1. concurrent position tracking (via multithreading)
1. database logging of price and trade data
1. automated program launch and shutdown (via chron/launchd)
1. interact with interactive brokers API using [IB-insync](https://github.com/erdewit/ib_insync)
1. automated API connection using [IBC](https://github.com/IbcAlpha/IBC)
