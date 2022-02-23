"""module for rough options pricing via the Black-Scholes model.
This is used as a point of validation when placing trades to
gauge whether a bid or ask is reasonable, not as a primary source
of valuation to be traded on."""

import math
from datetime import datetime

import utils
from exceptions.exceptions import ValidationError

from scipy.stats import norm


def price_option(db: object, option: object, underlying: object,
                 tz: object, time: datetime = None) -> float:
    """price 1 option"""
    # is this used??
    strike = option.contract.strike
    right = option.contract.right
    tenor = calc_tenor(option, tz)
    spot = underlying.data_line.last
    r = 0.02  # this should be logged in the DB as the daily rf rate.
    # ^ that's if pulling t-bill rates from an API.
    if time is None:
        time = utils.get_now()
    sigma = db.get_sigma(underlying.dbid, time, lookback=15)
    # refactor db.get_underlying_price_extrema()
    if right in {'C', 'CALL'}:
        price = price_call(spot, strike, tenor, sigma, r)
    elif right in {'P', 'PUT'}:
        price = price_put(spot, strike, tenor, sigma, r)
    else:
        price = 0.0
    return price


def price_options(db: object, options: list[object],
                  underlying: object, time: datetime = None) -> None:
    """a more efficient function and architecture than calling
       price_option() repeatedly since the costly calculations
       to get sigma and tenor are only performed once."""
    if time is None:
        time = utils.get_now()
    sigma = db.get_sigma(underlying.dbid, time, lookback=15)
    tenor = calc_tenor(underlying)
    spot = underlying.data_line.last  # already validated and guaranteed
    r = 0.02  # app.db.get_tbill_rate() or rf_rate or whatever
    for option in options:
        strike = option.contract.strike
        right = option.contract.right
        if right in {'C', 'CALL'}:
            price = price_call(spot, strike, tenor, sigma, r)
        else:
            # `right` is already validated, so else is safe here.
            price = price_put(spot, strike, tenor, sigma, r)
        option.bsm_price = price


def calc_tenor(underlying) -> float:
    """pull the datetime expiration of underlying and calculate the delta
       from current time, using continuous time, normalized to years."""
    expiration = underlying.options_expiration
    tenor = (expiration - utils.get_now()).total_seconds()
    if tenor <= 0:
        raise ValidationError(f'Invalid tenor of `{tenor}`')
    return tenor / (252 * 24 * 60 * 60)


def price_call(s, k, t, sigma, r) -> float:
    # from Hull p. 335, fig. (15.20)
    d1 = get_d1(s, k, r, sigma, t)
    d2 = get_d2(d1, sigma, t)
    call = s * norm.cdf(d1) - k * math.exp(-r * t) * norm.cdf(d2)
    return max(0, call)


def price_put(s, k, t, sigma, r) -> float:
    # from Hull p. 335, fig (15.21)
    d1 = get_d1(s, k, r, sigma, t)
    d2 = get_d2(d1, sigma, t)
    put = k * math.exp(-r * t) * norm.cdf(-d2) - s * norm.cdf(-d1)
    return max(0, put)


def get_d1(spot, strike, r, sigma, t) -> float:
    # BUG: runtime warning from numpy division by zero.
    try:
        return ((math.log(spot / strike) + (r + sigma ** 2 / 2) * t) /
                (sigma * math.sqrt(t)))
    except Exception as e:
        print('divide by zero caught...')
        print(type(e), e)


def get_d2(d1, sigma, t) -> float:
    return d1 - sigma * math.sqrt(t)
