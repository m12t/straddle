
class InitError(Exception):
    """Raised when initializing an Underlying fails and all further
       execution of Underlying init sequence should be stopped."""
    pass


class ValidationError(Exception):
    """Raised when the validation before a trade fails for whatever reason.
       Only ever raised before a trade is placed."""
    pass


class OrderError(Exception):
    """Raised when waiting for a placed order to
       fill and the order goes inactive"""
    pass
