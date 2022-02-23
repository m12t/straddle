
class Account:
    def __init__(self, app: object) -> None:
        self.app = app
        self.cash = 0.0
        self.cushion = 0.0
        self.buying_power = 0.0
        self.available_funds = 0.0
        self.excess_liquidity = 0.0
        self.total_cash_value = 0.0
        self.maintenance_margin = 0.0
        # self.refresh_account()
        # self.AVAILABLE_FUNDS_BB = self.available_funds  # used for PnL calc
        # self.CASH_BB = self.cash  # used for session PnL

    def refresh_account(self) -> None:
        # https://interactivebrokers.github.io/ \
        # tws-api/interfaceIBApi_1_1EWrapper \
        # .html#acd761f48771f61dd0fb9e9a7d88d4f04
        # for a complete list of tags.
        for v in self.app.ib.accountSummary(self.app.account_num):
            if v.tag == 'TotalCashValue':
                self.cash = float(v.value)
            elif v.tag == 'Cushion':
                self.cushion = float(v.value)
            elif v.tag == 'AvailableFunds':
                self.available_funds = float(v.value)
            elif v.tag == 'BuyingPower':
                self.buying_power = float(v.value)
            elif v.tag == 'MaintMarginReq':
                self.maintenance_margin = float(v.value)
            elif v.tag == 'ExcessLiquidity':
                self.excess_liquidity = float(v.value)
            elif v.tag == 'TotalCashValue':
                self.total_cash_value = float(v.value)
            else:
                continue
