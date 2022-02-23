
class Model:
    def __init__(self) -> None:
        self.w1 = 0.0
        self.w2 = 0.05
        self.w3 = 0.25

    def eval(self, f: list) -> bool:
        # input feature vector:
        #   [vol_ma_gap, vol_gap, iv, real_vol_last, real_vol_ma, now]
        # nan values will cause result to be False, but not throw errors.
        return bool(
            (f[0] > self.w1) *
            (f[1] > self.w2) *
            (f[2] < self.w3) *
            (f[3] < f[4]))
