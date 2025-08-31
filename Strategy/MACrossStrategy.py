from Strategy.BaseStrategy import CBaseStrategy
from Chan import CChan
from Common.CEnum import KL_TYPE
import numpy as np


class CMACrossStrategy(CBaseStrategy):
    """
    均线交叉策略
    当短期均线上穿长期均线时买入，下穿时卖出
    """

    def __init__(self, short_period: int = 5, long_period: int = 20):
        """
        初始化策略
        :param short_period: 短期均线周期
        :param long_period: 长期均线周期
        """
        super().__init__()
        self.short_period = short_period
        self.long_period = long_period
        self.last_short_ma = None
        self.last_long_ma = None
        self.current_short_ma = None
        self.current_long_ma = None

    def calculate_ma(self, closes, period):
        """
        计算简单移动平均线
        :param closes: 收盘价列表
        :param period: 周期
        :return: 移动平均值
        """
        if len(closes) < period:
            return None
        return np.mean(closes[-period:])

    def on_bar(self, chan: CChan, lv: KL_TYPE) -> None:
        """
        每根K线回调函数
        :param chan: CChan实例
        :param lv: 当前级别
        """
        # 获取当前级别的chan数据
        cur_lv_chan = chan[lv]
        
        # 确保我们有足够的K线数据
        if len(cur_lv_chan) < self.long_period:
            return

        # 获取收盘价列表
        closes = [klu.close for klu in cur_lv_chan.klu_iter()]
        
        # 计算均线
        self.last_short_ma = self.current_short_ma
        self.last_long_ma = self.current_long_ma
        self.current_short_ma = self.calculate_ma(closes, self.short_period)
        self.current_long_ma = self.calculate_ma(closes, self.long_period)
        
        # 确保均线值有效
        if (self.last_short_ma is None or self.last_long_ma is None or
                self.current_short_ma is None or self.current_long_ma is None):
            return

        current_time = cur_lv_chan[-1][-1].time
        current_price = cur_lv_chan[-1][-1].close

        # 短期均线上穿长期均线，买入信号
        if (self.last_short_ma <= self.last_long_ma and
                self.current_short_ma > self.current_long_ma and
                not self.is_hold):
            self.buy(current_price, 1, current_time, "MA Cross Buy")
            print(f'{current_time}: MA交叉买入价格 = {current_price}')

        # 短期均线下穿长期均线，卖出信号
        elif (self.last_short_ma >= self.last_long_ma and
              self.current_short_ma < self.current_long_ma and
              self.is_hold):
            self.sell(current_price, 1, current_time, "MA Cross Sell")
            # 修复类型错误
            if self.last_buy_price is not None and self.last_buy_price != 0:
                profit_rate = (current_price - self.last_buy_price) / self.last_buy_price * 100
                print(f'{current_time}: MA交叉卖出价格 = {current_price}, 收益率 = {profit_rate:.2f}%')
            else:
                print(f'{current_time}: MA交叉卖出价格 = {current_price}')