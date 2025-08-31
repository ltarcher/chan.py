from typing import List, Optional
from Strategy.BaseStrategy import CBaseStrategy
from Chan import CChan
from Common.CEnum import KL_TYPE, BSP_TYPE
from BuySellPoint.BS_Point import CBS_Point


class CChanBspStrategy(CBaseStrategy):
    """
    基于缠论买卖点的策略
    根据一买、二买、三买点进行交易
    """

    def __init__(self, max_position: float = 1.0, stop_loss_rate: float = 0.05):
        """
        初始化策略
        :param max_position: 最大仓位比例
        :param stop_loss_rate: 止损比例
        """
        super().__init__()
        self.max_position = max_position
        self.stop_loss_rate = stop_loss_rate
        self.bsp_cache: List[CBS_Point] = []  # 缓存已处理的买卖点
        self.buy_prices = {}  # 记录不同买点类型的买入价格

    def on_bar(self, chan: CChan, lv: KL_TYPE) -> None:
        """
        每根K线回调函数
        :param chan: CChan实例
        :param lv: 当前级别
        """
        # 获取最新的买卖点
        bsp_list = chan.get_latest_bsp()
        if not bsp_list:
            return

        # 获取最后一个买卖点
        last_bsp = bsp_list[0]
        
        # 检查是否已处理过该买卖点
        if last_bsp in self.bsp_cache:
            return

        # 获取当前级别的chan数据
        cur_lv_chan = chan[lv]
        if len(cur_lv_chan) < 1:
            return

        current_time = cur_lv_chan[-1][-1].time
        current_price = cur_lv_chan[-1][-1].close

        # 根据买卖点类型执行交易
        if self._process_buy_signals(last_bsp, current_time, current_price):
            self.bsp_cache.append(last_bsp)
        elif self._process_sell_signals(last_bsp, current_time, current_price):
            self.bsp_cache.append(last_bsp)
        else:
            # 检查止损
            self._check_stop_loss(current_time, current_price)

    def _process_buy_signals(self, bsp: CBS_Point, current_time, current_price: float) -> bool:
        """
        处理买入信号
        :param bsp: 买卖点
        :param current_time: 当前时间
        :param current_price: 当前价格
        :return: 是否处理了买入信号
        """
        if not bsp.is_buy:
            return False

        # 一买点
        if BSP_TYPE.T1 in bsp.type and not self.is_hold:
            self.buy(current_price, self.max_position, current_time, "一买点")
            self.buy_prices[BSP_TYPE.T1] = current_price
            print(f'{current_time}: 一买点买入，价格 = {current_price}')
            return True

        # 二买点
        elif BSP_TYPE.T2 in bsp.type and not self.is_hold:
            self.buy(current_price, self.max_position, current_time, "二买点")
            self.buy_prices[BSP_TYPE.T2] = current_price
            print(f'{current_time}: 二买点买入，价格 = {current_price}')
            return True

        # 三买点
        elif (BSP_TYPE.T3A in bsp.type or BSP_TYPE.T3B in bsp.type) and not self.is_hold:
            self.buy(current_price, self.max_position, current_time, "三买点")
            self.buy_prices[BSP_TYPE.T3A if BSP_TYPE.T3A in bsp.type else BSP_TYPE.T3B] = current_price
            print(f'{current_time}: 三买点买入，价格 = {current_price}')
            return True

        return False

    def _process_sell_signals(self, bsp: CBS_Point, current_time, current_price: float) -> bool:
        """
        处理卖出信号
        :param bsp: 买卖点
        :param current_time: 当前时间
        :param current_price: 当前价格
        :return: 是否处理了卖出信号
        """
        if bsp.is_buy:  # 只处理卖点
            return False

        # 如果持有仓位，根据卖点平仓
        if self.is_hold:
            # 一卖点平仓
            if BSP_TYPE.T1 in bsp.type:
                self.sell(current_price, self.position, current_time, "一卖点平仓")
                # 修复除零错误
                if self.last_buy_price and self.last_buy_price != 0:
                    profit_rate = (current_price - self.last_buy_price) / self.last_buy_price * 100
                    print(f'{current_time}: 一卖点平仓，价格 = {current_price}, 收益率 = {profit_rate:.2f}%')
                else:
                    print(f'{current_time}: 一卖点平仓，价格 = {current_price}')
                return True

            # 二卖点平仓
            elif BSP_TYPE.T2 in bsp.type:
                self.sell(current_price, self.position, current_time, "二卖点平仓")
                # 修复除零错误
                if self.last_buy_price and self.last_buy_price != 0:
                    profit_rate = (current_price - self.last_buy_price) / self.last_buy_price * 100
                    print(f'{current_time}: 二卖点平仓，价格 = {current_price}, 收益率 = {profit_rate:.2f}%')
                else:
                    print(f'{current_time}: 二卖点平仓，价格 = {current_price}')
                return True

            # 三卖点平仓
            elif BSP_TYPE.T3A in bsp.type or BSP_TYPE.T3B in bsp.type:
                self.sell(current_price, self.position, current_time, "三卖点平仓")
                # 修复除零错误
                if self.last_buy_price and self.last_buy_price != 0:
                    profit_rate = (current_price - self.last_buy_price) / self.last_buy_price * 100
                    print(f'{current_time}: 三卖点平仓，价格 = {current_price}, 收益率 = {profit_rate:.2f}%')
                else:
                    print(f'{current_time}: 三卖点平仓，价格 = {current_price}')
                return True

        return False

    def _check_stop_loss(self, current_time, current_price: float):
        """
        检查止损条件
        :param current_time: 当前时间
        :param current_price: 当前价格
        """
        if not self.is_hold or not self.last_buy_price:
            return

        # 防止除零错误
        if self.last_buy_price == 0:
            return

        # 计算当前收益率
        loss_rate = (current_price - self.last_buy_price) / self.last_buy_price
        
        # 如果亏损超过止损比例，则止损卖出
        if loss_rate <= -self.stop_loss_rate:
            self.sell(current_price, self.position, current_time, "止损卖出")
            print(f'{current_time}: 止损卖出，价格 = {current_price}, 亏损率 = {loss_rate*100:.2f}%')