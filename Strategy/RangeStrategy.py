from typing import List, Optional
from Strategy.BaseStrategy import CBaseStrategy
from Chan import CChan
from Common.CEnum import KL_TYPE, BSP_TYPE
from BuySellPoint.BS_Point import CBS_Point


class CRangeStrategy(CBaseStrategy):
    """
    区间套策略
    基于缠论区间套理论，在多个级别中确认买卖点信号以提高交易可靠性
    区间套的核心思想是：在高级别出现买卖点时，需要在低级别得到确认才能交易
    """

    def __init__(self, 
                 max_position: float = 1.0, 
                 stop_loss_rate: float = 0.05,
                 take_profit_rate: float = 0.15):
        """
        初始化策略
        :param max_position: 最大仓位比例
        :param stop_loss_rate: 止损比例
        :param take_profit_rate: 止盈比例
        """
        super().__init__()
        self.max_position = max_position
        self.stop_loss_rate = stop_loss_rate
        self.take_profit_rate = take_profit_rate
        
        # 缓存已处理的买卖点，避免重复处理
        self.bsp_cache: List[CBS_Point] = []
        
        # 记录区间套确认状态
        self.qjt_confirmed = {}  # 记录买卖点是否已通过区间套确认

    def on_bar(self, chan: CChan, lv: KL_TYPE) -> None:
        """
        每根K线回调函数
        :param chan: CChan实例
        :param lv: 当前级别
        """
        # 只在最高级别执行策略逻辑
        if lv != chan.lv_list[0]:
            return
            
        # 检查是否为多级别分析
        if len(chan.lv_list) < 2:
            print("警告: 区间套策略需要至少两个级别的数据")
            return

        # 获取高级别的最新买卖点（指定级别索引）
        bsp_list = chan.get_latest_bsp(0)  # 获取最高级别的买卖点
        if not bsp_list:
            # 即使没有新的买卖点，也要检查止盈止损
            self._check_take_profit_and_stop_loss(chan, lv)
            return

        # 获取最后一个买卖点
        last_bsp = bsp_list[0]
        
        # 检查是否已处理过该买卖点
        if last_bsp in self.bsp_cache:
            # 检查止盈止损
            self._check_take_profit_and_stop_loss(chan, lv)
            return

        # 获取当前级别的chan数据
        cur_lv_chan = chan[lv]
        if len(cur_lv_chan) < 1:
            return

        current_time = cur_lv_chan[-1][-1].time
        current_price = cur_lv_chan[-1][-1].close

        # 检查区间套确认
        if self._check_range_confirmation(chan, last_bsp):
            # 根据买卖点类型执行交易
            if self._process_buy_signals(last_bsp, current_time, current_price):
                self.bsp_cache.append(last_bsp)
            elif self._process_sell_signals(last_bsp, current_time, current_price):
                self.bsp_cache.append(last_bsp)
            else:
                # 检查止盈止损
                self._check_take_profit_and_stop_loss(chan, lv)
        else:
            # 检查止盈止损
            self._check_take_profit_and_stop_loss(chan, lv)

    def _check_range_confirmation(self, chan: CChan, bsp: CBS_Point) -> bool:
        """
        检查区间套确认
        在高级别出现买卖点时，需要在低级别得到确认才能交易
        :param chan: CChan实例
        :param bsp: 高级别买卖点
        :return: 是否通过区间套确认
        """
        # 检查是否为多级别分析
        if len(chan.lv_list) < 2:
            return True  # 单级别分析直接返回True
            
        # 获取高级别和次级别
        high_lv = chan.lv_list[0]   # 高级别
        low_lv = chan.lv_list[1]    # 次级别
        
        high_lv_chan = chan[high_lv]
        low_lv_chan = chan[low_lv]
        
        if len(high_lv_chan) < 1 or len(low_lv_chan) < 1:
            return False
            
        # 检查高级别的买卖点是否在次级别有对应的一类买卖点确认
        high_bsp_klu = bsp.klu
        
        # 查找次级别中与高级别买卖点K线对应的K线段
        for low_bsp in low_lv_chan.bs_point_lst.bsp_iter():
            # 检查次级别的买卖点是否是1类买卖点
            if BSP_TYPE.T1 not in low_bsp.type and BSP_TYPE.T1P not in low_bsp.type:
                continue
                
            # 检查次级别的买卖点是否在高级别买卖点的K线范围内
            if (low_bsp.klu.sup_kl is not None and 
                low_bsp.klu.sup_kl.idx == high_bsp_klu.idx):
                # 找到区间套确认
                return True
                
        return False

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

        # 只有通过区间套确认的买卖点才考虑买入
        if not self.is_hold:
            self.buy(current_price, self.max_position, current_time, "区间套买点")
            print(f'{current_time}: 区间套买点买入，价格 = {current_price}')
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

        # 如果持有仓位，则考虑卖出
        if self.is_hold:
            self.sell(current_price, self.position, current_time, "区间套卖点")
            # 修复除零错误
            if self.last_buy_price and self.last_buy_price != 0:
                profit_rate = (current_price - self.last_buy_price) / self.last_buy_price * 100
                print(f'{current_time}: 区间套卖点平仓，价格 = {current_price}, 收益率 = {profit_rate:.2f}%')
            else:
                print(f'{current_time}: 区间套卖点平仓，价格 = {current_price}')
            return True

        return False

    def _check_take_profit_and_stop_loss(self, chan: CChan, lv: KL_TYPE):
        """
        检查止盈和止损条件
        :param chan: CChan实例
        :param lv: 当前级别
        """
        if not self.is_hold or not self.last_buy_price:
            return

        # 防止除零错误
        if self.last_buy_price == 0:
            return

        # 获取当前价格
        cur_lv_chan = chan[lv]
        if len(cur_lv_chan) < 1:
            return
            
        current_time = cur_lv_chan[-1][-1].time
        current_price = cur_lv_chan[-1][-1].close

        # 计算收益率
        if self.last_buy_price != 0:
            profit_rate = (current_price - self.last_buy_price) / self.last_buy_price

            # 止盈
            if profit_rate >= self.take_profit_rate:
                self.sell(current_price, self.position, current_time, "止盈卖出")
                print(f'{current_time}: 止盈卖出，价格 = {current_price}, 收益率 = {profit_rate*100:.2f}%')
                return

            # 止损
            if profit_rate <= -self.stop_loss_rate:
                self.sell(current_price, self.position, current_time, "止损卖出")
                print(f'{current_time}: 止损卖出，价格 = {current_price}, 亏损率 = {profit_rate*100:.2f}%')
                return