from typing import List, Dict, Optional
from Strategy.BaseStrategy import CBaseStrategy
from Chan import CChan
from Common.CEnum import KL_TYPE, BSP_TYPE
from BuySellPoint.BS_Point import CBS_Point


class CAdvancedChanBspStrategy(CBaseStrategy):
    """
    高级缠论买卖点策略
    支持更多参数控制和风险控制
    """

    def __init__(self, 
                 max_position: float = 1.0, 
                 stop_loss_rate: float = 0.05,
                 take_profit_rate: float = 0.15,
                 enable_types: List[str] = None,
                 position_per_bsp: Dict[str, float] = None):
        """
        初始化策略
        :param max_position: 最大仓位比例
        :param stop_loss_rate: 止损比例
        :param take_profit_rate: 止盈比例
        :param enable_types: 启用的买卖点类型，如['1', '2', '3a']
        :param position_per_bsp: 不同买卖点类型的仓位比例
        """
        super().__init__()
        self.max_position = max_position
        self.stop_loss_rate = stop_loss_rate
        self.take_profit_rate = take_profit_rate
        self.enable_types = enable_types or ['1', '2', '3a', '3b']
        self.position_per_bsp = position_per_bsp or {
            '1': 1.0,   # 一买满仓
            '2': 0.7,   # 二买70%仓位
            '3a': 0.5,  # 三买50%仓位
            '3b': 0.5   # 三买50%仓位
        }
        
        self.bsp_cache: List[CBS_Point] = []  # 缓存已处理的买卖点
        self.buy_prices: Dict[str, float] = {}  # 记录不同买点类型的买入价格
        self.hold_bsp_type: Optional[str] = None  # 当前持仓基于哪种买卖点

    def on_bar(self, chan: CChan, lv: KL_TYPE) -> None:
        """
        每根K线回调函数
        :param chan: CChan实例
        :param lv: 当前级别
        """
        # 获取最新的买卖点
        bsp_list = chan.get_latest_bsp()
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

        # 根据买卖点类型执行交易
        if self._process_buy_signals(last_bsp, current_time, current_price):
            self.bsp_cache.append(last_bsp)
        elif self._process_sell_signals(last_bsp, current_time, current_price):
            self.bsp_cache.append(last_bsp)
        else:
            # 检查止盈止损
            self._check_take_profit_and_stop_loss(chan, lv)

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

        # 确定买卖点类型
        bsp_type_key = None
        if BSP_TYPE.T1.value in self.enable_types and BSP_TYPE.T1 in bsp.type:
            bsp_type_key = '1'
        elif BSP_TYPE.T2.value in self.enable_types and BSP_TYPE.T2 in bsp.type:
            bsp_type_key = '2'
        elif BSP_TYPE.T3A.value in self.enable_types and BSP_TYPE.T3A in bsp.type:
            bsp_type_key = '3a'
        elif BSP_TYPE.T3B.value in self.enable_types and BSP_TYPE.T3B in bsp.type:
            bsp_type_key = '3b'

        # 如果是启用的买卖点类型且当前未持仓
        if bsp_type_key and not self.is_hold:
            position_ratio = self.position_per_bsp.get(bsp_type_key, self.max_position)
            position = position_ratio
            
            self.buy(current_price, position, current_time, f"{bsp_type_key}买点")
            self.buy_prices[bsp_type_key] = current_price
            self.hold_bsp_type = bsp_type_key
            print(f'{current_time}: {bsp_type_key}买点买入，价格 = {current_price}, 仓位 = {position*100:.0f}%')
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
        # 只有当持仓是基于买卖点买入时，才考虑对应的卖点
        if not self.is_hold or not self.hold_bsp_type:
            return False

        # 检查是否为对应买点的卖点
        expected_sell_type = None
        if self.hold_bsp_type == '1':
            expected_sell_type = BSP_TYPE.T1
        elif self.hold_bsp_type == '2':
            expected_sell_type = BSP_TYPE.T2
        elif self.hold_bsp_type in ['3a', '3b']:
            # 三买可以被一类或二类卖点平仓
            if BSP_TYPE.T1 in bsp.type or BSP_TYPE.T2 in bsp.type:
                expected_sell_type = bsp.type[0]  # 取第一个类型

        # 如果是预期的卖点类型
        if expected_sell_type and expected_sell_type in bsp.type:
            self.sell(current_price, self.position, current_time, f"{self.hold_bsp_type}对应卖点")
            # 修复除零错误
            if self.last_buy_price and self.last_buy_price != 0:
                profit_rate = (current_price - self.last_buy_price) / self.last_buy_price * 100
                print(f'{current_time}: {self.hold_bsp_type}对应卖点平仓，价格 = {current_price}, 收益率 = {profit_rate:.2f}%')
            else:
                print(f'{current_time}: {self.hold_bsp_type}对应卖点平仓，价格 = {current_price}')
            self.hold_bsp_type = None
            return True

        return False

    def _check_take_profit_and_stop_loss(self, chan: CChan, lv: KL_TYPE):
        """
        检查止盈和止损条件
        :param chan: CChan实例
        :param lv: 当前级别
        """
        if not self.is_hold or not self.last_buy_price or not self.hold_bsp_type:
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
        profit_rate = (current_price - self.last_buy_price) / self.last_buy_price

        # 止盈
        if profit_rate >= self.take_profit_rate:
            self.sell(current_price, self.position, current_time, "止盈卖出")
            print(f'{current_time}: 止盈卖出，价格 = {current_price}, 收益率 = {profit_rate*100:.2f}%')
            self.hold_bsp_type = None
            return

        # 止损
        if profit_rate <= -self.stop_loss_rate:
            self.sell(current_price, self.position, current_time, "止损卖出")
            print(f'{current_time}: 止损卖出，价格 = {current_price}, 亏损率 = {profit_rate*100:.2f}%')
            self.hold_bsp_type = None
            return