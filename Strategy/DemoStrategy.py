from typing import Optional
from Strategy.BaseStrategy import CBaseStrategy
from Chan import CChan
from Common.CEnum import KL_TYPE, BSP_TYPE, FX_TYPE


class CDemoStrategy(CBaseStrategy):
    """
    示例策略：基于一类买卖点的简单策略
    底分型形成后开仓，顶分型形成后平仓
    """

    def __init__(self):
        super().__init__()
        self.last_bsp: Optional[CBS_Point] = None

    def on_bar(self, chan: CChan, lv: KL_TYPE) -> None:
        """
        每根K线回调函数
        :param chan: CChan实例
        :param lv: 当前级别
        """
        # 获取买卖点列表
        bsp_list = chan.get_latest_bsp()
        if not bsp_list:
            return

        # 获取最后一个买卖点
        last_bsp = bsp_list[0]
        
        # 只关注一类买卖点
        if BSP_TYPE.T1 not in last_bsp.type and BSP_TYPE.T1P not in last_bsp.type:
            return

        # 获取当前级别的chan数据
        cur_lv_chan = chan[lv]
        
        # 确保我们有足够的K线数据
        if len(cur_lv_chan) < 2:
            return
            
        # 检查是否是倒数第二根K线的分形
        if last_bsp.klu.klc.idx != cur_lv_chan[-2].idx:
            return

        current_time = cur_lv_chan[-1][-1].time
        current_price = cur_lv_chan[-1][-1].close

        # 底分型形成后开仓
        if cur_lv_chan[-2].fx == FX_TYPE.BOTTOM and last_bsp.is_buy and not self.is_hold:
            self.buy(current_price, 1, current_time, "T1 Bottom Formation")
            print(f'{current_time}: 买入价格 = {current_price}')

        # 顶分型形成后平仓
        elif cur_lv_chan[-2].fx == FX_TYPE.TOP and not last_bsp.is_buy and self.is_hold:
            self.sell(current_price, 1, current_time, "T1 Top Formation")
            profit_rate = (current_price - self.last_buy_price) / self.last_buy_price * 100
            print(f'{current_time}: 卖出价格 = {current_price}, 收益率 = {profit_rate:.2f}%')