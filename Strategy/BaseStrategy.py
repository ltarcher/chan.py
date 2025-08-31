from abc import ABC, abstractmethod
from typing import Optional, List
from Chan import CChan
from Common.CEnum import KL_TYPE
from BuySellPoint.BS_Point import CBS_Point


class CBaseStrategy(ABC):
    """
    基础策略类，所有自定义策略应继承此类
    """

    def __init__(self):
        """
        初始化策略
        """
        self.is_hold = False  # 持仓状态
        self.position = 0  # 持仓数量
        self.last_buy_price = None  # 最近买入价格，初始化为None
        self.transactions = []  # 交易记录

    @abstractmethod
    def on_bar(self, chan: CChan, lv: KL_TYPE) -> None:
        """
        每根K线回调函数，在这里实现策略逻辑
        :param chan: CChan实例
        :param lv: 当前级别
        """
        pass

    def buy(self, price: float, volume: float, time, reason: str = ""):
        """
        买入操作
        :param price: 买入价格
        :param volume: 买入数量
        :param time: 买入时间
        :param reason: 买入原因
        """
        if self.is_hold:
            return False

        self.is_hold = True
        self.position = volume
        self.last_buy_price = price
        transaction = {
            "type": "buy",
            "price": price,
            "volume": volume,
            "time": time,
            "reason": reason
        }
        self.transactions.append(transaction)
        return True

    def sell(self, price: float, volume: float, time, reason: str = ""):
        """
        卖出操作
        :param price: 卖出价格
        :param volume: 卖出数量
        :param time: 卖出时间
        :param reason: 卖出原因
        """
        if not self.is_hold or self.position < volume:
            return False

        profit_rate = 0
        if self.last_buy_price and self.last_buy_price != 0:
            profit_rate = (price - self.last_buy_price) / self.last_buy_price * 100
            
        self.is_hold = False
        self.position -= volume
        
        transaction = {
            "type": "sell",
            "price": price,
            "volume": volume,
            "time": time,
            "reason": reason,
            "profit_rate": profit_rate
        }
        self.transactions.append(transaction)
        
        if self.position == 0:
            self.last_buy_price = None
            
        return True

    def get_transactions(self) -> List[dict]:
        """
        获取交易记录
        """
        return self.transactions.copy()

    def get_current_position(self) -> float:
        """
        获取当前持仓
        """
        return self.position

    def get_hold_status(self) -> bool:
        """
        获取持仓状态
        """
        return self.is_hold