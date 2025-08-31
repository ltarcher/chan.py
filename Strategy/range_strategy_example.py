import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Strategy.RangeStrategy import CRangeStrategy
from Backtrace.SimpleBacktester import CSimpleBacktester
from ChanConfig import CChanConfig
from Common.CEnum import AUTYPE, DATA_SRC, KL_TYPE


def run_range_strategy_backtest():
    """
    运行区间套策略回测
    """
    # 创建区间套策略实例
    strategy = CRangeStrategy(
        max_position=1.0,      # 100%仓位
        stop_loss_rate=0.05,   # 5%止损
        take_profit_rate=0.15  # 15%止盈
    )
    
    # 创建回测引擎
    backtester = CSimpleBacktester(strategy)
    
    # 配置参数
    code = "sz.510050"  # 50ETF
    begin_time = "2024-01-01"
    end_time = "2024-06-30"
    data_src = DATA_SRC.QSTOCK
    lv_list = [KL_TYPE.K_DAY, KL_TYPE.K_60M]  # 日线和60分钟线构成区间套
    
    # 缠论配置
    config = CChanConfig({
        "bi_algo": "normal",
        "seg_algo": "chan", 
        "divergence_rate": 0.8,  # 背驰比例
        "min_zs_cnt": 1,         # 最少中枢数量
        "bs_type": "1,2,3a,3b,1p,2s",  # 计算的买卖点类型（移除了无效的"3ap"）
        "bs1_peak": False,       # 不需要创新高/新低
        "macd_algo": "area",     # MACD算法
        "bsp3_peak": False,      # 三类买卖点不突破中枢MinMax
    })
    
    # 运行回测
    result = backtester.run_backtest(
        code=code,
        begin_time=begin_time,
        end_time=end_time,
        data_src=data_src,
        lv_list=lv_list,
        config=config,
        autype=AUTYPE.QFQ
    )
    
    # 打印结果
    print("\n=== 区间套策略回测结果 ===")
    print(f"股票代码: {code}")
    print(f"回测周期: {begin_time} ~ {end_time}")
    print(f"初始资金: ¥{backtester.initial_capital:,.2f}")
    if 'final_capital' in result:
        print(f"最终资金: ¥{result['final_capital']:,.2f}")
        print(f"收益率: {result['roi']:.2f}%")
        print(f"交易次数: {result['total_trades']}")
        print(f"胜率: {result['win_rate']:.2f}%")
        print(f"平均收益: {result['avg_profit']:.2f}%")
    else:
        print("回测完成，但未产生交易")
    
    # 打印交易记录
    print("\n=== 交易记录 ===")
    transactions = strategy.get_transactions()
    if transactions:
        for transaction in transactions:
            if transaction["type"] == "buy":
                print(f"买入: {transaction['time']} 价格: {transaction['price']:.3f} 原因: {transaction['reason']}")
            else:
                print(f"卖出: {transaction['time']} 价格: {transaction['price']:.3f} 原因: {transaction['reason']} "
                      f"收益率: {transaction.get('profit_rate', 0):.2f}%")
    else:
        print("无交易记录")


if __name__ == "__main__":
    run_range_strategy_backtest()