"""
高级策略使用示例
展示如何使用高级策略和回测引擎
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ChanConfig import CChanConfig
from Common.CEnum import AUTYPE, DATA_SRC, KL_TYPE
from Strategy.MACrossStrategy import CMACrossStrategy
from Backtrace.AdvancedBacktester import CAdvancedBacktester


def run_advanced_backtest():
    """
    运行高级回测示例
    """
    # 配置参数
    code = "sz.000001"
    begin_time = "2023-01-01"
    end_time = "2023-12-31"
    data_src = DATA_SRC.BAO_STOCK
    lv_list = [KL_TYPE.K_DAY]

    # 缠论配置
    config = CChanConfig({
        "divergence_rate": 0.8,
        "min_zs_cnt": 1,
    })

    # 创建策略和回测引擎
    strategy = CMACrossStrategy(short_period=5, long_period=20)
    backtester = CAdvancedBacktester(strategy)

    # 运行回测
    result = backtester.run_backtest_with_external_data(
        code=code,
        begin_time=begin_time,
        end_time=end_time,
        data_src_type=data_src,
        lv_list=lv_list,
        config=config,
        autype=AUTYPE.QFQ,
        capital=100000.0
    )

    # 输出交易记录
    print("\n交易记录:")
    for transaction in result["transactions"]:
        print(f"  {transaction['time']}: {transaction['type']} "
              f"价格={transaction['price']:.2f} "
              f"收益={transaction.get('profit_rate', 0):.2f}%")

    # 绘制权益曲线
    backtester.plot_equity_curve(result, f"{code} 均线交叉策略回测结果")


if __name__ == "__main__":
    run_advanced_backtest()