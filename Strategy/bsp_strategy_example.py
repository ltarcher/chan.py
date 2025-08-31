"""
基于缠论买卖点的策略回测示例
展示如何使用基于一买、二买、三买的策略进行回测
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ChanConfig import CChanConfig
from Common.CEnum import AUTYPE, DATA_SRC, KL_TYPE
from Strategy.ChanBspStrategy import CChanBspStrategy
from Backtrace.AdvancedBacktester import CAdvancedBacktester


def run_bsp_strategy_backtest():
    """
    运行基于缠论买卖点的策略回测
    """
    # 配置参数
    code = "sz.000001"
    begin_time = "2023-01-01"
    end_time = "2023-12-31"
    data_src = DATA_SRC.BAO_STOCK
    lv_list = [KL_TYPE.K_DAY]

    # 缠论配置 - 启用所有类型的买卖点
    config = CChanConfig({
        "divergence_rate": 0.9,      # 背驰比例
        "min_zs_cnt": 1,             # 1类买卖点至少经历的中枢数
        "max_bs2_rate": 0.618,       # 2类买卖点回撤比例
        "bs_type": "1,2,3a,3b",      # 关注的买卖点类型
        "bs1_peak": True,            # 1类买卖点必须是中枢最低点
    })

    # 创建策略和回测引擎
    strategy = CChanBspStrategy(max_position=1.0, stop_loss_rate=0.05)
    backtester = CAdvancedBacktester(strategy)

    # 运行回测
    print("开始运行基于缠论买卖点的策略回测...")
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
    transactions = result["transactions"]
    if not transactions:
        print("  无交易记录")
    else:
        for transaction in transactions:
            profit_info = f", 收益率 = {transaction.get('profit_rate', 0):.2f}%" if transaction["type"] == "sell" else ""
            print(f"  {transaction['time']}: {transaction['type']} "
                  f"价格 = {transaction['price']:.2f}{profit_info}, 原因 = {transaction['reason']}")

    # 输出统计信息
    print(f"\n回测统计:")
    print(f"  初始资金: {result['initial_capital']:.2f}")
    print(f"  最终资金: {result['final_capital']:.2f}")
    print(f"  收益率: {result['roi']:.2f}%")
    print(f"  总交易数: {result['total_trades']}")
    print(f"  胜率: {result['win_rate']:.2f}")
    print(f"  平均收益: {result['avg_profit']:.2f}%")
    print(f"  最大回撤: {result['max_drawdown']:.2f}%")
    print(f"  夏普比率: {result['sharpe_ratio']:.2f}")

    # 按买卖点类型统计交易
    buy_type_count = {}
    for transaction in transactions:
        reason = transaction['reason']
        if reason not in buy_type_count:
            buy_type_count[reason] = 0
        buy_type_count[reason] += 1
    
    print(f"\n按买卖点类型统计:")
    for bsp_type, count in buy_type_count.items():
        print(f"  {bsp_type}: {count} 次")

    # 绘制权益曲线
    try:
        backtester.plot_equity_curve(result, f"{code} 缠论买卖点策略回测结果")
    except Exception as e:
        print(f"绘制权益曲线时出错: {e}")


def run_single_bsp_type_backtest(bsp_types: str, strategy_name: str):
    """
    运行特定买卖点类型的策略回测
    :param bsp_types: 买卖点类型，如 "1", "2", "3a,3b" 等
    :param strategy_name: 策略名称
    """
    # 配置参数
    code = "sz.000001"
    begin_time = "2023-01-01"
    end_time = "2023-12-31"
    data_src = DATA_SRC.BAO_STOCK
    lv_list = [KL_TYPE.K_DAY]

    # 缠论配置 - 仅启用指定类型的买卖点
    config = CChanConfig({
        "divergence_rate": 0.9,
        "min_zs_cnt": 1,
        "max_bs2_rate": 0.618,
        "bs_type": bsp_types,
        "bs1_peak": True,
    })

    # 创建策略和回测引擎
    strategy = CChanBspStrategy(max_position=1.0, stop_loss_rate=0.05)
    backtester = CAdvancedBacktester(strategy)

    # 运行回测
    print(f"\n开始运行{strategy_name}策略回测...")
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

    # 输出关键统计信息
    print(f"{strategy_name}策略回测结果:")
    print(f"  收益率: {result['roi']:.2f}%")
    print(f"  胜率: {result['win_rate']:.2f}")
    print(f"  最大回撤: {result['max_drawdown']:.2f}%")
    print(f"  夏普比率: {result['sharpe_ratio']:.2f}")


if __name__ == "__main__":
    # 运行完整的缠论买卖点策略回测
    run_bsp_strategy_backtest()
    
    # 分别测试不同买卖点类型的策略
    print("\n" + "="*50)
    print("分别测试不同买卖点类型的策略表现:")
    print("="*50)
    
    run_single_bsp_type_backtest("1", "一买点")
    run_single_bsp_type_backtest("2", "二买点")
    run_single_bsp_type_backtest("3a,3b", "三买点")