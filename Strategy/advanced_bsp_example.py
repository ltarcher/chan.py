"""
高级缠论买卖点策略使用示例
展示如何使用高级缠论买卖点策略进行回测
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ChanConfig import CChanConfig
from Common.CEnum import AUTYPE, DATA_SRC, KL_TYPE
from Strategy.AdvancedChanBspStrategy import CAdvancedChanBspStrategy
from Backtrace.AdvancedBacktester import CAdvancedBacktester


def run_advanced_bsp_strategy_backtest():
    """
    运行高级缠论买卖点策略回测
    """
    # 配置参数
    code = "sz.000001"
    begin_time = "2023-01-01"
    end_time = "2023-12-31"
    data_src = DATA_SRC.BAO_STOCK
    lv_list = [KL_TYPE.K_DAY]

    # 缠论配置
    config = CChanConfig({
        "divergence_rate": 0.9,         # 背驰比例
        "min_zs_cnt": 1,                # 1类买卖点至少经历的中枢数
        "max_bs2_rate": 0.618,          # 2类买卖点回撤比例
        "bs_type": "1,2,3a,3b",         # 关注的买卖点类型
        "bs1_peak": True,               # 1类买卖点必须是中枢最低点
        "bsp2_follow_1": True,          # 2类买卖点必须跟在1类买卖点后面
        "bsp3_follow_1": True,          # 3类买卖点必须跟在1类买卖点后面
    })

    # 创建高级策略实例
    strategy = CAdvancedChanBspStrategy(
        max_position=1.0,               # 最大仓位100%
        stop_loss_rate=0.05,            # 止损5%
        take_profit_rate=0.15,          # 止盈15%
        enable_types=['1', '2', '3a', '3b'],  # 启用所有买卖点类型
        position_per_bsp={
            '1': 1.0,   # 一买满仓
            '2': 0.7,   # 二买70%仓位
            '3a': 0.5,  # 三买50%仓位
            '3b': 0.5   # 三买50%仓位
        }
    )
    
    # 创建回测引擎
    backtester = CAdvancedBacktester(strategy)

    # 运行回测
    print("开始运行高级缠论买卖点策略回测...")
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


def run_compare_strategies():
    """
    对比不同买卖点策略的表现
    """
    print("="*60)
    print("对比不同买卖点策略的表现")
    print("="*60)
    
    # 配置参数
    code = "sz.000001"
    begin_time = "2023-01-01"
    end_time = "2023-12-31"
    data_src = DATA_SRC.BAO_STOCK
    lv_list = [KL_TYPE.K_DAY]

    # 基础配置
    base_config = {
        "divergence_rate": 0.9,
        "min_zs_cnt": 1,
        "max_bs2_rate": 0.618,
        "bs1_peak": True,
    }
    
    strategies_config = [
        {
            "name": "只使用一买点",
            "bs_type": "1",
            "enable_types": ['1']
        },
        {
            "name": "只使用二买点",
            "bs_type": "2",
            "enable_types": ['2']
        },
        {
            "name": "只使用三买点",
            "bs_type": "3a,3b",
            "enable_types": ['3a', '3b']
        },
        {
            "name": "综合使用所有买点",
            "bs_type": "1,2,3a,3b",
            "enable_types": ['1', '2', '3a', '3b']
        }
    ]
    
    results = []
    
    for strategy_config in strategies_config:
        # 更新配置
        config_dict = base_config.copy()
        config_dict["bs_type"] = strategy_config["bs_type"]
        config = CChanConfig(config_dict)
        
        # 创建策略
        strategy = CAdvancedChanBspStrategy(
            max_position=1.0,
            stop_loss_rate=0.05,
            take_profit_rate=0.15,
            enable_types=strategy_config["enable_types"]
        )
        
        # 创建回测引擎
        backtester = CAdvancedBacktester(strategy)
        
        # 运行回测
        print(f"\n运行策略: {strategy_config['name']}")
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
        
        results.append({
            "name": strategy_config['name'],
            "roi": result['roi'],
            "win_rate": result['win_rate'],
            "max_drawdown": result['max_drawdown'],
            "sharpe_ratio": result['sharpe_ratio'],
            "total_trades": result['total_trades']
        })
    
    # 输出对比结果
    print("\n策略表现对比:")
    print("-" * 80)
    print(f"{'策略名称':<20} {'收益率(%)':<12} {'胜率':<10} {'最大回撤(%)':<12} {'夏普比率':<10} {'交易次数':<8}")
    print("-" * 80)
    
    for result in results:
        print(f"{result['name']:<20} {result['roi']:<12.2f} {result['win_rate']:<10.2f} "
              f"{result['max_drawdown']:<12.2f} {result['sharpe_ratio']:<10.2f} {result['total_trades']:<8}")


if __name__ == "__main__":
    # 运行高级策略回测
    run_advanced_bsp_strategy_backtest()
    
    # 对比不同策略
    run_compare_strategies()