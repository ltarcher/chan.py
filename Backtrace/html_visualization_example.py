"""
HTML可视化使用示例
展示如何使用HTML可视化功能生成回测报告
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ChanConfig import CChanConfig
from Common.CEnum import AUTYPE, DATA_SRC, KL_TYPE
from Strategy.ChanBspStrategy import CChanBspStrategy
from Backtrace.AdvancedBacktester import CAdvancedBacktester
from Backtrace.HTMLVisualization import CHTMLVisualization


def run_backtest_with_html_report():
    """
    运行回测并生成HTML报告
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
    backtester = CAdvancedBacktester(strategy, result_dir="backtest_results")

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

    # 创建HTML可视化器
    html_visualizer = CHTMLVisualization(output_dir="backtest_results")

    # 生成摘要报告
    strategy_name = f"{code} 缠论买卖点策略"
    html_visualizer.generate_summary_html(result, strategy_name)

    # 生成权益曲线报告
    html_visualizer.generate_equity_curve_html(result, strategy_name)

    print("HTML报告已生成完成！")
    print("请查看 backtest_results 目录下的HTML文件")


def run_multiple_strategies_comparison():
    """
    运行多个策略并生成对比报告
    """
    from Strategy.MACrossStrategy import CMACrossStrategy
    
    # 配置参数
    code = "sz.000001"
    begin_time = "2023-01-01"
    end_time = "2023-12-31"
    data_src = DATA_SRC.BAO_STOCK
    lv_list = [KL_TYPE.K_DAY]

    # 缠论配置
    bsp_config = CChanConfig({
        "divergence_rate": 0.9,
        "min_zs_cnt": 1,
        "max_bs2_rate": 0.618,
        "bs_type": "1,2,3a,3b",
        "bs1_peak": True,
    })

    # 均线配置
    ma_config = CChanConfig({
        "divergence_rate": 0.9,
        "min_zs_cnt": 1,
        "max_bs2_rate": 0.618,
        "bs_type": "1,2,3a,3b",
        "bs1_peak": True,
    })

    # 策略和回测器列表
    strategies_and_names = [
        (CChanBspStrategy(max_position=1.0, stop_loss_rate=0.05), "缠论买卖点策略"),
        (CMACrossStrategy(short_period=5, long_period=20), "均线交叉策略")
    ]

    results = []
    strategy_names = []

    # 运行所有策略
    for strategy, name in strategies_and_names:
        print(f"\n运行策略: {name}")
        backtester = CAdvancedBacktester(strategy, result_dir="backtest_results")
        result = backtester.run_backtest_with_external_data(
            code=code,
            begin_time=begin_time,
            end_time=end_time,
            data_src_type=data_src,
            lv_list=lv_list,
            config=bsp_config if "缠论" in name else ma_config,
            autype=AUTYPE.QFQ,
            capital=100000.0
        )
        results.append(result)
        strategy_names.append(name)

    # 创建HTML可视化器
    html_visualizer = CHTMLVisualization(output_dir="backtest_results")

    # 生成策略对比报告
    html_visualizer.generate_comparison_html(results, strategy_names)

    print("策略对比报告已生成完成！")
    print("请查看 backtest_results 目录下的HTML文件")


if __name__ == "__main__":
    print("HTML可视化示例")
    print("=" * 30)
    
    print("\n1. 生成单策略HTML报告")
    run_backtest_with_html_report()
    
    print("\n2. 生成多策略对比报告")
    run_multiple_strategies_comparison()