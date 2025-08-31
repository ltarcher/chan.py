"""
区间套策略HTML可视化使用示例
展示如何使用HTML可视化功能生成区间套策略回测报告
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ChanConfig import CChanConfig
from Common.CEnum import AUTYPE, DATA_SRC, KL_TYPE
from Strategy.RangeStrategy import CRangeStrategy
from Backtrace.AdvancedBacktester import CAdvancedBacktester
from Backtrace.HTMLVisualization import CHTMLVisualization


def run_range_strategy_with_html_report():
    """
    运行区间套策略回测并生成HTML报告
    """
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
        "bs_type": "1,2,3a,3b,1p,2s",  # 计算的买卖点类型
        "bs1_peak": False,       # 不需要创新高/新低
        "macd_algo": "area",     # MACD算法
        "bsp3_peak": False,      # 三类买卖点不突破中枢MinMax
    })

    # 创建策略和回测引擎
    strategy = CRangeStrategy(
        max_position=1.0,      # 100%仓位
        stop_loss_rate=0.05,   # 5%止损
        take_profit_rate=0.15  # 15%止盈
    )
    backtester = CAdvancedBacktester(strategy, result_dir="backtest_results")

    # 运行回测
    print("开始运行区间套策略回测...")
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
    strategy_name = f"{code} 区间套策略"
    html_visualizer.generate_summary_html(result, strategy_name)

    # 生成权益曲线报告
    html_visualizer.generate_equity_curve_html(result, strategy_name)

    print("区间套策略HTML报告已生成完成！")
    print("请查看 backtest_results 目录下的HTML文件")


def run_range_vs_other_strategies_comparison():
    """
    运行区间套策略与其他策略的对比并生成HTML报告
    """
    from Strategy.ChanBspStrategy import CChanBspStrategy
    from Strategy.MACrossStrategy import CMACrossStrategy
    
    # 配置参数
    code = "sz.510050"  # 50ETF
    begin_time = "2024-01-01"
    end_time = "2024-06-30"
    data_src = DATA_SRC.QSTOCK
    lv_list = [KL_TYPE.K_DAY, KL_TYPE.K_60M]  # 日线和60分钟线构成区间套

    # 缠论配置
    chan_config = CChanConfig({
        "bi_algo": "normal",
        "seg_algo": "chan", 
        "divergence_rate": 0.8,  # 背驰比例
        "min_zs_cnt": 1,         # 最少中枢数量
        "bs_type": "1,2,3a,3b,1p,2s",  # 计算的买卖点类型
        "bs1_peak": False,       # 不需要创新高/新低
        "macd_algo": "area",     # MACD算法
        "bsp3_peak": False,      # 三类买卖点不突破中枢MinMax
    })

    # 策略和回测器列表
    strategies_and_names = [
        (CRangeStrategy(
            max_position=1.0, 
            stop_loss_rate=0.05, 
            take_profit_rate=0.15
        ), "区间套策略"),
        (CChanBspStrategy(
            max_position=1.0, 
            stop_loss_rate=0.05
        ), "缠论买卖点策略"),
        (CMACrossStrategy(
            short_period=5, 
            long_period=20
        ), "均线交叉策略"),
    ]

    # 存储结果
    results = []
    strategy_names = []

    # 运行各个策略
    for strategy, name in strategies_and_names:
        print(f"运行{name}...")
        backtester = CAdvancedBacktester(strategy, result_dir="backtest_results")
        
        # 对于非缠论策略，使用单级别数据
        use_lv_list = lv_list if name == "区间套策略" or name == "缠论买卖点策略" else [KL_TYPE.K_DAY]
        use_config = chan_config if name == "区间套策略" or name == "缠论买卖点策略" else CChanConfig()
        
        result = backtester.run_backtest_with_external_data(
            code=code,
            begin_time=begin_time,
            end_time=end_time,
            data_src_type=data_src,
            lv_list=use_lv_list,
            config=use_config,
            autype=AUTYPE.QFQ,
            capital=100000.0
        )
        results.append(result)
        strategy_names.append(name)
        print(f"{name}回测完成")

    # 创建HTML可视化器
    html_visualizer = CHTMLVisualization(output_dir="backtest_results")

    # 生成策略对比报告
    html_visualizer.generate_comparison_html(results, strategy_names, "区间套策略对比报告")

    print("策略对比HTML报告已生成完成！")
    print("请查看 backtest_results 目录下的HTML文件")


if __name__ == "__main__":
    print("请选择要运行的示例:")
    print("1. 区间套策略单独报告")
    print("2. 区间套策略与其他策略对比报告")
    
    choice = input("请输入选择 (1 或 2): ").strip()
    
    if choice == "1":
        run_range_strategy_with_html_report()
    elif choice == "2":
        run_range_vs_other_strategies_comparison()
    else:
        print("无效选择")