"""
运行所有策略的主入口文件
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from Strategy.bsp_strategy_example import run_bsp_strategy_backtest
from Strategy.advanced_bsp_example import run_advanced_bsp_strategy_backtest, run_compare_strategies
from Strategy.strategy_example import run_simple_backtest
from Strategy.advanced_strategy_example import run_advanced_backtest
from Strategy.range_strategy_example import run_range_strategy_backtest
from Strategy.range_strategy_html_example import run_range_strategy_with_html_report, run_range_vs_other_strategies_comparison
from Backtrace.html_visualization_example import run_backtest_with_html_report, run_multiple_strategies_comparison


def main():
    print("缠论策略测试系统")
    print("=" * 50)
    
    while True:
        print("\n请选择要运行的策略:")
        print("1. 基础缠论买卖点策略")
        print("2. 高级缠论买卖点策略")
        print("3. 策略对比测试")
        print("4. 基础技术指标策略")
        print("5. 高级技术指标策略")
        print("6. 区间套策略")
        print("7. 生成HTML可视化报告")
        print("8. 生成策略对比HTML报告")
        print("9. 区间套策略HTML报告")
        print("10. 区间套策略与其他策略对比报告")
        print("0. 退出")
        
        choice = input("\n请输入选择 (0-10): ").strip()
        
        if choice == "1":
            print("\n运行基础缠论买卖点策略...")
            run_bsp_strategy_backtest()
        elif choice == "2":
            print("\n运行高级缠论买卖点策略...")
            run_advanced_bsp_strategy_backtest()
        elif choice == "3":
            print("\n运行策略对比测试...")
            run_compare_strategies()
        elif choice == "4":
            print("\n运行基础技术指标策略...")
            run_simple_backtest()
        elif choice == "5":
            print("\n运行高级技术指标策略...")
            run_advanced_backtest()
        elif choice == "6":
            print("\n运行区间套策略...")
            run_range_strategy_backtest()
        elif choice == "7":
            print("\n生成HTML可视化报告...")
            run_backtest_with_html_report()
        elif choice == "8":
            print("\n生成策略对比HTML报告...")
            run_multiple_strategies_comparison()
        elif choice == "9":
            print("\n生成区间套策略HTML报告...")
            run_range_strategy_with_html_report()
        elif choice == "10":
            print("\n生成区间套策略与其他策略对比报告...")
            run_range_vs_other_strategies_comparison()
        elif choice == "0":
            print("退出程序")
            break
        else:
            print("无效选择，请重新输入")


if __name__ == "__main__":
    main()