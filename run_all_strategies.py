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
        print("0. 退出")
        
        choice = input("\n请输入选择 (0-5): ").strip()
        
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
        elif choice == "0":
            print("退出程序")
            break
        else:
            print("无效选择，请重新输入")


if __name__ == "__main__":
    main()