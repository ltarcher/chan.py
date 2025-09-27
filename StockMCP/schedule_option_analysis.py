#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
定时执行AI股票期权智能分析团队的脚本
"""

import schedule
import time
import logging
from datetime import datetime
from option_analysis_team import OptionAnalysisTeam

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('option_analysis.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def run_morning_analysis():
    """
    每日开盘前分析
    """
    logger.info("开始执行开盘前市场分析")
    try:
        team = OptionAnalysisTeam()
        result = team.run_daily_analysis()
        logger.info("开盘前市场分析完成")
        return result
    except Exception as e:
        logger.error(f"开盘前市场分析失败: {e}")
        return None

def run_market_time_analysis():
    """
    盘中定期分析
    """
    logger.info("开始执行盘中市场分析")
    try:
        team = OptionAnalysisTeam()
        result = team.run_daily_analysis()
        logger.info("盘中市场分析完成")
        return result
    except Exception as e:
        logger.error(f"盘中市场分析失败: {e}")
        return None

def run_end_of_day_analysis():
    """
    收盘后分析
    """
    logger.info("开始执行收盘后市场分析")
    try:
        team = OptionAnalysisTeam()
        result = team.run_daily_analysis()
        logger.info("收盘后市场分析完成")
        return result
    except Exception as e:
        logger.error(f"收盘后市场分析失败: {e}")
        return None

def main():
    """
    主函数，设置定时任务
    """
    logger.info("启动AI股票期权智能分析定时任务系统")
    
    # 设置定时任务
    # 开盘前分析 (9:00)
    schedule.every().day.at("09:00").do(run_morning_analysis)
    
    # 盘中定期分析 (每30分钟一次，交易时间内)
    schedule.every(30).minutes.do(run_market_time_analysis)
    
    # 收盘后分析 (15:30)
    schedule.every().day.at("15:30").do(run_end_of_day_analysis)
    
    logger.info("定时任务设置完成，系统开始运行...")
    
    # 保持程序运行
    while True:
        schedule.run_pending()
        time.sleep(60)  # 每分钟检查一次

if __name__ == "__main__":
    main()