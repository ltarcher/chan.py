# -*- coding:utf-8 -*-
# !/usr/bin/env python

"""
资金流向数据MCP工具接口
提供个股资金流向、北向资金流向和同花顺资金流向数据的MCP工具接口
"""

import pandas as pd
from typing import Dict, List, Optional, Union
import logging
import traceback
from .mcp_instance import mcp
from qstock.data import money  # 直接导入money模块

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@mcp.tool()
async def get_intraday_money(
    code: str
) -> List[Dict]:
    """
    获取单只股票最新交易日的日内分钟级单子流入流出数据
    
    参数:
        code: 股票代码，如"000001"、"600000"等
        
    返回:
        List[Dict]: 包含股票日内资金流向数据的字典列表
        
    示例:
        data = await get_intraday_money('000001')
    """
    try:
        logger.info(f"获取股票日内资金流向数据，股票代码: {code}")
        df = money.intraday_money(code=code)
        
        if df.empty:
            logger.warning("返回数据为空")
            return []
            
        # 将时间列转换为字符串格式
        if '时间' in df.columns:
            df['时间'] = df['时间'].astype(str)
        
        return df.to_dict(orient='records')
    except Exception as e:
        logger.error(f"获取股票日内资金流向数据失败: {str(e)}")
        traceback.print_exc()
        return []
    
@mcp.tool()
async def get_realtime_index_money() -> Dict:
    """
    获取当日实时全市场资金流向数据
    
    返回:
        Dict: 包含当日全市场实时资金流向数据的字典
    示例:
        data = await get_realtime_index_money()
    """
    index_list = ["上证指数", "深证成指", "北证50"]
    total_realtime_money_df = pd.DataFrame()
    for index in index_list:
        money_realtime_df = money.intraday_money(index)
        if money_realtime_df.empty:
            logger.warning("返回数据为空")
            continue

        #取最后一列
        latest_money_realtime_df = money_realtime_df.iloc[-1]
        total_realtime_money_df = pd.concat([total_realtime_money_df, pd.DataFrame(latest_money_realtime_df).T])
    total_realtime_money_df = total_realtime_money_df.reset_index()
    #删除index列
    total_realtime_money_df = total_realtime_money_df.drop(columns=["index"])
    record = {}
    record["数据"] = total_realtime_money_df.to_dict(orient='records')
    record["汇总"] = {
        "主力净流入": total_realtime_money_df["主力净流入"].sum(),
        "小单净流入": total_realtime_money_df["小单净流入"].sum(),
        "中单净流入": total_realtime_money_df["中单净流入"].sum(),
        "大单净流入": total_realtime_money_df["大单净流入"].sum(),
        "超大单净流入": total_realtime_money_df["超大单净流入"].sum()
    }
    return record

@mcp.tool()
async def get_hist_money(
    code: str
) -> List[Dict]:
    """
    获取单支股票、债券的历史单子流入流出数据
    
    参数:
        code: 股票代码，如"000001"、"600000"等
        
    返回:
        List[Dict]: 包含股票历史资金流向数据的字典列表
        
    示例:
        data = await get_hist_money('000001')
    """
    try:
        logger.info(f"获取股票历史资金流向数据，股票代码: {code}")
        df = money.hist_money(code=code)
        
        if df.empty:
            logger.warning("返回数据为空")
            return []
            
        # 将日期列转换为字符串格式
        if '日期' in df.columns:
            df['日期'] = df['日期'].astype(str)
        
        return df.to_dict(orient='records')
    except Exception as e:
        logger.error(f"获取股票历史资金流向数据失败: {str(e)}")
        traceback.print_exc()
        return []

@mcp.tool()
async def get_index_hist_money(freq: str="D") -> Dict:
    """
    获取全市场历史资金流向数据

    参数:
        freq: 数据周期，可选值: "D"、"M"、"Y"，分别表示日、月、年，默认为"D"
    
    返回:
        Dict: 包含全市场历史资金流向数据的字典
    """
    index_list = ["上证指数", "深证成指", "北证50"]

    #历史资金
    total_history_money_df = pd.DataFrame()
    for index in index_list:
        money_df = money.hist_money(index)
        if money_df.empty:
            continue
        total_history_money_df = pd.concat([total_history_money_df, money_df], axis=0)
    total_history_money_df = total_history_money_df.reset_index(drop=True)

    record = {}
    record["数据"] = total_history_money_df.to_dict(orient='records')
    #根据统计周期，计算资金流入流出
    if freq == "D":
        # 按日期分组统计
        daily_flow = total_history_money_df.groupby('日期').agg({
            '主力净流入': 'sum',
            '小单净流入': 'sum',
            '中单净流入': 'sum',
            '大单净流入': 'sum',
            '超大单净流入': 'sum'
        }).reset_index()
        daily_flow['日期'] = daily_flow['日期'].astype(str)
        record["汇总"] =daily_flow.to_dict(orient='records')
        return record
    elif freq == "M":
        # 按月份分组统计
        # 确保日期列是日期类型
        total_history_money_df['日期'] = pd.to_datetime(total_history_money_df['日期'])
        # 创建年月字段用于按月分组
        total_history_money_df['年月'] = total_history_money_df['日期'].dt.strftime('%Y-%m')
        # 按月分组统计
        monthly_flow = total_history_money_df.groupby('年月').agg({
            '主力净流入': 'sum',
            '小单净流入': 'sum',
            '中单净流入': 'sum',
            '大单净流入': 'sum',
            '超大单净流入': 'sum'
        }).reset_index()
        monthly_flow['年月'] = monthly_flow['年月'].astype(str)
        record['汇总'] = monthly_flow.to_dict(orient='records')
        return record
    elif freq == "Y":
        # 按年份分组统计
        # 确保日期列是日期类型
        total_history_money_df['日期'] = pd.to_datetime(total_history_money_df['日期'])
        # 创建年份字段用于按年分组
        total_history_money_df['年份'] = total_history_money_df['日期'].dt.strftime('%Y')
        # 按年分组统计
        yearly_flow = total_history_money_df.groupby('年份').agg({
            '主力净流入': 'sum',
            '小单净流入': 'sum',
            '中单净流入': 'sum',
            '大单净流入': 'sum',
            '超大单净流入': 'sum'
        }).reset_index()
        yearly_flow['年份'] = yearly_flow['年份'].astype(str)
        record['汇总'] = yearly_flow.to_dict(orient='records')
        return record
    else:
        logger.error(f"参数freq错误，可选值: D、M")
        return {}


@mcp.tool()
async def get_stock_money(
    code: str,
    ndays: Union[int, List[int]] = [3, 5, 10, 20]
) -> List[Dict]:
    """
    获取个股n日资金流
    
    参数:
        code: 股票代码或名称，如"000001"、"600000"等
        ndays: 时间周期，可以是单个整数或整数列表，如3、5、10、20等，默认为[3, 5, 10, 20]
        
    返回:
        List[Dict]: 包含个股n日资金流数据的字典列表
        
    示例:
        data = await get_stock_money('000001', [3, 5, 10])
    """
    try:
        logger.info(f"获取个股n日资金流数据，股票代码: {code}, 时间周期: {ndays}")
        df = money.stock_money(code=code, ndays=ndays)
        
        if df.empty:
            logger.warning("返回数据为空")
            return []
            
        # 重置索引，将日期列转换为字符串
        df = df.reset_index()
        if 'index' in df.columns:
            df['index'] = df['index'].astype(str)
        
        return df.to_dict(orient='records')
    except Exception as e:
        logger.error(f"获取个股n日资金流数据失败: {str(e)}")
        traceback.print_exc()
        return []

@mcp.tool()
async def get_north_money(
    flag: Optional[str] = None,
    n: Union[int, str] = 1
) -> Union[List[Dict], Dict]:
    """
    获取北向资金流向数据
    
    参数:
        flag: 数据类型，可选值:
             None: 返回北上资金总体每日净流入数据
             '行业': 北向资金增持行业板块排行
             '概念': 北向资金增持概念板块排行
             '地域': 北向资金增持地域板块排行
             '个股': 北向资金增持个股情况
             '沪股通': 沪股通资金流向
             '深股通': 深股通资金流向
        n: 代表n日排名，可选值:
           1, 3, 5, 10, 'M', 'Q', 'Y'
           即 {'1':"今日", '3':"3日",'5':"5日", '10':"10日",'M':"月", 'Q':"季", 'Y':"年"}
        
    返回:
        List[Dict]或Dict: 包含北向资金流向数据的字典列表或字典
        
    示例:
        data = await get_north_money('行业', 5)
    """
    try:
        logger.info(f"获取北向资金流向数据，类型: {flag}, 周期: {n}")
        result = money.north_money(flag=flag, n=n)
        
        if isinstance(result, pd.DataFrame):
            if result.empty:
                logger.warning("返回数据为空")
                return []
                
            # 处理日期列
            date_cols = ['日期', 'date']
            for col in date_cols:
                if col in result.columns:
                    result[col] = result[col].astype(str)
                    
            return result.to_dict(orient='records')
        elif isinstance(result, pd.Series):
            # 如果返回的是Series，转换为字典
            result = result.to_dict()
            return result
        else:
            return result
    except Exception as e:
        logger.error(f"获取北向资金流向数据失败: {str(e)}")
        traceback.print_exc()
        return []

@mcp.tool()
async def get_north_money_flow(
    flag: str = "北上"
) -> List[Dict]:
    """
    获取东方财富网沪深港通持股-北向资金净流入
    
    参数:
        flag: 资金流向类型，可选值:
             "沪股通": 沪股通资金流向
             "深股通": 深股通资金流向
             "北上": 北上资金总体流向（默认）
        
    返回:
        List[Dict]: 包含北向资金净流入数据的字典列表
        
    示例:
        data = await get_north_money_flow('沪股通')
    """
    try:
        logger.info(f"获取北向资金净流入数据，类型: {flag}")
        df = money.north_money_flow(flag=flag)
        
        if df.empty:
            logger.warning("返回数据为空")
            return []
            
        # 将日期列转换为字符串格式
        if 'date' in df.columns:
            df['date'] = df['date'].astype(str)
        
        return df.to_dict(orient='records')
    except Exception as e:
        logger.error(f"获取北向资金净流入数据失败: {str(e)}")
        traceback.print_exc()
        return []

@mcp.tool()
async def get_north_money_stock(
    n: Union[int, str] = 1
) -> List[Dict]:
    """
    获取东方财富北向资金增减持个股情况
    
    参数:
        n: 代表n日排名，可选值:
           1, 3, 5, 10, 'M', 'Q', 'Y'
           即 {'1':"今日", '3':"3日",'5':"5日", '10':"10日",'M':"月", 'Q':"季", 'Y':"年"}
        
    返回:
        List[Dict]: 包含北向资金增减持个股情况数据的字典列表
        
    示例:
        data = await get_north_money_stock(5)
    """
    try:
        logger.info(f"获取北向资金增减持个股情况数据，周期: {n}")
        df = money.north_money_stock(n=n)
        
        if df.empty:
            logger.warning("返回数据为空")
            return []
            
        # 将日期列转换为字符串格式
        if '日期' in df.columns:
            df['日期'] = df['日期'].astype(str)
        
        return df.to_dict(orient='records')
    except Exception as e:
        logger.error(f"获取北向资金增减持个股情况数据失败: {str(e)}")
        traceback.print_exc()
        return []

@mcp.tool()
async def get_north_money_sector(
    flag: str = "行业",
    n: Union[int, str] = 1
) -> List[Dict]:
    """
    获取东方财富网北向资金增持板块排行
    
    参数:
        flag: 板块类型，可选值:
             "行业": 行业板块（默认）
             "概念": 概念板块
             "地域": 地域板块
        n: 代表n日排名，可选值:
           1, 3, 5, 10, 'M', 'Q', 'Y'
           即 {'1':"今日", '3':"3日",'5':"5日", '10':"10日",'M':"月", 'Q':"季", 'Y':"年"}
        
    返回:
        List[Dict]: 包含北向资金增持板块排行数据的字典列表
        
    示例:
        data = await get_north_money_sector('概念', 5)
    """
    try:
        logger.info(f"获取北向资金增持板块排行数据，板块类型: {flag}, 周期: {n}")
        df = money.north_money_sector(flag=flag, n=n)
        
        if df.empty:
            logger.warning("返回数据为空")
            return []
            
        # 将日期列转换为字符串格式
        if '日期' in df.columns:
            df['日期'] = df['日期'].astype(str)
        
        return df.to_dict(orient='records')
    except Exception as e:
        logger.error(f"获取北向资金增持板块排行数据失败: {str(e)}")
        traceback.print_exc()
        return []

@mcp.tool()
async def get_ths_money(
    flag: Optional[str] = None,
    n: Optional[int] = None
) -> List[Dict]:
    """
    获取同花顺个股、行业、概念资金流数据
    
    参数:
        flag: 数据类型，可选值:
             None: 默认获取个股资金流数据
             '个股': 获取个股资金流数据
             '概念'或'概念板块': 获取概念板块资金流数据
             '行业'或'行业板块': 获取行业板块资金流数据
        n: 时间周期，可选值:
           None, 3, 5, 10, 20
           分别表示"即时", "3日排行", "5日排行", "10日排行", "20日排行"
        
    返回:
        List[Dict]: 包含同花顺资金流数据的字典列表
        
    示例:
        data = await get_ths_money('概念', 5)
    """
    try:
        logger.info(f"获取同花顺资金流数据，类型: {flag}, 周期: {n}")
        df = money.ths_money(flag=flag, n=n)
        
        if df.empty:
            logger.warning("返回数据为空")
            return []
        
        return df.to_dict(orient='records')
    except Exception as e:
        logger.error(f"获取同花顺资金流数据失败: {str(e)}")
        traceback.print_exc()
        return []

@mcp.tool()
async def get_ths_stock_money(
    n: Optional[int] = None
) -> List[Dict]:
    """
    获取同花顺个股资金流向
    
    参数:
        n: 时间周期，可选值:
           None, 3, 5, 10, 20
           分别表示"即时", "3日排行", "5日排行", "10日排行", "20日排行"
        
    返回:
        List[Dict]: 包含同花顺个股资金流向数据的字典列表
        
    示例:
        data = await get_ths_stock_money(5)
    """
    try:
        logger.info(f"获取同花顺个股资金流向数据，周期: {n}")
        df = money.ths_stock_money(n=n)
        
        if df.empty:
            logger.warning("返回数据为空")
            return []
        
        return df.to_dict(orient='records')
    except Exception as e:
        logger.error(f"获取同花顺个股资金流向数据失败: {str(e)}")
        traceback.print_exc()
        return []

@mcp.tool()
async def get_ths_concept_money(
    n: Optional[int] = None
) -> List[Dict]:
    """
    获取同花顺概念板块资金流
    
    参数:
        n: 时间周期，可选值:
           None, 3, 5, 10, 20
           分别表示"即时", "3日排行", "5日排行", "10日排行", "20日排行"
        
    返回:
        List[Dict]: 包含同花顺概念板块资金流数据的字典列表
        
    示例:
        data = await get_ths_concept_money(5)
    """
    try:
        logger.info(f"获取同花顺概念板块资金流数据，周期: {n}")
        df = money.ths_concept_money(n=n)
        
        if df.empty:
            logger.warning("返回数据为空")
            return []
        
        return df.to_dict(orient='records')
    except Exception as e:
        logger.error(f"获取同花顺概念板块资金流数据失败: {str(e)}")
        traceback.print_exc()
        return []

@mcp.tool()
async def get_ths_industry_money(
    n: Optional[int] = None
) -> List[Dict]:
    """
    获取同花顺行业资金流
    
    参数:
        n: 时间周期，可选值:
           None, 3, 5, 10, 20
           分别表示"即时", "3日排行", "5日排行", "10日排行", "20日排行"
        
    返回:
        List[Dict]: 包含同花顺行业资金流数据的字典列表
        
    示例:
        data = await get_ths_industry_money(5)
    """
    try:
        logger.info(f"获取同花顺行业资金流数据，周期: {n}")
        df = money.ths_industry_money(n=n)
        
        if df.empty:
            logger.warning("返回数据为空")
            return []
        
        return df.to_dict(orient='records')
    except Exception as e:
        logger.error(f"获取同花顺行业资金流数据失败: {str(e)}")
        traceback.print_exc()
        return []