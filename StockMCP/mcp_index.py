#!/usr/bin/env python
# -*- encoding=utf8 -*-

import logging
import traceback
import time
import qstock as qs
# 修复导入路径，使用相对导入
from .mcp_instance import mcp
from typing import Dict, List, Optional, Union
from .dataservices.index_data import IndexDataService

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

data_service = None

def intitialize_data_service():
    """ 初始化数据服务 """
    # 创建数据服务实例
    global data_service
    if data_service is None:
        logger.info("Initializing IndexDataService...")
        data_service = IndexDataService()
    return data_service

@mcp.tool()
async def get_index_realtime_data(codes: Union[list, str], market: str = "沪深A") -> list:
    """ 获取中国金融市场指定指数或股票、期权、期货的实时数据
    参数：
    - codes: 指数或股票、期权、期货代码/名称，类型可以是字符串或字符串列表；如果是字符串，只能是特定代码，例如："000001", 为空时默认获取指定的市场指数；如果是字符串列表，则可以同时指定多个代码，如["000001", "600000"]，默认为空；
    - market: 市场类型，可选：["沪深A", "港股", "美股", "期货", "外汇", "债券"]，默认为沪深A
    返回：
    - List[Dict]: 包含指数或股票代码、名称、最新价、涨跌幅等，
    Dict包含字段:
    ['代码', '名称', '涨幅', '最新', '最高', '最低', '今开', '换手率', '量比', '市盈率', '成交量', '成交额', '昨收', '总市值', '流通市值', '市场', '时间']
    """
    return data_service.get_index_realtime_data(codes, market)

# 获取实时沪深京三市场的总成交数据
@mcp.tool()
async def get_board_trade_realtime_data() -> Dict:
    """ 获取沪深京三市场的成交数据
    返回：
    - Dict: 包含日期和成交额的字典，格式为 {'日期': 日期, '总成交额': 总成交额'}
    """
    return data_service.get_board_trade_realtime_data()

# 获取沪深京三市场的历史总成交数据
@mcp.tool()
async def get_turnover_history_data(start_date: Optional[str] = '19000101', end_date: Optional[str] = None, freq: Optional[str] = 'd', fqt: Optional[int] = 1, use_chinese_fields: bool = True) -> List[Dict]:
    """ 获取沪深京三市场的历史总成交数据
    参数：
    - start_date: 开始日期，格式为'YYYY-MM-DD'，默认为None
    - end_date: 结束日期，格式为'YYYY-MM-DD'，默认为None
    - freq: 数据频率，'d'表示日频，'m'表示月频，默认为日频
    - fqt: 复权类型，默认为1，可选值为0， 1，2
    - use_chinese_fields: 是否使用中文字段名，默认为True
    返回：
    - List[Dict]: 包含成交数据的字典列表
    Dict包含字段: ['日期', '成交额'] 或 ['date', 'turnover']（取决于use_chinese_fields参数）
    """
    return data_service.get_turnover_history_data(start_date=start_date, end_date=end_date, freq=freq, fqt=fqt, use_chinese_fields=use_chinese_fields)

@mcp.tool()
async def get_turnover_history_in_5_days(start_date: Optional[str] = None, end_date: Optional[str] = None, use_chinese_fields: bool = True) -> List[Dict]:
    """ 获取沪深京三市场最近5个交易日的竞价总成交数据
    参数：
    - start_date: 开始日期，格式为'YYYY-MM-DD'，默认为None（取最近5个交易日）
    - end_date: 结束日期，格式为'YYYY-MM-DD'，默认为None（取到最新数据）
    - use_chinese_fields: 是否使用中文字段名，默认为True
    返回：
    - List[Dict]: 包含最近5个交易日的总成交数据，
    Dict包含字段: ['日期', '成交额'] 或 ['date', 'turnover']（取决于use_chinese_fields参数）
    """
    data = data_service.get_turnover_history_data(start_date=start_date, end_date=end_date, freq=1, fqt=1, use_chinese_fields=use_chinese_fields)
    # 遍历日期列只取当日包含9:30的数据,其他时间段的数据不取
    data = [d for d in data if '09:30:00' in d['日期']]
    return data

@mcp.tool()
async def get_rzrq_turnover_ratio(start_date = None, end_date = None, page: int = 1, page_size: int = 10, use_chinese_fields: bool = True) -> List[Dict]:
    """ 获取融资融券占总成交比例数据(含总融资余额与上证指数偏离率)
    参数：
    - start_date: 开始日期，格式为'YYYY-MM-DD'，默认为None
    - end_date: 结束日期，格式为'YYYY-MM-DD'，默认为None
    - page: 页码，默认为1
    - page_size: 每页数量，默认为10
    - use_chinese_fields: 是否使用中文字段名，默认为True
    返回：
    - List[Dict]: 包含日期和融资融券占成交比例的字典列表，格式为 [{'日期': 日期, '融资融券占总成交比例': 比例, '总融资余额与上证指数偏离率': 偏离比例}]
    """
    return data_service.get_rzrq_turnover_ratio(start_date, end_date, page, page_size, use_chinese_fields)

@mcp.tool()
async def get_usd_index_data() -> list:
    """ 获取美元指数实时数据
    参数：
    - 无
    返回：
    - List[Dict]: 包含美元指数的实时数据，
    Dict包含字段:  ['代码', '名称', '涨幅', '最新', '最高', '最低', '今开', '换手率', '量比', '市盈率', '成交量', '成交额', '昨收', '总市值', '流通市值', '市场', '时间']
    """
    return data_service.get_usd_index_data()

@mcp.tool()
async def get_ftse_a50_futures_data() -> list:
    """ 获取富时A50期货指数实时数据
    参数：
    - 无
    返回：
    - List[Dict]: 包含富时A50期货指数的实时数据，
    Dict包含字段: ['代码', '名称', '涨幅', '最新', '最高', '最低', '今开', '成交量', '成交额', '昨收', '持仓量', '市场', '时间']
    """
    return data_service.get_ftse_a50_futures_data()

@mcp.tool()
async def get_usd_cnh_futures_data() -> list:
    """ 获取美元兑离岸人民币主连实时数据
    参数：
    - 无
    返回：
    - List[Dict]: 包含美元兑离岸人民币主连的实时数据，
    Dict包含字段: ['代码', '名称', '涨幅', '最新', '最高', '最低', '今开', '成交量', '成交额', '昨收', '持仓量', '市场', '时间']
    """
    return data_service.get_usd_cnh_futures_data()

@mcp.tool()
async def get_thirty_year_bond_futures_data() -> list:
    """ 获取三十年国债主连实时数据
    参数：
    - 无
    返回：
    - List[Dict]: 包含三十年国债主连的实时数据，
    Dict包含字段: ['代码', '名称', '涨幅', '最新', '最高', '最低', '今开', '成交量', '成交额', '昨收', '持仓量', '市场', '时间']
    """
    return data_service.get_thirty_year_bond_futures_data()

@mcp.tool()
async def get_economic_calendar(start_date: str = None, end_date: str = None, country: str = None) -> List[Dict]:
    """ 获取未来7天的全球经济报告日历
    参数：
    - start_date: 开始日期，格式为'YYYY-MM-DD'，默认为None（表示当前日期）
    - end_date: 结束日期，格式为'YYYY-MM-DD'，默认为None（表示当前日期+7天）
    - country: 国家代码，默认为None（表示所有国家）
    返回：
    - List[Dict]: 包含经济日历数据的字典列表，
    Dict包含字段: ['序号', '公布日', '时间', '国家/地区', '事件', '报告期', '公布值', '预测值', '前值', '重要性', '趋势']
    """
    return data_service.get_economic_calendar(start_date, end_date, country)

@mcp.tool()
async def get_stock_history_data(code: Union[list, str], freq: Union[int ,str] = "d", fqt: int=1, start_date: str = '19000101', end_date: str = None, indicator: bool = False) -> list:
    """ 获取指定股票代码的历史数据
    参数：
    - code: 股票代码或名称，可以是字符串也可以是列表，如"000001"、"600000"等
    - freq: 数据频率，时间频率，默认是日(d)，
      - 1 : 分钟；
      - 5 : 5 分钟；
      - 15 : 15 分钟；
      - 30 : 30 分钟；
      - 60 : 60 分钟；
      - 101或'D'或'd'：日；
      - 102或‘w’或'W'：周; 
      - 103或'm'或'M': 月
    - start_date: 开始日期，格式为"YYYY-MM-DD"，如"2023-01-01"，默认为'19000101'（取尽可能早的数据）
    - end_date: 结束日期，格式为"YYYY-MM-DD"，如"2023-12-31"，默认为None（取到最新数据）
    - fqt: 前复权方式，默认为1（前复权），0为不复权，2为后复权
    - indicator: 是否计算技术指标，默认为False，不计算
    返回：
    - List[Dict]: 包含股票历史数据，
    Dict包含字段: ['日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率']
    - 如果indicator为True，则还会包含技术指标数据，如BOLL、MACD等返回字段包含：
    Dict包含字段: ['日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率']
    额外的技术指标字段：
    BOLL: 布林指标，包含boll_upper, boll_middle, boll_lower
    OBV: 能量潮指标，包含obv
    RSI: 相对强弱指标，包含rsi, rsi_ma, rsi_gc, rsi_dc
    EXPMA: 指数平滑移动平均线，包含expma, expma_ma, expma_gc, expma_dc
    MACD: 平滑异同移动平均线，包含diff, dea, macd, macd_gc, macd_dc
    KDJ: 随机指标，包含kdj_k, kdj_d, kdj_j, kdj_gc, kdj_dc
    VolumeAnalyze: 量能分析，包含va5, va10, va_gc, va_dc
    SupportAnalyze: 支撑位分析，包含sp30_max, sp30_min, sp30, sp45_max, sp45_min, sp45, sp60_max, sp60_min, sp60, sp75_max, sp75_min, sp75, sp90_max, sp90_min, sp90, sp120_max, sp120_min, sp120
    技术字段中的gc表示金叉，dc表示死叉
    """
    return data_service.get_stock_history_data(code, freq, fqt, start_date, end_date, indicator)