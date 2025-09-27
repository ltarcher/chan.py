import json
from typing import List, Dict, Any, Optional, Union
import qstock as qs
import logging
import traceback
from agno.tools import Toolkit

# 导入所有需要的模块
from mcp_index import intitialize_data_service as index_initialize, data_service as index_data_service
from mcp_rzrq import initialize_data_service as rzrq_initialize, data_service as rzrq_data_service
from mcp_qh import initialize_data_service as qh_initialize, data_service as qh_data_service
from mcp_option import initialize_data_service as option_initialize, data_service as option_data_service
from mcp_option_qvix import initialize_data_service as qvix_initialize, data_service as qvix_data_service

logger = logging.getLogger('mcp-stock')

class QStockTools(Toolkit):
    """
    QStockTools是基于qstock库封装的金融数据工具集，用于获取A股、期货、期权等市场数据。
    
    Args:
        enable_all (bool): 是否启用所有工具.
    """

    def __init__(self, enable_all: bool = True, **kwargs: Any):
        """
        初始化QStockTools工具集
        """
        self.tools = []
        if enable_all:
            self.tools = [
                self.get_china_latest_trade_day,
                # 添加mcp_index.py中的工具
                self.get_index_realtime_data,
                self.get_board_trade_realtime_data,
                self.get_turnover_history_data,
                self.get_turnover_history_in_5_days,
                self.get_rzrq_turnover_ratio,
                self.get_usd_index_data,
                self.get_ftse_a50_futures_data,
                self.get_usd_cnh_futures_data,
                self.get_thirty_year_bond_futures_data,
                self.get_economic_calendar,
                self.get_stock_history_data,
                # 添加mcp_rzrq.py中的工具
                self.get_rzrq_industry_rank_mcp,
                self.get_rzrq_industry_detail_mcp,
                self.get_rzrq_history_mcp,
                self.get_rzrq_market_summary_mcp,
                self.get_rzrq_concept_rank_mcp,
                self.get_rzrq_account_data_mcp,
                # 添加mcp_qh.py中的工具
                self.get_futures_market_codes,
                self.get_future_org_list,
                self.get_futures_list,
                self.get_exchange_codes,
                self.get_exchange_products,
                self.get_qh_lhb_data,
                self.get_qh_lhb_rank,
                self.get_qh_ccjg_data,
                self.get_qh_ccjg_multi_market,
                self.get_qh_jcgc_data,
                self.get_qh_jcgc_history,
                self.get_qh_jcgc_summary,
                # 添加mcp_option.py中的工具
                self.get_option_target_list,
                self.get_option_realtime_data,
                self.get_option_value_data,
                self.get_option_risk_data,
                self.get_option_tboard_data,
                self.get_option_expire_all_data,
                self.get_option_expire_info_data,
                # 添加mcp_option_qvix.py中的工具
                self.get_50etf_qvix,
                self.get_50etf_min_qvix,
                self.get_300etf_qvix,
                self.get_300etf_min_qvix,
                self.get_500etf_qvix,
                self.get_500etf_min_qvix,
                self.get_cyb_qvix,
                self.get_cyb_min_qvix,
                self.get_kcb_qvix,
                self.get_kcb_min_qvix,
                self.get_100etf_qvix,
                self.get_100etf_min_qvix,
                self.get_300index_qvix,
                self.get_300index_min_qvix,
                self.get_1000index_qvix,
                self.get_1000index_min_qvix,
                self.get_50index_qvix,
                self.get_50index_min_qvix,
            ]
            super().__init__(name="qstock_tools", tools=self.tools, **kwargs)
        
        # 初始化所有数据服务
        index_initialize()
        rzrq_initialize()
        qh_initialize()
        option_initialize()
        qvix_initialize()

    def get_china_latest_trade_day(self) -> str:
        """
        获取A股最后一个交易日
        
        返回:
            str: 包含最后一个交易日信息的JSON字符串
        """
        try:
            # 使用qstock获取A股最后一个交易日
            latest_day = qs.latest_trade_date()
            
            logger.info(f"获取A股最后一个交易日: {latest_day}")
            
            # 构建返回结果
            result = {
                "trade_date": str(latest_day),
                "is_open": 1  # 默认为交易日
            }
            
            # 检查今天是否为交易日
            from datetime import datetime
            today = datetime.now().strftime('%Y-%m-%d')
            if latest_day != today:
                result["is_open"] = 0  # 如果最后交易日不是今天，则今天不是交易日
            
            return json.dumps(result, ensure_ascii=False)
        except Exception as e:
            logger.error(f"获取A股最后一个交易日失败: {str(e)}")
            traceback.print_exc()
            return json.dumps({"trade_date": "", "is_open": 0, "error": str(e)}, ensure_ascii=False)

    # 从mcp_index.py复制的工具方法
    def get_index_realtime_data(self, codes: Union[list, str], market: str = "沪深A") -> list:
        """ 获取中国金融市场指定指数或股票、期权、期货的实时数据
        参数：
        - codes: 指数或股票、期权、期货代码/名称，类型可以是字符串或列表；如果是字符串，多个代码用逗号分隔，为空时默认获取指定的市场指数
        - market: 市场类型，可选：["沪深A", "港股", "美股", "期货", "外汇", "债券"]，默认为沪深A
        返回：
        - List[Dict]: 包含指数或股票代码、名称、最新价、涨跌幅等，
        Dict包含字段:
        ['代码', '名称', '涨幅', '最新', '最高', '最低', '今开', '换手率', '量比', '市盈率', '成交量', '成交额', '昨收', '总市值', '流通市值', '市场', '时间']
        """
        return index_data_service.get_index_realtime_data(codes, market)

    def get_board_trade_realtime_data(self) -> Dict:
        """ 获取沪深京三市场的成交数据
        返回：
        - Dict: 包含日期和成交额的字典，格式为 {'日期': 日期, '总成交额': 总成交额'}
        """
        return index_data_service.get_board_trade_realtime_data()

    def get_turnover_history_data(self, start_date: Optional[str] = '19000101', end_date: Optional[str] = None, freq: Optional[str] = 'd', fqt: Optional[int] = 1, use_chinese_fields: bool = True) -> List[Dict]:
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
        return index_data_service.get_turnover_history_data(start_date=start_date, end_date=end_date, freq=freq, fqt=fqt, use_chinese_fields=use_chinese_fields)

    def get_turnover_history_in_5_days(self, start_date: Optional[str] = None, end_date: Optional[str] = None, use_chinese_fields: bool = True) -> List[Dict]:
        """ 获取沪深京三市场最近5个交易日的竞价总成交数据
        参数：
        - start_date: 开始日期，格式为'YYYY-MM-DD'，默认为None（取最近5个交易日）
        - end_date: 结束日期，格式为'YYYY-MM-DD'，默认为None（取到最新数据）
        - use_chinese_fields: 是否使用中文字段名，默认为True
        返回：
        - List[Dict]: 包含最近5个交易日的总成交数据，
        Dict包含字段: ['日期', '成交额'] 或 ['date', 'turnover']（取决于use_chinese_fields参数）
        """
        data = index_data_service.get_turnover_history_data(start_date=start_date, end_date=end_date, freq=1, fqt=1, use_chinese_fields=use_chinese_fields)
        # 遍历日期列只取当日包含9:30的数据,其他时间段的数据不取
        data = [d for d in data if '09:30:00' in d['日期']]
        return data

    def get_rzrq_turnover_ratio(self, start_date = None, end_date = None, page: int = 1, page_size: int = 10, use_chinese_fields: bool = True) -> List[Dict]:
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
        return index_data_service.get_rzrq_turnover_ratio(start_date, end_date, page, page_size, use_chinese_fields)

    def get_usd_index_data(self) -> list:
        """ 获取美元指数实时数据
        参数：
        - 无
        返回：
        - List[Dict]: 包含美元指数的实时数据，
        Dict包含字段:  ['代码', '名称', '涨幅', '最新', '最高', '最低', '今开', '换手率', '量比', '市盈率', '成交量', '成交额', '昨收', '总市值', '流通市值', '市场', '时间']
        """
        return index_data_service.get_usd_index_data()

    def get_ftse_a50_futures_data(self) -> list:
        """ 获取富时A50期货指数实时数据
        参数：
        - 无
        返回：
        - List[Dict]: 包含富时A50期货指数的实时数据，
        Dict包含字段: ['代码', '名称', '涨幅', '最新', '最高', '最低', '今开', '成交量', '成交额', '昨收', '持仓量', '市场', '时间']
        """
        return index_data_service.get_ftse_a50_futures_data()

    def get_usd_cnh_futures_data(self) -> list:
        """ 获取美元兑离岸人民币主连实时数据
        参数：
        - 无
        返回：
        - List[Dict]: 包含美元兑离岸人民币主连的实时数据，
        Dict包含字段: ['代码', '名称', '涨幅', '最新', '最高', '最低', '今开', '成交量', '成交额', '昨收', '持仓量', '市场', '时间']
        """
        return index_data_service.get_usd_cnh_futures_data()

    def get_thirty_year_bond_futures_data(self) -> list:
        """ 获取三十年国债主连实时数据
        参数：
        - 无
        返回：
        - List[Dict]: 包含三十年国债主连的实时数据，
        Dict包含字段: ['代码', '名称', '涨幅', '最新', '最高', '最低', '今开', '成交量', '成交额', '昨收', '持仓量', '市场', '时间']
        """
        return index_data_service.get_thirty_year_bond_futures_data()

    def get_economic_calendar(self, start_date: str = None, end_date: str = None, country: str = None) -> List[Dict]:
        """ 获取未来7天的全球经济报告日历
        参数：
        - start_date: 开始日期，格式为'YYYY-MM-DD'，默认为None（表示当前日期）
        - end_date: 结束日期，格式为'YYYY-MM-DD'，默认为None（表示当前日期+7天）
        - country: 国家代码，默认为None（表示所有国家）
        返回：
        - List[Dict]: 包含经济日历数据的字典列表，
        Dict包含字段: ['序号', '公布日', '时间', '国家/地区', '事件', '报告期', '公布值', '预测值', '前值', '重要性', '趋势']
        """
        return index_data_service.get_economic_calendar(start_date, end_date, country)

    def get_stock_history_data(self, code: Union[list, str], freq: str = "d", fqt: int=1, start_date: str = '19000101', end_date: str = None, indicator: bool = False) -> list:
        """ 获取指定股票代码的历史数据
        参数：
        - code: 股票代码或名称，可以是字符串也可以是列表
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
        return index_data_service.get_stock_history_data(code, freq, fqt, start_date, end_date, indicator)

    # 从mcp_rzrq.py复制的工具方法
    def get_rzrq_industry_rank_mcp(self, page: int = 1, page_size: int = 5, sort_column: str = "FIN_NETBUY_AMT", sort_type: int = -1, board_type_code: str = "006", use_chinese_fields: bool = True) -> List[Dict]:
        """
        获取东方财富网融资融券行业板块排行数据(MCP工具版本)
        
        参数:
            page: 页码，默认为1
            page_size: 每页数量，默认为5
            sort_column: 排序列，默认为"FIN_NETBUY_AMT"(融资净买入额)
            sort_type: 排序类型，1为升序，-1为降序，默认为-1
            board_type_code: 板块类型代码，默认为"006"(财富通行业)
            use_chinese_fields: 是否使用中文字段名，默认为True
        
        返回:
            List[Dict]: 包含融资融券行业板块排行数据的字典列表
        """
        return rzrq_data_service.get_rzrq_industry_rank(
            page=page,
            page_size=page_size,
            sort_column=sort_column,
            sort_type=sort_type,
            board_type_code=board_type_code,
            use_chinese_fields=use_chinese_fields
        )

    def get_rzrq_industry_detail_mcp(self, board_code: str, start_date: Optional[str] = None, end_date: Optional[str] = None, page: int = 1, page_size: int = 20, use_chinese_fields: bool = True) -> List[Dict]:
        """
        获取特定行业板块的融资融券明细数据(MCP工具版本)
        
        参数:
            board_code: 板块代码
            start_date: 开始日期，格式为'YYYY-MM-DD'，默认为None
            end_date: 结束日期，格式为'YYYY-MM-DD'，默认为None
            page: 页码，默认为1
            page_size: 每页数量，默认为20
            use_chinese_fields: 是否使用中文字段名，默认为True
        
        返回:
            List[Dict]: 包含特定行业板块融资融券明细数据的字典列表
        """
        return rzrq_data_service.get_rzrq_industry_detail(
            board_code=board_code,
            start_date=start_date,
            end_date=end_date,
            page=page,
            page_size=page_size,
            use_chinese_fields=use_chinese_fields
        )

    def get_rzrq_history_mcp(self, start_date: Optional[str] = None, end_date: Optional[str] = None, page: int = 1, page_size: int = 10, use_chinese_fields: bool = True) -> List[Dict]:
        """
        获取融资融券交易历史明细数据(沪深北三市场)(MCP工具版本)
        
        参数:
            start_date: 开始日期，格式为'YYYY-MM-DD'，默认为None
            end_date: 结束日期，格式为'YYYY-MM-DD'，默认为None
            page: 页码，默认为1
            page_size: 每页数量，默认为10
            use_chinese_fields: 是否使用中文字段名，默认为True
        
        返回:
            List[Dict]: 包含融资融券交易历史明细数据的字典列表
        """
        return rzrq_data_service.get_rzrq_history(
            start_date=start_date,
            end_date=end_date,
            page=page,
            page_size=page_size,
            use_chinese_fields=use_chinese_fields
        )

    def get_rzrq_market_summary_mcp(self, start_date: Optional[str] = None, end_date: Optional[str] = None, page: int = 1, page_size: int = 10, use_chinese_fields: bool = True) -> List[Dict]:
        """
        获取市场融资融券交易总量数据(含上证指数和融资融券汇总数据)(MCP工具版本)
        
        参数:
            start_date: 开始日期，格式为'YYYY-MM-DD'，默认为None
            end_date: 结束日期，格式为'YYYY-MM-DD'，默认为None
            page: 页码，默认为1
            page_size: 每页数量，默认为10
            use_chinese_fields: 是否使用中文字段名，默认为True
        
        返回:
            List[Dict]: 包含市场融资融券交易总量数据的字典列表
        """
        return rzrq_data_service.get_rzrq_market_summary(
            start_date=start_date,
            end_date=end_date,
            page=page,
            page_size=page_size,
            use_chinese_fields=use_chinese_fields
        )

    def get_rzrq_concept_rank_mcp(self, page: int = 1, page_size: int = 50, sort_column: str = "FIN_NETBUY_AMT", sort_type: int = -1, use_chinese_fields: bool = True) -> List[Dict]:
        """
        获取东方财富网融资融券概念板块排行数据(MCP工具版本)
        
        参数:
            page: 页码，默认为1
            page_size: 每页数量，默认为50
            sort_column: 排序列，默认为"FIN_NETBUY_AMT"(融资净买入额)
            sort_type: 排序类型，1为升序，-1为降序，默认为-1
            use_chinese_fields: 是否使用中文字段名，默认为True
        
        返回:
            List[Dict]: 包含融资融券概念板块排行数据的字典列表
        """
        return rzrq_data_service.get_rzrq_concept_rank(
            page=page,
            page_size=page_size,
            sort_column=sort_column,
            sort_type=sort_type,
            use_chinese_fields=use_chinese_fields
        )

    def get_rzrq_account_data_mcp(self, page: int = 1, page_size: int = 50, sort_column: str = "STATISTICS_DATE", sort_type: int = -1, start_date: Optional[str] = None, end_date: Optional[str] = None, use_chinese_fields: bool = True) -> List[Dict]:
        """
        获取两融账户信息数据(MCP工具版本)
        
        参数:
            page: 页码，默认为1
            page_size: 每页数量，默认为50
            sort_column: 排序列，默认为"STATISTICS_DATE"(统计日期)
            sort_type: 排序类型，1为升序，-1为降序，默认为-1
            start_date: 开始日期，格式为'YYYY-MM-DD'，默认为None
            end_date: 结束日期，格式为'YYYY-MM-DD'，默认为None
            use_chinese_fields: 是否使用中文字段名，默认为True
        
        返回:
            List[Dict]: 包含两融账户信息数据的字典列表
        """
        return rzrq_data_service.get_rzrq_account_data(
            page=page,
            page_size=page_size,
            sort_column=sort_column,
            sort_type=sort_type,
            start_date=start_date,
            end_date=end_date,
            use_chinese_fields=use_chinese_fields
        )

    # 从mcp_qh.py复制的工具方法
    def get_futures_market_codes(self) -> Dict[str, str]:
        """
        获取所有期货交易市场代码
        
        返回:
            Dict[str, str]: 市场名称和代码的映射字典
        """
        try:
            logger.info("调用获取期货交易市场代码接口")
            return qh_data_service.futures_list.get_market_codes()
        except Exception as e:
            logger.error(f"获取交易市场代码失败: {e}")
            traceback.print_exc()
            return {"错误": str(e)}

    def get_future_org_list(self, page_size: int = 200, use_chinese_fields: bool = True) -> List[Dict]:
        """
        获取期货公司列表数据
        
        参数:
            page_size: 每页数据量，默认为200
            use_chinese_fields: 是否使用中文字段名，默认为True
                
        返回:
            List[Dict]: 包含期货公司数据的字典列表
        """
        try:
            logger.info("调用获取期货公司列表数据接口")
            return qh_data_service.get_future_org_list(
                page_size=page_size,
                use_chinese_fields=use_chinese_fields
            )
        except Exception as e:
            logger.error(f"获取期货公司列表数据失败: {e}")
            traceback.print_exc()
            return []

    def get_futures_list(self, is_main_code: bool = True, use_chinese_fields: bool = True) -> List[Dict]:
        """
        获取期货品种列表数据
        
        参数:
            is_main_code: 是否只获取主力合约，默认为True
            use_chinese_fields: 是否使用中文字段名，默认为True
                
        返回:
            List[Dict]: 包含期货品种数据的字典列表
        """
        try:
            logger.info("调用获取期货品种列表数据接口")
            return qh_data_service.get_futures_list(
                is_main_code=is_main_code,
                use_chinese_fields=use_chinese_fields
            )
        except Exception as e:
            logger.error(f"获取期货品种列表数据失败: {e}")
            traceback.print_exc()
            return []

    def get_exchange_codes(self) -> Dict[str, str]:
        """
        获取交易所编码映射(用于获取交易所名称和编码的映射或查询交易所可交易品种列表)
        
        返回:
            Dict[str, str]: 交易所名称和编码的映射字典
        """
        try:
            logger.info("获取交易所编码映射")
            return qh_data_service.futures_list.EXCHANGE_MSGID.copy()
        except Exception as e:
            logger.error(f"获取交易所编码失败: {e}")
            traceback.print_exc()
            return {"错误": str(e)}

    def get_exchange_products(self, exchange_name: str, use_chinese_fields: bool = True) -> List[Dict]:
        """
        获取交易所可交易品种列表
        
        参数:
            exchange_name: 交易所名称(如上期所、中金所等)
            use_chinese_fields: 是否使用中文字段名，默认为True
            
        返回:
            List[Dict]: 包含交易所品种数据的字典列表
        """
        try:
            logger.info(f"获取交易所品种数据，交易所: {exchange_name}")
            
            # 获取交易所编码
            msgid = qh_data_service.futures_list.EXCHANGE_MSGID.get(exchange_name)
            if not msgid:
                raise ValueError(f"不支持的交易所名称: {exchange_name}")
                
            # 获取品种数据
            return qh_data_service.get_exchange_products(
                msgid=msgid,
                use_chinese_fields=use_chinese_fields
            )
        except ValueError as e:
            logger.error(f"参数错误: {str(e)}")
            traceback.print_exc()
            return []
        except Exception as e:
            logger.error(f"获取交易所品种数据失败: {str(e)}")
            traceback.print_exc()
            return []

    def get_qh_lhb_data(self, security_code: str, trade_date: str, cookies: Optional[Dict[str, str]] = None, use_chinese_fields: bool = True) -> List[Dict]:
        """
        获取期货龙虎榜数据
        
        参数:
            security_code: 合约代码(如IF2509)
            trade_date: 交易日期(YYYY-MM-DD)
            cookies: 请求cookies字典
            use_chinese_fields: 是否使用中文字段名，默认为True
            
        返回:
            List[Dict]: 包含龙虎榜数据的字典列表
        """
        try:
            logger.info(f"获取期货龙虎榜数据，合约: {security_code}, 日期: {trade_date}")
            return qh_data_service.get_qh_lhb_data(
                security_code=security_code,
                trade_date=trade_date,
                cookies=cookies,
                use_chinese_fields=use_chinese_fields
            )
        except Exception as e:
            logger.error(f"获取期货龙虎榜数据失败: {str(e)}")
            traceback.print_exc()
            return []

    def get_qh_lhb_rank(self, security_code: str, trade_date: str, rank_field: str, cookies: Optional[Dict[str, str]] = None, use_chinese_fields: bool = True) -> List[Dict]:
        """
        获取特定排名的期货龙虎榜数据
        
        参数:
            security_code: 合约代码(如IF2509)
            trade_date: 交易日期(YYYY-MM-DD)
            rank_field: 排名字段(如VOLUMERANK, LPRANK等)
            cookies: 请求cookies字典
            use_chinese_fields: 是否使用中文字段名，默认为True
            
        返回:
            List[Dict]: 包含特定排名龙虎榜数据的字典列表
        """
        try:
            logger.info(f"获取期货{rank_field}排名数据，合约: {security_code}, 日期: {trade_date}")
            return qh_data_service.get_qh_lhb_rank(
                security_code=security_code,
                trade_date=trade_date,
                rank_field=rank_field,
                cookies=cookies,
                use_chinese_fields=use_chinese_fields
            )
        except Exception as e:
            logger.error(f"获取期货排名数据失败: {str(e)}")
            traceback.print_exc()
            return []

    def get_qh_ccjg_data(self, org_code: str, trade_date: str, market_name: str, cookies: Optional[Dict[str, str]] = None, use_chinese_fields: bool = True) -> List[Dict]:
        """
        获取期货公司持仓结构数据
        
        参数:
            org_code: 机构代码
            trade_date: 交易日期(YYYY-MM-DD)
            market_name: 市场名称(如上期所、中金所等)
            cookies: 请求cookies字典
            use_chinese_fields: 是否使用中文字段名，默认为True
            
        返回:
            List[Dict]: 包含持仓结构数据的字典列表
        """
        try:
            logger.info(f"获取期货公司持仓结构数据，机构: {org_code}, 日期: {trade_date}, 市场: {market_name}")
            return qh_data_service.get_qh_ccjg_data(
                org_code=org_code,
                trade_date=trade_date,
                market_name=market_name,
                cookies=cookies,
                use_chinese_fields=use_chinese_fields
            )
        except Exception as e:
            logger.error(f"获取期货公司持仓结构数据失败: {str(e)}")
            traceback.print_exc()
            return []

    def get_qh_ccjg_multi_market(self, org_code: str, trade_date: str, markets: List[str], cookies: Optional[Dict[str, str]] = None, use_chinese_fields: bool = True) -> List[Dict]:
        """
        获取期货公司在多个市场的持仓数据
        
        参数:
            org_code: 机构代码
            trade_date: 交易日期(YYYY-MM-DD)
            markets: 市场名称列表(如上期所、中金所等)
            cookies: 请求cookies字典
            use_chinese_fields: 是否使用中文字段名，默认为True
            
        返回:
            List[Dict]: 包含多市场持仓数据的字典列表
        """
        try:
            logger.info(f"获取多市场持仓数据，机构: {org_code}, 日期: {trade_date}, 市场: {markets}")
            return qh_data_service.get_qh_ccjg_multi_market(
                org_code=org_code,
                trade_date=trade_date,
                markets=markets,
                cookies=cookies,
                use_chinese_fields=use_chinese_fields
            )
        except Exception as e:
            logger.error(f"获取多市场持仓数据失败: {str(e)}")
            traceback.print_exc()
            return []

    def get_qh_jcgc_data(self, security_code: str, org_code: str, start_date: Optional[str] = None, end_date: Optional[str] = None, cookies: Optional[Dict[str, str]] = None, use_chinese_fields: bool = True) -> List[Dict]:
        """
        获取期货建仓过程数据
        
        参数:
            security_code: 合约代码(如IF2507)
            org_code: 机构代码
            start_date: 开始日期(YYYY-MM-DD)，可选
            end_date: 结束日期(YYYY-MM-DD)，可选
            cookies: 请求cookies字典
            use_chinese_fields: 是否使用中文字段名，默认为True
            
        返回:
            List[Dict]: 包含建仓过程数据的字典列表
        """
        try:
            logger.info(f"获取建仓过程数据，合约: {security_code}, 机构: {org_code}")
            return qh_data_service.get_qh_jcgc_data(
                security_code=security_code,
                org_code=org_code,
                start_date=start_date,
                end_date=end_date,
                cookies=cookies,
                use_chinese_fields=use_chinese_fields
            )
        except Exception as e:
            logger.error(f"获取建仓过程数据失败: {str(e)}")
            traceback.print_exc()
            return []

    def get_qh_jcgc_history(self, security_code: str, org_code: str, days: int = 30, end_date: Optional[str] = None, cookies: Optional[Dict[str, str]] = None, use_chinese_fields: bool = True) -> List[Dict]:
        """
        获取指定天数的持仓历史数据
        
        参数:
            security_code: 合约代码(如IF2507)
            org_code: 机构代码
            days: 获取的天数，默认30天
            end_date: 结束日期(YYYY-MM-DD)，可选
            cookies: 请求cookies字典
            use_chinese_fields: 是否使用中文字段名，默认为True
            
        返回:
            List[Dict]: 包含持仓历史数据的字典列表
        """
        try:
            logger.info(f"获取持仓历史数据，合约: {security_code}, 机构: {org_code}, 天数: {days}")
            return qh_data_service.get_qh_jcgc_history(
                security_code=security_code,
                org_code=org_code,
                days=days,
                end_date=end_date,
                cookies=cookies,
                use_chinese_fields=use_chinese_fields
            )
        except Exception as e:
            logger.error(f"获取持仓历史数据失败: {str(e)}")
            traceback.print_exc()
            return []

    def get_qh_jcgc_summary(self, security_code: str, org_code: str, start_date: Optional[str] = None, end_date: Optional[str] = None, cookies: Optional[Dict[str, str]] = None) -> Dict[str, Dict[str, float]]:
        """
        获取持仓数据摘要
        
        参数:
            security_code: 合约代码(如IF2507)
            org_code: 机构代码
            start_date: 开始日期(YYYY-MM-DD)，可选
            end_date: 结束日期(YYYY-MM-DD)，可选
            cookies: 请求cookies字典
            
        返回:
            Dict: 包含持仓数据摘要的字典
        """
        try:
            logger.info(f"获取持仓数据摘要，合约: {security_code}, 机构: {org_code}")
            jcgc = qh_data_service.futures_list  # 这里应该使用FuturesJCGC实例，但为了保持与原代码一致，暂时这样处理
            df = jcgc.get_data(
                security_code=security_code,
                org_code=org_code,
                start_date=start_date,
                end_date=end_date,
                use_chinese_fields=True  # 摘要使用中文字段名
            )
            
            if df.empty:
                logger.warning("返回数据为空")
                return {}
                
            return jcgc.get_position_summary(df)
        except Exception as e:
            logger.error(f"获取持仓数据摘要失败: {str(e)}")
            traceback.print_exc()
            return {}

    # 从mcp_option.py复制的工具方法
    def get_option_target_list(self) -> list:
        '''获取中国金融市场期权标的列表
        参数：
        - 无
        返回：
        - List[Dict]: 包含期权标的代码(code)、名称(name)、市场(market)等，
        Dict包含字段: ['market', 'code', 'name']
        '''
        try:
            return option_data_service.get_option_target_list()
        except Exception as e:
            logger.error(f"获取期权标的数据失败: {str(e)}")
            traceback.print_exc()

    def get_option_realtime_data(self, codes: Union[list, str], market: str = "期权") -> list:
        """ 获取中国金融市场多个期权的实时数据
        参数：
        - codes: 期权代码/名称，多个代码用逗号分隔，为空时默认获取指定的市场期权
        - market: 市场类型，默认为期权
        返回：
        - List[Dict]: 包含期权代码、名称、最新价、涨跌幅等，
        Dict包含字段: ['代码', '名称', '涨幅', '最新价', '成交量', '成交额', '今开', '昨结', '持仓量', '行权价', '剩余日', '日增']
        """
        if isinstance(codes, str):
            if not codes:
                option_codes = []  # 空字符串情况下，使用空列表
            else:
                option_codes = codes.split(',')  # 将逗号分隔的字符串转换为列表
        elif isinstance(codes, list):
            option_codes = codes
        else:
            logger.error("参数 'codes' 必须是字符串或列表")
            return []
        
        try:
            return option_data_service.get_option_realtime_data(codes=option_codes, market=market)
        except Exception as e:
            logger.error(f"获取期权实时数据失败: {str(e)}")
            traceback.print_exc()
        return []

    def get_option_value_data(self, codes: Union[list, str], market: str = "期权") -> list:
        """ 获取中国金融市场多个期权的价值数据
        参数：
        - codes: 期权代码/名称，多个代码用逗号分隔，为空时默认获取指定的市场期权
        - market: 市场类型，默认为期权
        返回：
        - List[Dict]: 包含期权代码、名称、隐含波动率、时间价值、内在价值、理论价格等，
        Dict包含字段: ['代码', '名称', '涨幅', '最新价', '隐含波动率', '时间价值', '内在价值', '理论价格', '到期日', 
                    '标的代码', '标的名称', '标的最新价', '标的涨幅', '标的近1年波动率']
        """
        if isinstance(codes, str):
            if not codes:
                option_codes = []  # 空字符串情况下，使用空列表
            else:
                option_codes = codes.split(',')  # 将逗号分隔的字符串转换为列表
        elif isinstance(codes, list):
            option_codes = codes
        else:
            logger.error("参数 'codes' 必须是字符串或列表")
            return []
        
        try:
            return option_data_service.get_option_value_data(codes=option_codes, market=market)
        except Exception as e:
            logger.error(f"获取期权价值数据失败: {str(e)}")
            traceback.print_exc()
        return []

    def get_option_risk_data(self, codes: Union[list, str], market: str = "期权风险") -> list:
        """ 获取中国金融市场多个期权的风险数据
        参数：
        - codes: 期权代码/名称，多个代码用逗号分隔，为空时默认获取指定的市场期权
        - market: 市场类型，默认为期权风险
        返回：
        - List[Dict]: 包含期权代码、名称、Delta、Gamma、Vega、Theta、Rho等希腊字母指标，
        Dict包含字段: ['代码', '名称', '涨幅', '最新价', '杠杆比率', '实际杠杆比率', 'Delta', 'Gamma', 'Vega', 'Rho', 'Theta', '到期日']
        """
        if isinstance(codes, str):
            if not codes:
                option_codes = []  # 空字符串情况下，使用空列表
            else:
                option_codes = codes.split(',')  # 将逗号分隔的字符串转换为列表
        elif isinstance(codes, list):
            option_codes = codes
        else:
            logger.error("参数 'codes' 必须是字符串或列表")
            return []
        
        try:
            return option_data_service.get_option_risk_data(codes=option_codes, market=market)
        except Exception as e:
            logger.error(f"获取期权风险数据失败: {str(e)}")
            traceback.print_exc()
        return []

    def get_option_tboard_data(self, expire_month: str = None) -> list:
        """ 获取期权T型看板数据
        参数：
        - expire_month: 到期月份，格式为YYYYMM，如202312。为空时获取所有月份的T型看板数据
        返回：
        - List[Dict]: 包含期权T型看板数据，
        Dict包含字段: ['购代码', '购名称', '购最新价', '购涨跌额', '标的最新价', '购涨跌幅', '购持仓量', '购成交量', 
                    '购隐含波动率', '购折溢价率', '行权价', '沽名称', '沽代码', '沽最新价', '沽涨跌额', '沽涨跌幅', 
                    '沽持仓量', '沽成交量', '沽隐含波动率', '沽折溢价率', '时间', '到期日']
        """
        try:
            return option_data_service.get_option_tboard_data(expire_month=expire_month)
        except Exception as e:
            logger.error(f"获取期权T型看板数据失败: {str(e)}")
            traceback.print_exc()
        return []

    def get_option_expire_all_data(self) -> list:
        """ 获取所有期权标的的到期日信息
        参数：
        - 无
        返回：
        - List[Dict]: 包含所有期权标的的到期日信息，
        Dict包含字段: ['到期日', '剩余日', '市场', '代码']
        """
        try:
            return option_data_service.get_option_expire_all_data()
        except Exception as e:
            logger.error(f"获取所有期权标的的到期日信息失败: {str(e)}")
            traceback.print_exc()
        return []

    def get_option_expire_info_data(self, code: str, market: int = 0) -> list:
        """ 获取指定期权标的代码的到期日信息
        参数：
        - code: 期权标的代码，如510050（50ETF）
        - market: 市场类型，0表示深交所，1表示上交所，默认为0
        返回：
        - List[Dict]: 包含指定期权代码的到期日信息，
        Dict包含字段: ['到期日', '剩余日', '市场', '代码']
        """
        # 参数验证
        if not code:
            logger.error("参数 'code' 不能为空")
            return []
        
        if market not in [0, 1]:
            logger.warning(f"参数 'market' 必须是0或1，当前值为: {market}，将使用默认值0")
            market = 0
        
        try:
            return option_data_service.get_option_expire_info_data(code=code, market=market)
        except Exception as e:
            logger.error(f"获取期权标的代码 {code} 的到期日信息失败: {str(e)}")
            traceback.print_exc()
        return []

    # 从mcp_option_qvix.py复制的工具方法
    def get_50etf_qvix(self) -> list:
        """ 获取50ETF期权波动率指数QVIX日线数据
        参数：
        - 无
        返回：
        - List[Dict]: 包含50ETF期权波动率指数QVIX日线数据，
        Dict包含字段: ['date', 'open', 'high', 'low', 'close']
        """
        try:
            return qvix_data_service.get_50etf_qvix()
        except Exception as e:
            logger.error(f"获取50ETF期权波动率指数QVIX日线数据失败: {str(e)}")
            traceback.print_exc()
            return []

    def get_50etf_min_qvix(self) -> list:
        """ 获取50ETF期权波动率指数QVIX分钟线数据
        参数：
        - 无
        返回：
        - List[Dict]: 包含50ETF期权波动率指数QVIX分钟线数据，
        Dict包含字段: ['time', 'qvix']
        """
        try:
            return qvix_data_service.get_50etf_min_qvix()
        except Exception as e:
            logger.error(f"获取50ETF期权波动率指数QVIX分钟线数据失败: {str(e)}")
            traceback.print_exc()
            return []

    def get_300etf_qvix(self) -> list:
        """ 获取300ETF期权波动率指数QVIX日线数据
        参数：
        - 无
        返回：
        - List[Dict]: 包含300ETF期权波动率指数QVIX日线数据，
        Dict包含字段: ['date', 'open', 'high', 'low', 'close']
        """
        try:
            return qvix_data_service.get_300etf_qvix()
        except Exception as e:
            logger.error(f"获取300ETF期权波动率指数QVIX日线数据失败: {str(e)}")
            traceback.print_exc()
            return []

    def get_300etf_min_qvix(self) -> list:
        """ 获取300ETF期权波动率指数QVIX分钟线数据
        参数：
        - 无
        返回：
        - List[Dict]: 包含300ETF期权波动率指数QVIX分钟线数据，
        Dict包含字段: ['time', 'qvix']
        """
        try:
            return qvix_data_service.get_300etf_min_qvix()
        except Exception as e:
            logger.error(f"获取300ETF期权波动率指数QVIX分钟线数据失败: {str(e)}")
            traceback.print_exc()
            return []

    def get_500etf_qvix(self) -> list:
        """ 获取500ETF期权波动率指数QVIX日线数据
        参数：
        - 无
        返回：
        - List[Dict]: 包含500ETF期权波动率指数QVIX日线数据，
        Dict包含字段: ['date', 'open', 'high', 'low', 'close']
        """
        try:
            return qvix_data_service.get_500etf_qvix()
        except Exception as e:
            logger.error(f"获取500ETF期权波动率指数QVIX日线数据失败: {str(e)}")
            traceback.print_exc()
            return []

    def get_500etf_min_qvix(self) -> list:
        """ 获取500ETF期权波动率指数QVIX分钟线数据
        参数：
        - 无
        返回：
        - List[Dict]: 包含500ETF期权波动率指数QVIX分钟线数据，
        Dict包含字段: ['time', 'qvix']
        """
        try:
            return qvix_data_service.get_500etf_min_qvix()
        except Exception as e:
            logger.error(f"获取500ETF期权波动率指数QVIX分钟线数据失败: {str(e)}")
            traceback.print_exc()
            return []

    def get_cyb_qvix(self) -> list:
        """ 获取创业板期权波动率指数QVIX日线数据
        参数：
        - 无
        返回：
        - List[Dict]: 包含创业板期权波动率指数QVIX日线数据，
        Dict包含字段: ['date', 'open', 'high', 'low', 'close']
        """
        try:
            return qvix_data_service.get_cyb_qvix()
        except Exception as e:
            logger.error(f"获取创业板期权波动率指数QVIX日线数据失败: {str(e)}")
            traceback.print_exc()
            return []

    def get_cyb_min_qvix(self) -> list:
        """ 获取创业板期权波动率指数QVIX分钟线数据
        参数：
        - 无
        返回：
        - List[Dict]: 包含创业板期权波动率指数QVIX分钟线数据，
        Dict包含字段: ['time', 'qvix']
        """
        try:
            return qvix_data_service.get_cyb_min_qvix()
        except Exception as e:
            logger.error(f"获取创业板期权波动率指数QVIX分钟线数据失败: {str(e)}")
            traceback.print_exc()
            return []

    def get_kcb_qvix(self) -> list:
        """ 获取科创板期权波动率指数QVIX日线数据
        参数：
        - 无
        返回：
        - List[Dict]: 包含科创板期权波动率指数QVIX日线数据，
        Dict包含字段: ['date', 'open', 'high', 'low', 'close']
        """
        try:
            return qvix_data_service.get_kcb_qvix()
        except Exception as e:
            logger.error(f"获取科创板期权波动率指数QVIX日线数据失败: {str(e)}")
            traceback.print_exc()
            return []

    def get_kcb_min_qvix(self) -> list:
        """ 获取科创板期权波动率指数QVIX分钟线数据
        参数：
        - 无
        返回：
        - List[Dict]: 包含科创板期权波动率指数QVIX分钟线数据，
        Dict包含字段: ['time', 'qvix']
        """
        try:
            return qvix_data_service.get_kcb_min_qvix()
        except Exception as e:
            logger.error(f"获取科创板期权波动率指数QVIX分钟线数据失败: {str(e)}")
            traceback.print_exc()
            return []

    def get_100etf_qvix(self) -> list:
        """ 获取深证100ETF期权波动率指数QVIX日线数据
        参数：
        - 无
        返回：
        - List[Dict]: 包含深证100ETF期权波动率指数QVIX日线数据，
        Dict包含字段: ['date', 'open', 'high', 'low', 'close']
        """
        try:
            return qvix_data_service.get_100etf_qvix()
        except Exception as e:
            logger.error(f"获取深证100ETF期权波动率指数QVIX日线数据失败: {str(e)}")
            traceback.print_exc()
            return []

    def get_100etf_min_qvix(self) -> list:
        """ 获取深证100ETF期权波动率指数QVIX分钟线数据
        参数：
        - 无
        返回：
        - List[Dict]: 包含深证100ETF期权波动率指数QVIX分钟线数据，
        Dict包含字段: ['time', 'qvix']
        """
        try:
            return qvix_data_service.get_100etf_min_qvix()
        except Exception as e:
            logger.error(f"获取深证100ETF期权波动率指数QVIX分钟线数据失败: {str(e)}")
            traceback.print_exc()
            return []

    def get_300index_qvix(self) -> list:
        """ 获取中证300股指期权波动率指数QVIX日线数据
        参数：
        - 无
        返回：
        - List[Dict]: 包含中证300股指期权波动率指数QVIX日线数据，
        Dict包含字段: ['date', 'open', 'high', 'low', 'close']
        """
        try:
            return qvix_data_service.get_300index_qvix()
        except Exception as e:
            logger.error(f"获取中证300股指期权波动率指数QVIX日线数据失败: {str(e)}")
            traceback.print_exc()
            return []

    def get_300index_min_qvix(self) -> list:
        """ 获取中证300股指期权波动率指数QVIX分钟线数据
        参数：
        - 无
        返回：
        - List[Dict]: 包含中证300股指期权波动率指数QVIX分钟线数据，
        Dict包含字段: ['time', 'qvix']
        """
        try:
            return qvix_data_service.get_300index_min_qvix()
        except Exception as e:
            logger.error(f"获取中证300股指期权波动率指数QVIX分钟线数据失败: {str(e)}")
            traceback.print_exc()
            return []

    def get_1000index_qvix(self) -> list:
        """ 获取中证1000股指期权波动率指数QVIX日线数据
        参数：
        - 无
        返回：
        - List[Dict]: 包含中证1000股指期权波动率指数QVIX日线数据，
        Dict包含字段: ['date', 'open', 'high', 'low', 'close']
        """
        try:
            return qvix_data_service.get_1000index_qvix()
        except Exception as e:
            logger.error(f"获取中证1000股指期权波动率指数QVIX日线数据失败: {str(e)}")
            traceback.print_exc()
            return []

    def get_1000index_min_qvix(self) -> list:
        """ 获取中证1000股指期权波动率指数QVIX分钟线数据
        参数：
        - 无
        返回：
        - List[Dict]: 包含中证1000股指期权波动率指数QVIX分钟线数据，
        Dict包含字段: ['time', 'qvix']
        """
        try:
            return qvix_data_service.get_1000index_min_qvix()
        except Exception as e:
            logger.error(f"获取中证1000股指期权波动率指数QVIX分钟线数据失败: {str(e)}")
            traceback.print_exc()
            return []

    def get_50index_qvix(self) -> list:
        """ 获取上证50股指期权波动率指数QVIX日线数据
        参数：
        - 无
        返回：
        - List[Dict]: 包含上证50股指期权波动率指数QVIX日线数据，
        Dict包含字段: ['date', 'open', 'high', 'low', 'close']
        """
        try:
            return qvix_data_service.get_50index_qvix()
        except Exception as e:
            logger.error(f"获取上证50股指期权波动率指数QVIX日线数据失败: {str(e)}")
            traceback.print_exc()
            return []

    def get_50index_min_qvix(self) -> list:
        """ 获取上证50股指期权波动率指数QVIX分钟线数据
        参数：
        - 无
        返回：
        - List[Dict]: 包含上证50股指期权波动率指数QVIX分钟线数据，
        Dict包含字段: ['time', 'qvix']
        """
        try:
            return qvix_data_service.get_50index_min_qvix()
        except Exception as e:
            logger.error(f"获取上证50股指期权波动率指数QVIX分钟线数据失败: {str(e)}")
            traceback.print_exc()
            return []