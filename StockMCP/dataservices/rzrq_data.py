import logging
from typing import Dict, List, Optional, Any
import pandas as pd
from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from .data_service import DataService, adjust_start_date, adjust_end_date
from stock_rzrq import (
    get_rzrq_industry_rank,
    get_rzrq_industry_detail,
    get_rzrq_history,
    get_rzrq_market_summary,
    get_rzrq_concept_rank,
    get_rzrq_account_data
)

logger = logging.getLogger(__name__)

class RzrqDataService(DataService):
    """融资融券数据类，封装融资融券相关数据获取逻辑"""
    def __init__(self):
        super().__init__()
        
    def get_rzrq_industry_rank(self,
        page: int = 1,
        page_size: int = 5,
        sort_column: str = "FIN_NETBUY_AMT",
        sort_type: int = -1,
        board_type_code: str = "006",
        use_chinese_fields: bool = True
    ) -> List[Dict]:
        """ 获取东方财富网融资融券行业板块排行数据(带缓存支持)
        
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
        # 生成缓存键
        cache_key = f"rzrq_industry_rank:{sort_column}:{sort_type}:{board_type_code}:{use_chinese_fields}"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取融资融券行业板块排行数据: {cache_key}")
            # 检查是否需要更新（基于交易日变化）
            if self._need_incremental_update(cached_data):
                logger.info("检测到需要更新融资融券行业板块排行数据")
                # 获取最新数据
                new_data = self._fetch_rzrq_industry_rank(
                    page=page,
                    page_size=page_size,
                    sort_column=sort_column,
                    sort_type=sort_type,
                    board_type_code=board_type_code,
                    use_chinese_fields=use_chinese_fields
                )
                if new_data:
                    # 更新缓存
                    self.set_cached_data(cache_key, new_data, expiry=3600)
                    logger.info("已更新融资融券行业板块排行数据缓存")
                    return new_data
                else:
                    # 如果获取新数据失败，返回缓存数据
                    logger.warning("获取最新数据失败，返回缓存数据")
                    return cached_data
            else:
                return cached_data

        try:
            # 获取实时数据
            result = self._fetch_rzrq_industry_rank(
                page=page,
                page_size=page_size,
                sort_column=sort_column,
                sort_type=sort_type,
                board_type_code=board_type_code,
                use_chinese_fields=use_chinese_fields
            )
            
            # 缓存数据1小时
            if result:
                self.set_cached_data(cache_key, result, expiry=3600)
            return result
        except Exception as e:
            logger.error(f"获取融资融券行业板块排行数据时发生错误：{str(e)}")
            return []

    def _fetch_rzrq_industry_rank(self,
        page: int = 1,
        page_size: int = 5,
        sort_column: str = "FIN_NETBUY_AMT",
        sort_type: int = -1,
        board_type_code: str = "006",
        use_chinese_fields: bool = True
    ) -> List[Dict]:
        """ 获取融资融券行业板块排行数据的实际实现 """
        try:
            # 获取实时数据
            data = get_rzrq_industry_rank(
                page=page,
                page_size=page_size,
                sort_column=sort_column,
                sort_type=sort_type,
                board_type_code=board_type_code,
                use_chinese_fields=use_chinese_fields
            )
            
            result = data.get('result', {}).get('data', []) if data else []
            return result
        except Exception as e:
            logger.error(f"获取融资融券行业板块排行数据时发生错误：{str(e)}")
            return []

    def get_rzrq_industry_detail(self,
        board_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
        use_chinese_fields: bool = True
    ) -> List[Dict]:
        """ 获取特定行业板块的融资融券明细数据(带缓存支持)
        
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
        # 生成缓存键
        cache_key = f"rzrq_industry_detail:{board_code}:{use_chinese_fields}"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取板块{board_code}的融资融券明细数据: {cache_key}")
            # 检查是否需要增量更新
            if self._need_incremental_update(cached_data, start_date=start_date, end_date=end_date):
                logger.info("检测到需要增量更新融资融券行业明细数据")
                incremental_data = self._get_incremental_rzrq_industry_detail(
                    cached_data, board_code, start_date, end_date, page, page_size, use_chinese_fields)
                if incremental_data:
                    # 合并数据
                    merged_data = self._merge_and_deduplicate_data(cached_data, incremental_data)
                    # 更新缓存
                    self.set_cached_data(cache_key, merged_data, expiry=3600)
                    logger.info(f"更新缓存数据，新增{len(incremental_data)}条记录")
                    return merged_data
            
            return cached_data

        try:
            # 获取实时数据
            data = get_rzrq_industry_detail(
                board_code=board_code,
                start_date=start_date,
                end_date=end_date,
                page=page,
                page_size=page_size,
                use_chinese_fields=use_chinese_fields
            )
            
            result = data.get('result', {}).get('data', []) if data else []
            
            # 缓存数据1小时
            if result:
                self.set_cached_data(cache_key, result, expiry=3600)
            return result
        except Exception as e:
            logger.error(f"获取行业板块融资融券明细数据时发生错误：{str(e)}")
            return []

    def _get_incremental_rzrq_industry_detail(self, cached_data: List[Dict], board_code: str,
                                              start_date: Optional[str], end_date: Optional[str],
                                              page: int, page_size: int, use_chinese_fields: bool) -> List[Dict]:
        """获取增量的融资融券行业明细数据"""
        try:
            # 处理不同类型的数据
            if isinstance(cached_data, pd.DataFrame):
                cached_data_list = cached_data.to_dict(orient='records')
            elif isinstance(cached_data, list):
                cached_data_list = cached_data
            else:
                logger.warning(f"Unexpected cached_data type: {type(cached_data)}")
                cached_data_list = list(cached_data) if cached_data else []
            
            # 获取缓存数据的最新日期
            latest_cached_date = None
            earliest_cached_date = None
            
            for item in cached_data_list:
                if "交易日期" in item:
                    if isinstance(item["交易日期"], str):
                        item_date = datetime.strptime(item["交易日期"], "%Y-%m-%d")
                    elif isinstance(item["交易日期"], datetime):
                        item_date = item["交易日期"]
                    else:
                        continue
                elif "日期" in item:
                    if isinstance(item["日期"], str):
                        item_date = datetime.strptime(item["日期"], "%Y-%m-%d")
                    elif isinstance(item["日期"], datetime):
                        item_date = item["日期"]
                    else:
                        continue
                else:
                    return []
                
                if latest_cached_date is None or item_date > latest_cached_date:
                    latest_cached_date = item_date
                    
                if earliest_cached_date is None or item_date < earliest_cached_date:
                    earliest_cached_date = item_date
            
            # 确定增量开始日期
            incremental_start_date = latest_cached_date + timedelta(days=1) if latest_cached_date else datetime.now()
            
            if start_date:
                requested_start_date = datetime.strptime(start_date, "%Y-%m-%d")
                # 如果指定的开始日期是周六周日，则调整为之后最近的工作日
                requested_start_date = adjust_start_date(requested_start_date)
            else:
                incremental_start_date = adjust_start_date(incremental_start_date)
            
            # 如果请求的开始日期早于缓存中最早的日期，则需要从更早的日期开始获取数据
            if earliest_cached_date and requested_start_date < earliest_cached_date:
                incremental_start_date = requested_start_date
            elif earliest_cached_date is None:
                incremental_start_date = requested_start_date
            
            # 增量结束日期为今天或指定的结束日期
            incremental_end_date = datetime.now()
            if end_date:
                incremental_end_date = datetime.strptime(end_date, "%Y-%m-%d")
                # 如果指定的结束日期是周六周日，则调整为之前最近的工作日
                incremental_end_date = adjust_end_date(incremental_end_date)
            else:
                incremental_end_date = adjust_end_date(incremental_end_date)
            
            # 如果请求的结束时间比缓存中最早的日期晚，则只需要获取更早的数据
            if earliest_cached_date and incremental_end_date > earliest_cached_date:
                incremental_end_date = earliest_cached_date - timedelta(days=1)
            
            # 如果不需要获取更早的数据且最新缓存日期已是最新的，则返回空列表
            if latest_cached_date and incremental_start_date > incremental_end_date:
                return []
            
            incremental_start_str = incremental_start_date.strftime("%Y-%m-%d")
            incremental_end_str = incremental_end_date.strftime("%Y-%m-%d")

            logger.info(f"获取指定日期范围（{incremental_start_str}至{incremental_end_str}）的融资融券行业明细数据")
            
            # 获取增量数据
            data = get_rzrq_industry_detail(
                board_code=board_code,
                start_date=incremental_start_str,
                end_date=incremental_end_str,
                page=page,
                page_size=page_size,
                use_chinese_fields=use_chinese_fields
            )
            
            result = data.get('result', {}).get('data', []) if data else []
            return result
        except Exception as e:
            logger.error(f"获取增量融资融券行业明细数据失败: {str(e)}")
            return []

    def get_rzrq_history(self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        page: int = 1,
        page_size: int = 10,
        use_chinese_fields: bool = True
    ) -> List[Dict]:
        """ 获取融资融券交易历史明细数据(沪深北三市场)(带缓存支持)
        
        参数:
            start_date: 开始日期，格式为'YYYY-MM-DD'，默认为None
            end_date: 结束日期，格式为'YYYY-MM-DD'，默认为None
            page: 页码，默认为1
            page_size: 每页数量，默认为10
            use_chinese_fields: 是否使用中文字段名，默认为True
        
        返回:
            List[Dict]: 包含融资融券交易历史明细数据的字典列表
        """
        # 生成缓存键
        cache_key = f"rzrq_history:{use_chinese_fields}"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取融资融券交易历史明细数据: {cache_key}")
            # 检查是否需要增量更新
            if self._need_incremental_update(cached_data, start_date=start_date, end_date=end_date):
                logger.info("检测到需要增量更新融资融券历史数据")
                incremental_data = self._get_incremental_rzrq_history(
                    cached_data, start_date, end_date, page, page_size, use_chinese_fields)
                if incremental_data:
                    # 合并数据
                    merged_data = self._merge_and_deduplicate_data(cached_data, incremental_data)
                    # 更新缓存
                    self.set_cached_data(cache_key, merged_data, expiry=3600)
                    logger.info(f"更新缓存数据，新增{len(incremental_data)}条记录")
                    return merged_data
            
            return cached_data

        try:
            # 获取实时数据
            data = get_rzrq_history(
                start_date=start_date,
                end_date=end_date,
                page=page,
                page_size=page_size,
                use_chinese_fields=use_chinese_fields
            )
            
            result = data.get('result', {}).get('data', []) if data else []
            
            # 缓存数据1小时
            if result:
                self.set_cached_data(cache_key, result, expiry=3600)
            return result
        except Exception as e:
            logger.error(f"获取融资融券交易历史明细数据时发生错误：{str(e)}")
            return []

    def _get_incremental_rzrq_history(self, cached_data: List[Dict],
                                      start_date: Optional[str], end_date: Optional[str],
                                      page: int, page_size: int, use_chinese_fields: bool) -> List[Dict]:
        """获取增量的融资融券历史数据"""
        try:
            # 处理不同类型的数据
            if isinstance(cached_data, pd.DataFrame):
                cached_data_list = cached_data.to_dict(orient='records')
            elif isinstance(cached_data, list):
                cached_data_list = cached_data
            else:
                logger.warning(f"Unexpected cached_data type: {type(cached_data)}")
                cached_data_list = list(cached_data) if cached_data else []
            
            # 获取缓存数据的最新日期
            latest_cached_date = None
            earliest_cached_date = None
            
            for item in cached_data_list:
                if "交易日期" in item:
                    if isinstance(item["交易日期"], str):
                        item_date = datetime.strptime(item["交易日期"], "%Y-%m-%d")
                    elif isinstance(item["交易日期"], datetime):
                        item_date = item["交易日期"]
                    else:
                        continue
                elif "日期" in item:
                    if isinstance(item["日期"], str):
                        item_date = datetime.strptime(item["日期"], "%Y-%m-%d")
                    elif isinstance(item["日期"], datetime):
                        item_date = item["日期"]
                    else:
                        continue
                else:
                    return []
                
                if latest_cached_date is None or item_date > latest_cached_date:
                    latest_cached_date = item_date
                    
                if earliest_cached_date is None or item_date < earliest_cached_date:
                    earliest_cached_date = item_date
            
            # 确定增量开始日期
            incremental_start_date = latest_cached_date + timedelta(days=1) if latest_cached_date else datetime.now()
            
            if start_date:
                requested_start_date = datetime.strptime(start_date, "%Y-%m-%d")
                # 如果指定的开始日期是周六周日，则调整为之后最近的工作日
                requested_start_date = adjust_start_date(requested_start_date)
            else:
                incremental_start_date = adjust_start_date(incremental_start_date)
            
            # 如果请求的开始日期早于缓存中最早的日期，则需要从更早的日期开始获取数据
            if earliest_cached_date and requested_start_date < earliest_cached_date:
                incremental_start_date = requested_start_date
            elif earliest_cached_date is None:
                incremental_start_date = requested_start_date
            
            # 增量结束日期为今天或指定的结束日期
            incremental_end_date = datetime.now()
            if end_date:
                incremental_end_date = datetime.strptime(end_date, "%Y-%m-%d")
                # 如果指定的结束日期是周六周日，则调整为之前最近的工作日
                incremental_end_date = adjust_end_date(incremental_end_date)
            else:
                incremental_end_date = adjust_end_date(incremental_end_date)
            
            # 如果请求的结束时间比缓存中最早的日期晚，则只需要获取更早的数据
            if earliest_cached_date and incremental_end_date > earliest_cached_date:
                incremental_end_date = earliest_cached_date - timedelta(days=1)
            
            # 如果不需要获取更早的数据且最新缓存日期已是最新的，则返回空列表
            if latest_cached_date and incremental_start_date > incremental_end_date:
                return []
            
            incremental_start_str = incremental_start_date.strftime("%Y-%m-%d")
            incremental_end_str = incremental_end_date.strftime("%Y-%m-%d")

            logger.info(f"获取指定日期范围（{incremental_start_str}至{incremental_end_str}）的融资融券历史数据")
            
            # 获取增量数据
            data = get_rzrq_history(
                start_date=incremental_start_str,
                end_date=incremental_end_str,
                page=page,
                page_size=page_size,
                use_chinese_fields=use_chinese_fields
            )
            
            result = data.get('result', {}).get('data', []) if data else []
            return result
        except Exception as e:
            logger.error(f"获取增量融资融券历史数据失败: {str(e)}")
            return []

    def get_rzrq_market_summary(self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        page: int = 1,
        page_size: int = 10,
        use_chinese_fields: bool = True
    ) -> List[Dict]:
        """ 获取市场融资融券交易总量数据(含上证指数和融资融券汇总数据)(带缓存支持)
        
        参数:
            start_date: 开始日期，格式为'YYYY-MM-DD'，默认为None
            end_date: 结束日期，格式为'YYYY-MM-DD'，默认为None
            page: 页码，默认为1
            page_size: 每页数量，默认为10
            use_chinese_fields: 是否使用中文字段名，默认为True
        
        返回:
            List[Dict]: 包含市场融资融券交易总量数据的字典列表
        """
        # 生成缓存键
        cache_key = f"rzrq_market_summary:{use_chinese_fields}"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取市场融资融券交易总量数据: {cache_key}")
            # 检查是否需要增量更新
            if self._need_incremental_update(cached_data, start_date=start_date, end_date=end_date):
                logger.info("检测到需要增量更新融资融券市场汇总数据")
                incremental_data = self._get_incremental_rzrq_market_summary(
                    cached_data, start_date, end_date, page, page_size, use_chinese_fields)
                if incremental_data:
                    # 合并数据
                    merged_data = self._merge_and_deduplicate_data(cached_data, incremental_data)
                    # 更新缓存
                    self.set_cached_data(cache_key, merged_data, expiry=3600)
                    logger.info(f"更新缓存数据，新增{len(incremental_data)}条记录")
                    return merged_data
            
            return cached_data

        try:
            # 获取实时数据
            data = get_rzrq_market_summary(
                start_date=start_date,
                end_date=end_date,
                page=page,
                page_size=page_size,
                use_chinese_fields=use_chinese_fields
            )
            
            result = data.get('result', {}).get('data', []) if data else []
            
            # 缓存数据1小时
            if result:
                self.set_cached_data(cache_key, result, expiry=3600)
            return result
        except Exception as e:
            logger.error(f"获取市场融资融券交易总量数据时发生错误：{str(e)}")
            return []

    def _get_incremental_rzrq_market_summary(self, cached_data: List[Dict],
                                             start_date: Optional[str], end_date: Optional[str],
                                             page: int, page_size: int, use_chinese_fields: bool) -> List[Dict]:
        """获取增量的融资融券市场汇总数据"""
        try:
            # 处理不同类型的数据
            if isinstance(cached_data, pd.DataFrame):
                cached_data_list = cached_data.to_dict(orient='records')
            elif isinstance(cached_data, list):
                cached_data_list = cached_data
            else:
                logger.warning(f"Unexpected cached_data type: {type(cached_data)}")
                cached_data_list = list(cached_data) if cached_data else []
            
            # 获取缓存数据的最新日期
            latest_cached_date = None
            earliest_cached_date = None
            
            for item in cached_data_list:
                if "交易日期" in item:
                    if isinstance(item["交易日期"], str):
                        item_date = datetime.strptime(item["交易日期"], "%Y-%m-%d")
                    elif isinstance(item["交易日期"], datetime):
                        item_date = item["交易日期"]
                    else:
                        continue
                elif "日期" in item:
                    if isinstance(item["日期"], str):
                        item_date = datetime.strptime(item["日期"], "%Y-%m-%d")
                    elif isinstance(item["日期"], datetime):
                        item_date = item["日期"]
                    else:
                        continue
                else:
                    return []
                
                if latest_cached_date is None or item_date > latest_cached_date:
                    latest_cached_date = item_date
                    
                if earliest_cached_date is None or item_date < earliest_cached_date:
                    earliest_cached_date = item_date
            
            # 确定增量开始日期
            incremental_start_date = latest_cached_date + timedelta(days=1) if latest_cached_date else datetime.now()
            
            if start_date:
                requested_start_date = datetime.strptime(start_date, "%Y-%m-%d")
                # 如果指定的开始日期是周六周日，则调整为之后最近的工作日
                requested_start_date = adjust_start_date(requested_start_date)
            else:
                incremental_start_date = adjust_start_date(incremental_start_date)
            
            # 如果请求的开始日期早于缓存中最早的日期，则需要从更早的日期开始获取数据
            if earliest_cached_date and requested_start_date < earliest_cached_date:
                incremental_start_date = requested_start_date
            elif earliest_cached_date is None:
                incremental_start_date = requested_start_date
            
            # 增量结束日期为今天或指定的结束日期
            incremental_end_date = datetime.now()
            if end_date:
                incremental_end_date = datetime.strptime(end_date, "%Y-%m-%d")
                # 如果指定的结束日期是周六周日，则调整为之前最近的工作日
                incremental_end_date = adjust_end_date(incremental_end_date)
            else:
                incremental_end_date = adjust_end_date(incremental_end_date)
            
            # 如果请求的结束时间比缓存中最早的日期晚，则只需要获取更早的数据
            if earliest_cached_date and incremental_end_date > earliest_cached_date:
                incremental_end_date = earliest_cached_date - timedelta(days=1)
            
            # 如果不需要获取更早的数据且最新缓存日期已是最新的，则返回空列表
            if latest_cached_date and incremental_start_date > incremental_end_date:
                return []
            
            incremental_start_str = incremental_start_date.strftime("%Y-%m-%d")
            incremental_end_str = incremental_end_date.strftime("%Y-%m-%d")

            logger.info(f"获取指定日期范围（{incremental_start_str}至{incremental_end_str}）的融资融券市场汇总数据")
            
            # 获取增量数据
            data = get_rzrq_market_summary(
                start_date=incremental_start_str,
                end_date=incremental_end_str,
                page=page,
                page_size=page_size,
                use_chinese_fields=use_chinese_fields
            )
            
            result = data.get('result', {}).get('data', []) if data else []
            return result
        except Exception as e:
            logger.error(f"获取增量融资融券市场汇总数据失败: {str(e)}")
            return []

    def get_rzrq_concept_rank(self,
        page: int = 1,
        page_size: int = 50,
        sort_column: str = "FIN_NETBUY_AMT",
        sort_type: int = -1,
        use_chinese_fields: bool = True
    ) -> List[Dict]:
        """ 获取东方财富网融资融券概念板块排行数据(带缓存支持)
        
        参数:
            page: 页码，默认为1
            page_size: 每页数量，默认为50
            sort_column: 排序列，默认为"FIN_NETBUY_AMT"(融资净买入额)
            sort_type: 排序类型，1为升序，-1为降序，默认为-1
            use_chinese_fields: 是否使用中文字段名，默认为True
        
        返回:
            List[Dict]: 包含融资融券概念板块排行数据的字典列表
        """
        # 生成缓存键
        cache_key = f"rzrq_concept_rank:{sort_column}:{sort_type}:{use_chinese_fields}"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取融资融券概念板块排行数据: {cache_key}")
            # 检查是否需要更新（基于交易日变化）
            if self._need_incremental_update(cached_data):
                logger.info("检测到需要更新融资融券概念板块排行数据")
                # 获取最新数据
                new_data = self._fetch_rzrq_concept_rank(
                    page=page,
                    page_size=page_size,
                    sort_column=sort_column,
                    sort_type=sort_type,
                    use_chinese_fields=use_chinese_fields
                )
                if new_data:
                    # 更新缓存
                    self.set_cached_data(cache_key, new_data, expiry=3600)
                    logger.info("已更新融资融券概念板块排行数据缓存")
                    return new_data
                else:
                    # 如果获取新数据失败，返回缓存数据
                    logger.warning("获取最新数据失败，返回缓存数据")
                    return cached_data
            else:
                return cached_data

        try:
            # 获取实时数据
            result = self._fetch_rzrq_concept_rank(
                page=page,
                page_size=page_size,
                sort_column=sort_column,
                sort_type=sort_type,
                use_chinese_fields=use_chinese_fields
            )
            
            # 缓存数据1小时
            if result:
                self.set_cached_data(cache_key, result, expiry=3600)
            return result
        except Exception as e:
            logger.error(f"获取融资融券概念板块排行数据时发生错误：{str(e)}")
            return []

    def _fetch_rzrq_concept_rank(self,
        page: int = 1,
        page_size: int = 50,
        sort_column: str = "FIN_NETBUY_AMT",
        sort_type: int = -1,
        use_chinese_fields: bool = True
    ) -> List[Dict]:
        """ 获取融资融券概念板块排行数据的实际实现 """
        try:
            # 获取实时数据
            data = get_rzrq_concept_rank(
                page=page,
                page_size=page_size,
                sort_column=sort_column,
                sort_type=sort_type,
                use_chinese_fields=use_chinese_fields
            )
            
            result = data.get('result', {}).get('data', []) if data else []
            return result
        except Exception as e:
            logger.error(f"获取融资融券概念板块排行数据时发生错误：{str(e)}")
            return []

    def get_rzrq_account_data(self,
        page: int = 1,
        page_size: int = 50,
        sort_column: str = "STATISTICS_DATE",
        sort_type: int = -1,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        use_chinese_fields: bool = True
    ) -> List[Dict]:
        """ 获取两融账户信息数据(带缓存支持)
        
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
        # 生成缓存键
        cache_key = f"rzrq_account_data::{sort_column}:{sort_type}:{use_chinese_fields}"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取两融账户信息数据: {cache_key}")
            # 检查是否需要增量更新
            if self._need_incremental_update(cached_data, start_date=start_date, end_date=end_date):
                logger.info("检测到需要增量更新两融账户数据")
                incremental_data = self._get_incremental_rzrq_account_data(
                    cached_data, page, page_size, sort_column, sort_type, start_date, end_date, use_chinese_fields)
                if incremental_data:
                    # 合并数据
                    merged_data = self._merge_and_deduplicate_data(cached_data, incremental_data)
                    # 更新缓存
                    self.set_cached_data(cache_key, merged_data, expiry=3600)
                    logger.info(f"更新缓存数据，新增{len(incremental_data)}条记录")
                    return merged_data
            
            return cached_data

        try:
            # 获取实时数据
            data = get_rzrq_account_data(
                page=page,
                page_size=page_size,
                sort_column=sort_column,
                sort_type=sort_type,
                start_date=start_date,
                end_date=end_date,
                use_chinese_fields=use_chinese_fields
            )
            
            result = data.get('result', {}).get('data', []) if data else []
            
            # 缓存数据1小时
            if result:
                self.set_cached_data(cache_key, result, expiry=3600)
            return result
        except Exception as e:
            logger.error(f"获取两融账户信息数据时发生错误：{str(e)}")
            return []

    def _get_incremental_rzrq_account_data(self, cached_data: List[Dict],
                                           page: int, page_size: int,
                                           sort_column: str, sort_type: int,
                                           start_date: Optional[str], end_date: Optional[str],
                                           use_chinese_fields: bool) -> List[Dict]:
        """获取增量的两融账户数据"""
        try:
            # 处理不同类型的数据
            if isinstance(cached_data, pd.DataFrame):
                cached_data_list = cached_data.to_dict(orient='records')
            elif isinstance(cached_data, list):
                cached_data_list = cached_data
            else:
                logger.warning(f"Unexpected cached_data type: {type(cached_data)}")
                cached_data_list = list(cached_data) if cached_data else []
            
            # 获取缓存数据的最新日期
            latest_cached_date = None
            earliest_cached_date = None
            
            for item in cached_data_list:
                if "统计日期" in item:
                    if isinstance(item["统计日期"], str):
                        item_date = datetime.strptime(item["统计日期"], "%Y-%m-%d")
                    elif isinstance(item["统计日期"], datetime):
                        item_date = item["统计日期"]
                    else:
                        continue
                elif "日期" in item:
                    if isinstance(item["日期"], str):
                        item_date = datetime.strptime(item["日期"], "%Y-%m-%d")
                    elif isinstance(item["日期"], datetime):
                        item_date = item["日期"]
                    else:
                        continue
                else:
                    return []
                
                if latest_cached_date is None or item_date > latest_cached_date:
                    latest_cached_date = item_date
                    
                if earliest_cached_date is None or item_date < earliest_cached_date:
                    earliest_cached_date = item_date
            
            # 确定增量开始日期
            incremental_start_date = latest_cached_date + timedelta(days=1) if latest_cached_date else datetime.now()
            
            if start_date:
                requested_start_date = datetime.strptime(start_date, "%Y-%m-%d")
                # 如果指定的开始日期是周六周日，则调整为之后最近的工作日
                requested_start_date = adjust_start_date(requested_start_date)
            else:
                incremental_start_date = adjust_start_date(incremental_start_date)
            
            # 如果请求的开始日期早于缓存中最早的日期，则需要从更早的日期开始获取数据
            if earliest_cached_date and requested_start_date < earliest_cached_date:
                incremental_start_date = requested_start_date
            elif earliest_cached_date is None:
                incremental_start_date = requested_start_date
            
            # 增量结束日期为今天或指定的结束日期
            incremental_end_date = datetime.now()
            if end_date:
                incremental_end_date = datetime.strptime(end_date, "%Y-%m-%d")
                # 如果指定的结束日期是周六周日，则调整为之前最近的工作日
                incremental_end_date = adjust_end_date(incremental_end_date)
            else:
                incremental_end_date = adjust_end_date(incremental_end_date)
            
            # 如果请求的结束时间比缓存中最早的日期晚，则只需要获取更早的数据
            if earliest_cached_date and incremental_end_date > earliest_cached_date:
                incremental_end_date = earliest_cached_date - timedelta(days=1)
            
            # 如果不需要获取更早的数据且最新缓存日期已是最新的，则返回空列表
            if latest_cached_date and incremental_start_date > incremental_end_date:
                return []
            
            incremental_start_str = incremental_start_date.strftime("%Y-%m-%d")
            incremental_end_str = incremental_end_date.strftime("%Y-%m-%d")

            logger.info(f"获取指定日期范围（{incremental_start_str}至{incremental_end_str}）的两融账户数据")
            
            # 获取增量数据
            data = get_rzrq_account_data(
                page=page,
                page_size=page_size,
                sort_column=sort_column,
                sort_type=sort_type,
                start_date=incremental_start_str,
                end_date=incremental_end_str,
                use_chinese_fields=use_chinese_fields
            )
            
            result = data.get('result', {}).get('data', []) if data else []
            return result
        except Exception as e:
            logger.error(f"获取增量两融账户数据失败: {str(e)}")
            return []