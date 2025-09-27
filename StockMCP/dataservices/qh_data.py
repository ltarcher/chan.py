import logging
from typing import Dict, List, Optional, Any
import pandas as pd
from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from .data_service import DataService, adjust_start_date, adjust_end_date
from qh_list import FuturesList
from qh_lhb import FuturesLHB
from qh_ccjg import FuturesCCJG
from qh_jcgc import FuturesJCGC

logger = logging.getLogger(__name__)

class QhDataService(DataService):
    """期货数据类，封装期货相关数据获取逻辑"""
    def __init__(self):
        super().__init__()
        self.futures_list = FuturesList()
        
    def get_futures_list(self,
        is_main_code: bool = True,
        use_chinese_fields: bool = True
    ) -> List[Dict]:
        """ 获取期货品种列表数据(带缓存支持)
        
        参数:
            is_main_code: 是否只获取主力合约，默认为True
            use_chinese_fields: 是否使用中文字段名，默认为True
            
        返回:
            List[Dict]: 包含期货品种数据的字典列表
        """
        # 生成缓存键
        cache_key = f"futures_list:{is_main_code}:{use_chinese_fields}"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取期货品种列表数据: {cache_key}")
            # 检查是否需要更新（基于交易日变化）
            if self._need_incremental_update(cached_data):
                logger.info("检测到需要更新期货品种列表数据")
                # 获取最新数据
                new_data = self._fetch_futures_list(
                    is_main_code=is_main_code,
                    use_chinese_fields=use_chinese_fields
                )
                if new_data:
                    # 更新缓存
                    self.set_cached_data(cache_key, new_data, expiry=3600)
                    logger.info("已更新期货品种列表数据缓存")
                    return new_data
                else:
                    # 如果获取新数据失败，返回缓存数据
                    logger.warning("获取最新数据失败，返回缓存数据")
                    return cached_data
            else:
                return cached_data

        try:
            # 获取实时数据
            result = self._fetch_futures_list(
                is_main_code=is_main_code,
                use_chinese_fields=use_chinese_fields
            )
            
            # 缓存数据1小时
            if result:
                self.set_cached_data(cache_key, result, expiry=3600)
            return result
        except Exception as e:
            logger.error(f"获取期货品种列表数据时发生错误：{str(e)}")
            return []

    def _fetch_futures_list(self,
        is_main_code: bool = True,
        use_chinese_fields: bool = True
    ) -> List[Dict]:
        """ 获取期货品种列表数据的实际实现 """
        try:
            # 获取实时数据
            df = self.futures_list.get_futures_list(
                is_main_code=is_main_code,
                use_chinese_fields=use_chinese_fields
            )
            
            if df.empty:
                return []
                
            return df.to_dict(orient='records')
        except Exception as e:
            logger.error(f"获取期货品种列表数据时发生错误：{str(e)}")
            return []

    def get_future_org_list(self,
        page_size: int = 200,
        use_chinese_fields: bool = True
    ) -> List[Dict]:
        """ 获取期货公司列表数据(带缓存支持)
        
        参数:
            page_size: 每页数据量，默认为200
            use_chinese_fields: 是否使用中文字段名，默认为True
            
        返回:
            List[Dict]: 包含期货公司数据的字典列表
        """
        # 生成缓存键
        cache_key = f"future_org_list:{use_chinese_fields}"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取期货公司列表数据: {cache_key}")
            # 检查是否需要更新（基于交易日变化）
            if self._need_incremental_update(cached_data):
                logger.info("检测到需要更新期货公司列表数据")
                # 获取最新数据
                new_data = self._fetch_future_org_list(
                    page_size=page_size,
                    use_chinese_fields=use_chinese_fields
                )
                if new_data:
                    # 更新缓存
                    self.set_cached_data(cache_key, new_data, expiry=3600)
                    logger.info("已更新期货公司列表数据缓存")
                    return new_data
                else:
                    # 如果获取新数据失败，返回缓存数据
                    logger.warning("获取最新数据失败，返回缓存数据")
                    return cached_data
            else:
                return cached_data

        try:
            # 获取实时数据
            result = self._fetch_future_org_list(
                page_size=page_size,
                use_chinese_fields=use_chinese_fields
            )
            
            # 缓存数据1小时
            if result:
                self.set_cached_data(cache_key, result, expiry=3600)
            return result
        except Exception as e:
            logger.error(f"获取期货公司列表数据时发生错误：{str(e)}")
            return []

    def _fetch_future_org_list(self,
        page_size: int = 200,
        use_chinese_fields: bool = True
    ) -> List[Dict]:
        """ 获取期货公司列表数据的实际实现 """
        try:
            # 获取实时数据
            df = self.futures_list.get_future_org_list(
                page_size=page_size,
                use_chinese_fields=use_chinese_fields
            )
            
            if df.empty:
                return []
                
            return df.to_dict(orient='records')
        except Exception as e:
            logger.error(f"获取期货公司列表数据时发生错误：{str(e)}")
            return []

    def get_exchange_products(self,
        msgid: str,
        use_chinese_fields: bool = True
    ) -> List[Dict]:
        """ 获取交易所可交易品种列表(带缓存支持)
        
        参数:
            msgid: 交易所编号(如220表示中金所)
            use_chinese_fields: 是否使用中文字段名，默认为True
            
        返回:
            List[Dict]: 包含交易所品种数据的字典列表
        """
        # 生成缓存键
        cache_key = f"exchange_products:{msgid}:{use_chinese_fields}"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取交易所品种列表数据: {cache_key}")
            # 检查是否需要更新（基于交易日变化）
            if self._need_incremental_update(cached_data):
                logger.info("检测到需要更新交易所品种列表数据")
                # 获取最新数据
                new_data = self._fetch_exchange_products(
                    msgid=msgid,
                    use_chinese_fields=use_chinese_fields
                )
                if new_data:
                    # 更新缓存
                    self.set_cached_data(cache_key, new_data, expiry=3600)
                    logger.info("已更新交易所品种列表数据缓存")
                    return new_data
                else:
                    # 如果获取新数据失败，返回缓存数据
                    logger.warning("获取最新数据失败，返回缓存数据")
                    return cached_data
            else:
                return cached_data

        try:
            # 获取实时数据
            result = self._fetch_exchange_products(
                msgid=msgid,
                use_chinese_fields=use_chinese_fields
            )
            
            # 缓存数据1小时
            if result:
                self.set_cached_data(cache_key, result, expiry=3600)
            return result
        except Exception as e:
            logger.error(f"获取交易所品种列表数据时发生错误：{str(e)}")
            return []

    def _fetch_exchange_products(self,
        msgid: str,
        use_chinese_fields: bool = True
    ) -> List[Dict]:
        """ 获取交易所可交易品种列表的实际实现 """
        try:
            # 获取实时数据
            df = self.futures_list.get_exchange_products(
                msgid=msgid,
                use_chinese_fields=use_chinese_fields
            )
            
            if df.empty:
                return []
                
            return df.to_dict(orient='records')
        except Exception as e:
            logger.error(f"获取交易所品种列表数据时发生错误：{str(e)}")
            return []

    def get_qh_lhb_data(self,
        security_code: str,
        trade_date: str,
        cookies: Optional[Dict[str, str]] = None,
        use_chinese_fields: bool = True
    ) -> List[Dict]:
        """ 获取期货龙虎榜数据(带缓存支持)
        
        参数:
            security_code: 合约代码(如IF2509)
            trade_date: 交易日期(YYYY-MM-DD)
            cookies: 请求cookies字典
            use_chinese_fields: 是否使用中文字段名，默认为True
            
        返回:
            List[Dict]: 包含龙虎榜数据的字典列表
        """
        # 生成缓存键，包含交易日期以支持按交易日更新
        cache_key = f"qh_lhb_data:{security_code}:{trade_date}:{use_chinese_fields}"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取期货龙虎榜数据: {cache_key}")
            return cached_data

        try:
            # 获取实时数据
            lhb = FuturesLHB(cookies=cookies)
            df = lhb.get_data(
                security_code=security_code,
                trade_date=trade_date,
                use_chinese_fields=use_chinese_fields
            )
            
            if df.empty:
                result = []
            else:
                result = df.to_dict(orient='records')
            
            # 缓存数据1小时
            if result:
                self.set_cached_data(cache_key, result, expiry=3600)
            return result
        except Exception as e:
            logger.error(f"获取期货龙虎榜数据时发生错误：{str(e)}")
            return []

    def get_qh_lhb_rank(self,
        security_code: str,
        trade_date: str,
        rank_field: str,
        cookies: Optional[Dict[str, str]] = None,
        use_chinese_fields: bool = True
    ) -> List[Dict]:
        """ 获取特定排名的期货龙虎榜数据(带缓存支持)
        
        参数:
            security_code: 合约代码(如IF2509)
            trade_date: 交易日期(YYYY-MM-DD)
            rank_field: 排名字段(如VOLUMERANK, LPRANK等)
            cookies: 请求cookies字典
            use_chinese_fields: 是否使用中文字段名，默认为True
            
        返回:
            List[Dict]: 包含特定排名龙虎榜数据的字典列表
        """
        # 生成缓存键，包含交易日期以支持按交易日更新
        cache_key = f"qh_lhb_rank:{security_code}:{trade_date}:{rank_field}:{use_chinese_fields}"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取期货{rank_field}排名数据: {cache_key}")
            return cached_data

        try:
            # 获取实时数据
            lhb = FuturesLHB(cookies=cookies)
            df = lhb.get_data(
                security_code=security_code,
                trade_date=trade_date,
                sort_field=rank_field,
                use_chinese_fields=use_chinese_fields
            )
            
            if df.empty:
                result = []
            else:
                result = df.to_dict(orient='records')
            
            # 缓存数据1小时
            if result:
                self.set_cached_data(cache_key, result, expiry=3600)
            return result
        except Exception as e:
            logger.error(f"获取期货排名数据时发生错误：{str(e)}")
            return []

    def get_qh_ccjg_data(self,
        org_code: str,
        trade_date: str,
        market_name: str,
        cookies: Optional[Dict[str, str]] = None,
        use_chinese_fields: bool = True
    ) -> List[Dict]:
        """ 获取期货公司持仓结构数据(带缓存支持)
        
        参数:
            org_code: 机构代码
            trade_date: 交易日期(YYYY-MM-DD)
            market_name: 市场名称(如上期所、中金所等)
            cookies: 请求cookies字典
            use_chinese_fields: 是否使用中文字段名，默认为True
            
        返回:
            List[Dict]: 包含持仓结构数据的字典列表
        """
        # 生成缓存键，包含交易日期以支持按交易日更新
        cache_key = f"qh_ccjg_data:{org_code}:{trade_date}:{market_name}:{use_chinese_fields}"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取期货公司持仓结构数据: {cache_key}")
            return cached_data

        try:
            # 获取实时数据
            ccjg = FuturesCCJG(cookies=cookies)
            df = ccjg.get_data(
                org_code=org_code,
                trade_date=trade_date,
                market_name=market_name,
                use_chinese_fields=use_chinese_fields
            )
            
            if df.empty:
                result = []
            else:
                result = df.to_dict(orient='records')
            
            # 缓存数据1小时
            if result:
                self.set_cached_data(cache_key, result, expiry=3600)
            return result
        except Exception as e:
            logger.error(f"获取期货公司持仓结构数据时发生错误：{str(e)}")
            return []

    def get_qh_ccjg_multi_market(self,
        org_code: str,
        trade_date: str,
        markets: List[str],
        cookies: Optional[Dict[str, str]] = None,
        use_chinese_fields: bool = True
    ) -> List[Dict]:
        """ 获取期货公司在多个市场的持仓数据(带缓存支持)
        
        参数:
            org_code: 机构代码
            trade_date: 交易日期(YYYY-MM-DD)
            markets: 市场名称列表(如上期所、中金所等)
            cookies: 请求cookies字典
            use_chinese_fields: 是否使用中文字段名，默认为True
            
        返回:
            List[Dict]: 包含多市场持仓数据的字典列表
        """
        # 生成缓存键，包含交易日期以支持按交易日更新
        markets_str = ",".join(markets)
        cache_key = f"qh_ccjg_multi_market:{org_code}:{trade_date}:{markets_str}:{use_chinese_fields}"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取多市场持仓数据: {cache_key}")
            return cached_data

        try:
            # 获取实时数据
            ccjg = FuturesCCJG(cookies=cookies)
            df = ccjg.get_org_positions(
                org_code=org_code,
                trade_date=trade_date,
                markets=markets,
                use_chinese_fields=use_chinese_fields
            )
            
            if df.empty:
                result = []
            else:
                result = df.to_dict(orient='records')
            
            # 缓存数据1小时
            if result:
                self.set_cached_data(cache_key, result, expiry=3600)
            return result
        except Exception as e:
            logger.error(f"获取多市场持仓数据时发生错误：{str(e)}")
            return []

    def get_qh_jcgc_data(self,
        security_code: str,
        org_code: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        cookies: Optional[Dict[str, str]] = None,
        use_chinese_fields: bool = True
    ) -> List[Dict]:
        """ 获取期货建仓过程数据(带缓存支持)
        
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
        # 生成缓存键，包含日期范围以支持按交易日更新
        date_range_key = f"{start_date or ''}-{end_date or ''}"
        cache_key = f"qh_jcgc_data:{security_code}:{org_code}:{date_range_key}:{use_chinese_fields}"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取建仓过程数据: {cache_key}")
            # 检查是否需要增量更新
            if self._need_incremental_update(cached_data, start_date=start_date, end_date=end_date):
                logger.info("检测到需要增量更新建仓过程数据")
                incremental_data = self._get_incremental_qh_jcgc_data(
                    cached_data, security_code, org_code, start_date, end_date, cookies, use_chinese_fields)
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
            jcgc = FuturesJCGC(cookies=cookies)
            df = jcgc.get_data(
                security_code=security_code,
                org_code=org_code,
                start_date=start_date,
                end_date=end_date,
                use_chinese_fields=use_chinese_fields
            )
            
            if df.empty:
                result = []
            else:
                result = df.to_dict(orient='records')
            
            # 缓存数据1小时
            if result:
                self.set_cached_data(cache_key, result, expiry=3600)
            return result
        except Exception as e:
            logger.error(f"获取建仓过程数据时发生错误：{str(e)}")
            return []

    def _get_incremental_qh_jcgc_data(self, cached_data: List[Dict],
                                      security_code: str, org_code: str,
                                      start_date: Optional[str], end_date: Optional[str],
                                      cookies: Optional[Dict[str, str]], use_chinese_fields: bool) -> List[Dict]:
        """获取增量的建仓过程数据"""
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

            logger.info(f"获取指定日期范围（{incremental_start_str}至{incremental_end_str}）的建仓过程数据")
            
            # 获取增量数据
            jcgc = FuturesJCGC(cookies=cookies)
            df = jcgc.get_data(
                security_code=security_code,
                org_code=org_code,
                start_date=incremental_start_str,
                end_date=incremental_end_str,
                use_chinese_fields=use_chinese_fields
            )
            
            if df.empty:
                result = []
            else:
                result = df.to_dict(orient='records')
            return result
        except Exception as e:
            logger.error(f"获取增量建仓过程数据失败: {str(e)}")
            return []

    def get_qh_jcgc_history(self,
        security_code: str,
        org_code: str,
        days: int = 30,
        end_date: Optional[str] = None,
        cookies: Optional[Dict[str, str]] = None,
        use_chinese_fields: bool = True
    ) -> List[Dict]:
        """ 获取指定天数的持仓历史数据(带缓存支持)
        
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
        # 生成缓存键，包含日期参数以支持按交易日更新
        date_params_key = f"{days}-{end_date or ''}"
        cache_key = f"qh_jcgc_history:{security_code}:{org_code}:{date_params_key}:{use_chinese_fields}"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取持仓历史数据: {cache_key}")
            # 检查是否需要增量更新
            if self._need_incremental_update(cached_data):
                logger.info("检测到需要增量更新持仓历史数据")
                incremental_data = self._get_incremental_qh_jcgc_history(
                    cached_data, security_code, org_code, days, end_date, cookies, use_chinese_fields)
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
            jcgc = FuturesJCGC(cookies=cookies)
            df = jcgc.get_position_history(
                security_code=security_code,
                org_code=org_code,
                days=days,
                end_date=end_date,
                use_chinese_fields=use_chinese_fields
            )
            
            if df.empty:
                result = []
            else:
                result = df.to_dict(orient='records')
            
            # 缓存数据1小时
            if result:
                self.set_cached_data(cache_key, result, expiry=3600)
            return result
        except Exception as e:
            logger.error(f"获取持仓历史数据时发生错误：{str(e)}")
            return []

    def _get_incremental_qh_jcgc_history(self, cached_data: List[Dict],
                                         security_code: str, org_code: str,
                                         days: int, end_date: Optional[str],
                                         cookies: Optional[Dict[str, str]], use_chinese_fields: bool) -> List[Dict]:
        """获取增量的持仓历史数据"""
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
            
            # 计算结束日期
            if end_date is None:
                incremental_end_date = datetime.now()
            else:
                incremental_end_date = datetime.strptime(end_date, "%Y-%m-%d")
            
            # 计算开始日期
            incremental_end_date = adjust_end_date(incremental_end_date)
            start_date_obj = incremental_end_date - timedelta(days=days)
            incremental_start_date = adjust_start_date(start_date_obj)
            
            # 如果请求的结束时间比缓存中最早的日期晚，则只需要获取更早的数据
            if earliest_cached_date and incremental_end_date > earliest_cached_date:
                incremental_end_date = earliest_cached_date - timedelta(days=1)
            
            # 如果不需要获取更早的数据且最新缓存日期已是最新的，则返回空列表
            if latest_cached_date and incremental_start_date > incremental_end_date:
                return []
            
            incremental_start_str = incremental_start_date.strftime("%Y-%m-%d")
            incremental_end_str = incremental_end_date.strftime("%Y-%m-%d")

            logger.info(f"获取指定日期范围（{incremental_start_str}至{incremental_end_str}）的持仓历史数据")
            
            # 获取增量数据
            jcgc = FuturesJCGC(cookies=cookies)
            df = jcgc.get_position_history(
                security_code=security_code,
                org_code=org_code,
                days=days,
                end_date=end_date,
                use_chinese_fields=use_chinese_fields
            )
            
            if df.empty:
                result = []
            else:
                result = df.to_dict(orient='records')
            return result
        except Exception as e:
            logger.error(f"获取增量持仓历史数据失败: {str(e)}")
            return []