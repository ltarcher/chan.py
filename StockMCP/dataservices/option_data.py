import logging
from typing import Dict, List, Optional, Any
import pandas as pd
from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from .data_service import DataService, adjust_start_date, adjust_end_date
import replace_qstock_func

logger = logging.getLogger(__name__)

class OptionDataService(DataService):
    """期权数据类，封装期权相关数据获取逻辑"""
    def __init__(self):
        super().__init__()
        
    def get_option_target_list(self) -> List[Dict]:
        """ 获取中国金融市场期权标的列表(带缓存支持)
        
        返回:
            List[Dict]: 包含期权标的代码(code)、名称(name)、市场(market)等
        """
        # 生成缓存键
        cache_key = "option_target_list"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取期权标的数据: {cache_key}")
            # 检查是否需要更新（基于交易日变化）
            if self._need_incremental_update(cached_data):
                logger.info("检测到需要更新期权标的数据")
                # 获取最新数据
                new_data = self._fetch_option_target_list()
                if new_data:
                    # 更新缓存
                    self.set_cached_data(cache_key, new_data, expiry=3600)
                    logger.info("已更新期权标的数据缓存")
                    return new_data
                else:
                    # 如果获取新数据失败，返回缓存数据
                    logger.warning("获取最新数据失败，返回缓存数据")
                    return cached_data
            else:
                return cached_data

        try:
            # 获取实时数据
            result = self._fetch_option_target_list()
            
            # 缓存数据1小时
            if result:
                self.set_cached_data(cache_key, result, expiry=3600)
            return result
        except Exception as e:
            logger.error(f"获取期权标的数据时发生错误：{str(e)}")
            return []

    def _fetch_option_target_list(self) -> List[Dict]:
        """ 获取期权标的数据的实际实现 """
        try:
            # 使用replace_qstock_func中的option_market_code获取期权标的数据
            return replace_qstock_func.option_market_code
        except Exception as e:
            logger.error(f"获取期权标的数据失败: {str(e)}")
            return []

    def get_option_realtime_data(self, 
        codes: List[str], 
        market: str = "期权"
    ) -> List[Dict]:
        """ 获取中国金融市场多个期权的实时数据(带缓存支持)
        
        参数:
            codes: 期权代码列表
            market: 市场类型，默认为期权
            
        返回:
            List[Dict]: 包含期权代码、名称、最新价、涨跌幅等
        """
        # 生成缓存键
        codes_str = ",".join(sorted(codes)) if codes else "all"
        cache_key = f"option_realtime_data:{codes_str}:{market}"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取期权实时数据: {cache_key}")
            # 检查是否需要更新（基于交易日变化）
            if self._need_incremental_update(cached_data):
                logger.info("检测到需要更新期权实时数据")
                # 获取最新数据
                new_data = self._fetch_option_realtime_data(
                    codes=codes,
                    market=market
                )
                if new_data:
                    # 更新缓存
                    self.set_cached_data(cache_key, new_data, expiry=300)  # 实时数据缓存5分钟
                    logger.info("已更新期权实时数据缓存")
                    return new_data
                else:
                    # 如果获取新数据失败，返回缓存数据
                    logger.warning("获取最新数据失败，返回缓存数据")
                    return cached_data
            else:
                return cached_data

        try:
            # 获取实时数据
            result = self._fetch_option_realtime_data(
                codes=codes,
                market=market
            )
            
            # 缓存数据5分钟
            if result:
                self.set_cached_data(cache_key, result, expiry=300)
            return result
        except Exception as e:
            logger.error(f"获取期权实时数据时发生错误：{str(e)}")
            return []

    def _fetch_option_realtime_data(self,
        codes: List[str],
        market: str = "期权"
    ) -> List[Dict]:
        """ 获取期权实时数据的实际实现 """
        try:
            # 调用bso_option_realtime函数，传入option_detail_dict作为字段定义
            df = replace_qstock_func.bso_option_realtime(
                market=market, 
                trade_detail_dict=replace_qstock_func.option_detail_dict, 
                fltt='1'
            )
            
            if df.empty:
                return []
                
            # 如果指定了期权代码，则过滤数据
            if codes:
                df = df[df['代码'].isin(codes)]
                if df.empty:
                    return []
            
            return df.to_dict(orient='records')
        except Exception as e:
            logger.error(f"获取期权实时数据失败: {str(e)}")
            return []

    def get_option_value_data(self,
        codes: List[str],
        market: str = "期权"
    ) -> List[Dict]:
        """ 获取中国金融市场多个期权的价值数据(带缓存支持)
        
        参数:
            codes: 期权代码列表
            market: 市场类型，默认为期权
            
        返回:
            List[Dict]: 包含期权代码、名称、隐含波动率、时间价值、内在价值、理论价格等
        """
        # 生成缓存键
        codes_str = ",".join(sorted(codes)) if codes else "all"
        cache_key = f"option_value_data:{codes_str}:{market}"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取期权价值数据: {cache_key}")
            # 检查是否需要更新（基于交易日变化）
            if self._need_incremental_update(cached_data):
                logger.info("检测到需要更新期权价值数据")
                # 获取最新数据
                new_data = self._fetch_option_value_data(
                    codes=codes,
                    market=market
                )
                if new_data:
                    # 更新缓存
                    self.set_cached_data(cache_key, new_data, expiry=300)  # 实时数据缓存5分钟
                    logger.info("已更新期权价值数据缓存")
                    return new_data
                else:
                    # 如果获取新数据失败，返回缓存数据
                    logger.warning("获取最新数据失败，返回缓存数据")
                    return cached_data
            else:
                return cached_data

        try:
            # 获取实时数据
            result = self._fetch_option_value_data(
                codes=codes,
                market=market
            )
            
            # 缓存数据5分钟
            if result:
                self.set_cached_data(cache_key, result, expiry=300)
            return result
        except Exception as e:
            logger.error(f"获取期权价值数据时发生错误：{str(e)}")
            return []

    def _fetch_option_value_data(self,
        codes: List[str],
        market: str = "期权"
    ) -> List[Dict]:
        """ 获取期权价值数据的实际实现 """
        try:
            # 调用bso_option_realtime函数，传入option_value_detail_dict作为字段定义
            df = replace_qstock_func.bso_option_realtime(
                market=market, 
                trade_detail_dict=replace_qstock_func.option_value_detail_dict, 
                fltt='1'
            )
            
            if df.empty:
                return []
                
            # 如果指定了期权代码，则过滤数据
            if codes:
                df = df[df['代码'].isin(codes)]
                if df.empty:
                    return []
            
            return df.to_dict(orient='records')
        except Exception as e:
            logger.error(f"获取期权价值数据失败: {str(e)}")
            return []

    def get_option_risk_data(self,
        codes: List[str],
        market: str = "期权风险"
    ) -> List[Dict]:
        """ 获取中国金融市场多个期权的风险数据(带缓存支持)
        
        参数:
            codes: 期权代码列表
            market: 市场类型，默认为期权风险
            
        返回:
            List[Dict]: 包含期权代码、名称、Delta、Gamma、Vega、Theta、Rho等希腊字母指标
        """
        # 生成缓存键
        codes_str = ",".join(sorted(codes)) if codes else "all"
        cache_key = f"option_risk_data:{codes_str}:{market}"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取期权风险数据: {cache_key}")
            # 检查是否需要更新（基于交易日变化）
            if self._need_incremental_update(cached_data):
                logger.info("检测到需要更新期权风险数据")
                # 获取最新数据
                new_data = self._fetch_option_risk_data(
                    codes=codes,
                    market=market
                )
                if new_data:
                    # 更新缓存
                    self.set_cached_data(cache_key, new_data, expiry=300)  # 实时数据缓存5分钟
                    logger.info("已更新期权风险数据缓存")
                    return new_data
                else:
                    # 如果获取新数据失败，返回缓存数据
                    logger.warning("获取最新数据失败，返回缓存数据")
                    return cached_data
            else:
                return cached_data

        try:
            # 获取实时数据
            result = self._fetch_option_risk_data(
                codes=codes,
                market=market
            )
            
            # 缓存数据5分钟
            if result:
                self.set_cached_data(cache_key, result, expiry=300)
            return result
        except Exception as e:
            logger.error(f"获取期权风险数据时发生错误：{str(e)}")
            return []

    def _fetch_option_risk_data(self,
        codes: List[str],
        market: str = "期权风险"
    ) -> List[Dict]:
        """ 获取期权风险数据的实际实现 """
        try:
            # 调用bso_option_realtime函数，传入option_risk_detail_dict作为字段定义
            df = replace_qstock_func.bso_option_realtime(
                market=market, 
                trade_detail_dict=replace_qstock_func.option_risk_detail_dict, 
                fltt='1'
            )
            
            if df.empty:
                return []
                
            # 如果指定了期权代码，则过滤数据
            if codes:
                df = df[df['代码'].isin(codes)]
                if df.empty:
                    return []
            
            return df.to_dict(orient='records')
        except Exception as e:
            logger.error(f"获取期权风险数据失败: {str(e)}")
            return []

    def get_option_tboard_data(self,
        expire_month: Optional[str] = None
    ) -> List[Dict]:
        """ 获取期权T型看板数据(带缓存支持)
        
        参数:
            expire_month: 到期月份，格式为YYYYMM，如202312。为空时获取所有月份的T型看板数据
            
        返回:
            List[Dict]: 包含期权T型看板数据
        """
        # 生成缓存键
        cache_key = f"option_tboard_data:{expire_month or 'all'}"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取期权T型看板数据: {cache_key}")
            # 检查是否需要更新（基于交易日变化）
            if self._need_incremental_update(cached_data):
                logger.info("检测到需要更新期权T型看板数据")
                # 获取最新数据
                new_data = self._fetch_option_tboard_data(expire_month=expire_month)
                if new_data:
                    # 更新缓存
                    self.set_cached_data(cache_key, new_data, expiry=300)  # 实时数据缓存5分钟
                    logger.info("已更新期权T型看板数据缓存")
                    return new_data
                else:
                    # 如果获取新数据失败，返回缓存数据
                    logger.warning("获取最新数据失败，返回缓存数据")
                    return cached_data
            else:
                return cached_data

        try:
            # 获取实时数据
            result = self._fetch_option_tboard_data(expire_month=expire_month)
            
            # 缓存数据5分钟
            if result:
                self.set_cached_data(cache_key, result, expiry=300)
            return result
        except Exception as e:
            logger.error(f"获取期权T型看板数据时发生错误：{str(e)}")
            return []

    def _fetch_option_tboard_data(self,
        expire_month: Optional[str] = None
    ) -> List[Dict]:
        """ 获取期权T型看板数据的实际实现 """
        try:
            if expire_month:
                # 获取指定月份的T型看板数据
                df = replace_qstock_func.bso_option_tboard_month(expire_month)
            else:
                # 获取所有月份的T型看板数据
                df = replace_qstock_func.bso_option_tboard_all()
            
            if df.empty:
                return []
            
            return df.to_dict(orient='records')
        except Exception as e:
            logger.error(f"获取期权T型看板数据失败: {str(e)}")
            return []

    def get_option_expire_all_data(self) -> List[Dict]:
        """ 获取所有期权标的的到期日信息(带缓存支持)
        
        返回:
            List[Dict]: 包含所有期权标的的到期日信息
        """
        # 生成缓存键
        cache_key = "option_expire_all_data"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取所有期权标的的到期日信息: {cache_key}")
            # 检查是否需要更新（基于交易日变化）
            if self._need_incremental_update(cached_data):
                logger.info("检测到需要更新期权标的到期日信息")
                # 获取最新数据
                new_data = self._fetch_option_expire_all_data()
                if new_data:
                    # 更新缓存
                    self.set_cached_data(cache_key, new_data, expiry=3600)
                    logger.info("已更新期权标的到期日信息缓存")
                    return new_data
                else:
                    # 如果获取新数据失败，返回缓存数据
                    logger.warning("获取最新数据失败，返回缓存数据")
                    return cached_data
            else:
                return cached_data

        try:
            # 获取实时数据
            result = self._fetch_option_expire_all_data()
            
            # 缓存数据1小时
            if result:
                self.set_cached_data(cache_key, result, expiry=3600)
            return result
        except Exception as e:
            logger.error(f"获取所有期权标的的到期日信息时发生错误：{str(e)}")
            return []

    def _fetch_option_expire_all_data(self) -> List[Dict]:
        """ 获取所有期权标的的到期日信息的实际实现 """
        try:
            df = replace_qstock_func.bso_option_expire_all()
            
            if df.empty:
                return []
            
            return df.to_dict(orient='records')
        except Exception as e:
            logger.error(f"获取所有期权标的的到期日信息失败: {str(e)}")
            return []

    def get_option_expire_info_data(self,
        code: str,
        market: int = 0
    ) -> List[Dict]:
        """ 获取指定期权标的代码的到期日信息(带缓存支持)
        
        参数:
            code: 期权标的代码，如510050（50ETF）
            market: 市场类型，0表示深交所，1表示上交所，默认为0
            
        返回:
            List[Dict]: 包含指定期权代码的到期日信息
        """
        # 生成缓存键
        cache_key = f"option_expire_info_data:{code}:{market}"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取期权标的代码 {code} 的到期日信息: {cache_key}")
            # 检查是否需要更新（基于交易日变化）
            if self._need_incremental_update(cached_data):
                logger.info("检测到需要更新期权标的到期日信息")
                # 获取最新数据
                new_data = self._fetch_option_expire_info_data(
                    code=code,
                    market=market
                )
                if new_data:
                    # 更新缓存
                    self.set_cached_data(cache_key, new_data, expiry=3600)
                    logger.info("已更新期权标的到期日信息缓存")
                    return new_data
                else:
                    # 如果获取新数据失败，返回缓存数据
                    logger.warning("获取最新数据失败，返回缓存数据")
                    return cached_data
            else:
                return cached_data

        try:
            # 获取实时数据
            result = self._fetch_option_expire_info_data(
                code=code,
                market=market
            )
            
            # 缓存数据1小时
            if result:
                self.set_cached_data(cache_key, result, expiry=3600)
            return result
        except Exception as e:
            logger.error(f"获取期权标的代码 {code} 的到期日信息时发生错误：{str(e)}")
            return []

    def _fetch_option_expire_info_data(self,
        code: str,
        market: int = 0
    ) -> List[Dict]:
        """ 获取指定期权标的代码的到期日信息的实际实现 """
        try:
            df = replace_qstock_func.bso_option_expire_info(code, market)
            
            if df.empty:
                return []
            
            return df.to_dict(orient='records')
        except Exception as e:
            logger.error(f"获取期权标的代码 {code} 的到期日信息失败: {str(e)}")
            return []