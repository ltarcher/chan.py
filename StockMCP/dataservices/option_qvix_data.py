import logging
from typing import Dict, List, Optional, Any
import pandas as pd
from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from .data_service import DataService, adjust_start_date, adjust_end_date
import index_option_qvix

logger = logging.getLogger(__name__)

class OptionQvixDataService(DataService):
    """期权QVIX数据类，封装期权波动率指数相关数据获取逻辑"""
    
    def __init__(self):
        super().__init__()
        self.proxy = None
    
    def set_proxy(self, proxy: Optional[str|dict] = None):
        """设置HTTP代理
        
        参数:
            proxy: 代理配置，支持 dict 格式，如 {"http": "http://127.0.0.1:1080", "https": "http://127.0.0.1:1080"}
                 或字符串格式，如 "http://127.0.0.1:1080"
        """
        self.proxy = proxy
        
    def get_50etf_qvix(self) -> List[Dict]:
        """ 获取50ETF期权波动率指数QVIX日线数据(带缓存支持)
        
        返回:
            List[Dict]: 包含50ETF期权波动率指数QVIX日线数据
        """
        # 生成缓存键
        cache_key = "option_qvix:50etf"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取50ETF期权波动率指数QVIX日线数据: {cache_key}")
            # 检查是否需要更新（基于交易日变化）
            if self._need_incremental_update(cached_data):
                logger.info("检测到需要更新50ETF期权波动率指数QVIX日线数据")
                # 获取最新数据
                new_data = self._fetch_50etf_qvix()
                if new_data:
                    # 更新缓存
                    self.set_cached_data(cache_key, new_data, expiry=3600)
                    logger.info("已更新50ETF期权波动率指数QVIX日线数据缓存")
                    return new_data
                else:
                    # 如果获取新数据失败，返回缓存数据
                    logger.warning("获取最新数据失败，返回缓存数据")
                    return cached_data
            else:
                return cached_data

        try:
            # 获取实时数据
            result = self._fetch_50etf_qvix()
            
            # 缓存数据1小时
            if result:
                self.set_cached_data(cache_key, result, expiry=3600)
            return result
        except Exception as e:
            logger.error(f"获取50ETF期权波动率指数QVIX日线数据时发生错误：{str(e)}")
            return []

    def _fetch_50etf_qvix(self) -> List[Dict]:
        """ 获取50ETF期权波动率指数QVIX日线数据的实际实现 """
        try:
            df = index_option_qvix.index_option_50etf_qvix(proxy=self.proxy)
            
            if df.empty:
                return []
            
            # 确保日期列是字符串格式
            df['date'] = df['date'].astype(str)
            
            return df.to_dict(orient='records')
        except Exception as e:
            logger.error(f"获取50ETF期权波动率指数QVIX日线数据失败: {str(e)}")
            return []

    def get_50etf_min_qvix(self) -> List[Dict]:
        """ 获取50ETF期权波动率指数QVIX分钟线数据(带缓存支持)
        
        返回:
            List[Dict]: 包含50ETF期权波动率指数QVIX分钟线数据
        """
        # 生成缓存键
        cache_key = "option_qvix:50etf_min"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取50ETF期权波动率指数QVIX分钟线数据: {cache_key}")
            # 检查是否需要更新（基于交易日变化）
            if self._need_incremental_update(cached_data):
                logger.info("检测到需要更新50ETF期权波动率指数QVIX分钟线数据")
                # 获取最新数据
                new_data = self._fetch_50etf_min_qvix()
                if new_data:
                    # 更新缓存
                    self.set_cached_data(cache_key, new_data, expiry=300)  # 实时数据缓存5分钟
                    logger.info("已更新50ETF期权波动率指数QVIX分钟线数据缓存")
                    return new_data
                else:
                    # 如果获取新数据失败，返回缓存数据
                    logger.warning("获取最新数据失败，返回缓存数据")
                    return cached_data
            else:
                return cached_data

        try:
            # 获取实时数据
            result = self._fetch_50etf_min_qvix()
            
            # 缓存数据5分钟
            if result:
                self.set_cached_data(cache_key, result, expiry=300)
            return result
        except Exception as e:
            logger.error(f"获取50ETF期权波动率指数QVIX分钟线数据时发生错误：{str(e)}")
            return []

    def _fetch_50etf_min_qvix(self) -> List[Dict]:
        """ 获取50ETF期权波动率指数QVIX分钟线数据的实际实现 """
        try:
            df = index_option_qvix.index_option_50etf_min_qvix(proxy=self.proxy)
            
            if df.empty:
                return []
            
            return df.to_dict(orient='records')
        except Exception as e:
            logger.error(f"获取50ETF期权波动率指数QVIX分钟线数据失败: {str(e)}")
            return []

    def get_300etf_qvix(self) -> List[Dict]:
        """ 获取300ETF期权波动率指数QVIX日线数据(带缓存支持)
        
        返回:
            List[Dict]: 包含300ETF期权波动率指数QVIX日线数据
        """
        # 生成缓存键
        cache_key = "option_qvix:300etf"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取300ETF期权波动率指数QVIX日线数据: {cache_key}")
            # 检查是否需要更新（基于交易日变化）
            if self._need_incremental_update(cached_data):
                logger.info("检测到需要更新300ETF期权波动率指数QVIX日线数据")
                # 获取最新数据
                new_data = self._fetch_300etf_qvix()
                if new_data:
                    # 更新缓存
                    self.set_cached_data(cache_key, new_data, expiry=3600)
                    logger.info("已更新300ETF期权波动率指数QVIX日线数据缓存")
                    return new_data
                else:
                    # 如果获取新数据失败，返回缓存数据
                    logger.warning("获取最新数据失败，返回缓存数据")
                    return cached_data
            else:
                return cached_data

        try:
            # 获取实时数据
            result = self._fetch_300etf_qvix()
            
            # 缓存数据1小时
            if result:
                self.set_cached_data(cache_key, result, expiry=3600)
            return result
        except Exception as e:
            logger.error(f"获取300ETF期权波动率指数QVIX日线数据时发生错误：{str(e)}")
            return []

    def _fetch_300etf_qvix(self) -> List[Dict]:
        """ 获取300ETF期权波动率指数QVIX日线数据的实际实现 """
        try:
            df = index_option_qvix.index_option_300etf_qvix(proxy=self.proxy)
            
            if df.empty:
                return []
            
            # 确保日期列是字符串格式
            df['date'] = df['date'].astype(str)
            
            return df.to_dict(orient='records')
        except Exception as e:
            logger.error(f"获取300ETF期权波动率指数QVIX日线数据失败: {str(e)}")
            return []

    def get_300etf_min_qvix(self) -> List[Dict]:
        """ 获取300ETF期权波动率指数QVIX分钟线数据(带缓存支持)
        
        返回:
            List[Dict]: 包含300ETF期权波动率指数QVIX分钟线数据
        """
        # 生成缓存键
        cache_key = "option_qvix:300etf_min"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取300ETF期权波动率指数QVIX分钟线数据: {cache_key}")
            # 检查是否需要更新（基于交易日变化）
            if self._need_incremental_update(cached_data):
                logger.info("检测到需要更新300ETF期权波动率指数QVIX分钟线数据")
                # 获取最新数据
                new_data = self._fetch_300etf_min_qvix()
                if new_data:
                    # 更新缓存
                    self.set_cached_data(cache_key, new_data, expiry=300)  # 实时数据缓存5分钟
                    logger.info("已更新300ETF期权波动率指数QVIX分钟线数据缓存")
                    return new_data
                else:
                    # 如果获取新数据失败，返回缓存数据
                    logger.warning("获取最新数据失败，返回缓存数据")
                    return cached_data
            else:
                return cached_data

        try:
            # 获取实时数据
            result = self._fetch_300etf_min_qvix()
            
            # 缓存数据5分钟
            if result:
                self.set_cached_data(cache_key, result, expiry=300)
            return result
        except Exception as e:
            logger.error(f"获取300ETF期权波动率指数QVIX分钟线数据时发生错误：{str(e)}")
            return []

    def _fetch_300etf_min_qvix(self) -> List[Dict]:
        """ 获取300ETF期权波动率指数QVIX分钟线数据的实际实现 """
        try:
            df = index_option_qvix.index_option_300etf_min_qvix(proxy=self.proxy)
            
            if df.empty:
                return []
            
            return df.to_dict(orient='records')
        except Exception as e:
            logger.error(f"获取300ETF期权波动率指数QVIX分钟线数据失败: {str(e)}")
            return []

    def get_500etf_qvix(self) -> List[Dict]:
        """ 获取500ETF期权波动率指数QVIX日线数据(带缓存支持)
        
        返回:
            List[Dict]: 包含500ETF期权波动率指数QVIX日线数据
        """
        # 生成缓存键
        cache_key = "option_qvix:500etf"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取500ETF期权波动率指数QVIX日线数据: {cache_key}")
            # 检查是否需要更新（基于交易日变化）
            if self._need_incremental_update(cached_data):
                logger.info("检测到需要更新500ETF期权波动率指数QVIX日线数据")
                # 获取最新数据
                new_data = self._fetch_500etf_qvix()
                if new_data:
                    # 更新缓存
                    self.set_cached_data(cache_key, new_data, expiry=3600)
                    logger.info("已更新500ETF期权波动率指数QVIX日线数据缓存")
                    return new_data
                else:
                    # 如果获取新数据失败，返回缓存数据
                    logger.warning("获取最新数据失败，返回缓存数据")
                    return cached_data
            else:
                return cached_data

        try:
            # 获取实时数据
            result = self._fetch_500etf_qvix()
            
            # 缓存数据1小时
            if result:
                self.set_cached_data(cache_key, result, expiry=3600)
            return result
        except Exception as e:
            logger.error(f"获取500ETF期权波动率指数QVIX日线数据时发生错误：{str(e)}")
            return []

    def _fetch_500etf_qvix(self) -> List[Dict]:
        """ 获取500ETF期权波动率指数QVIX日线数据的实际实现 """
        try:
            df = index_option_qvix.index_option_500etf_qvix(proxy=self.proxy)
            
            if df.empty:
                return []
            
            # 确保日期列是字符串格式
            df['date'] = df['date'].astype(str)
            
            return df.to_dict(orient='records')
        except Exception as e:
            logger.error(f"获取500ETF期权波动率指数QVIX日线数据失败: {str(e)}")
            return []

    def get_500etf_min_qvix(self) -> List[Dict]:
        """ 获取500ETF期权波动率指数QVIX分钟线数据(带缓存支持)
        
        返回:
            List[Dict]: 包含500ETF期权波动率指数QVIX分钟线数据
        """
        # 生成缓存键
        cache_key = "option_qvix:500etf_min"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取500ETF期权波动率指数QVIX分钟线数据: {cache_key}")
            # 检查是否需要更新（基于交易日变化）
            if self._need_incremental_update(cached_data):
                logger.info("检测到需要更新500ETF期权波动率指数QVIX分钟线数据")
                # 获取最新数据
                new_data = self._fetch_500etf_min_qvix()
                if new_data:
                    # 更新缓存
                    self.set_cached_data(cache_key, new_data, expiry=300)  # 实时数据缓存5分钟
                    logger.info("已更新500ETF期权波动率指数QVIX分钟线数据缓存")
                    return new_data
                else:
                    # 如果获取新数据失败，返回缓存数据
                    logger.warning("获取最新数据失败，返回缓存数据")
                    return cached_data
            else:
                return cached_data

        try:
            # 获取实时数据
            result = self._fetch_500etf_min_qvix()
            
            # 缓存数据5分钟
            if result:
                self.set_cached_data(cache_key, result, expiry=300)
            return result
        except Exception as e:
            logger.error(f"获取500ETF期权波动率指数QVIX分钟线数据时发生错误：{str(e)}")
            return []

    def _fetch_500etf_min_qvix(self) -> List[Dict]:
        """ 获取500ETF期权波动率指数QVIX分钟线数据的实际实现 """
        try:
            df = index_option_qvix.index_option_500etf_min_qvix(proxy=self.proxy)
            
            if df.empty:
                return []
            
            return df.to_dict(orient='records')
        except Exception as e:
            logger.error(f"获取500ETF期权波动率指数QVIX分钟线数据失败: {str(e)}")
            return []

    def get_cyb_qvix(self) -> List[Dict]:
        """ 获取创业板期权波动率指数QVIX日线数据(带缓存支持)
        
        返回:
            List[Dict]: 包含创业板期权波动率指数QVIX日线数据
        """
        # 生成缓存键
        cache_key = "option_qvix:cyb"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取创业板期权波动率指数QVIX日线数据: {cache_key}")
            # 检查是否需要更新（基于交易日变化）
            if self._need_incremental_update(cached_data):
                logger.info("检测到需要更新创业板期权波动率指数QVIX日线数据")
                # 获取最新数据
                new_data = self._fetch_cyb_qvix()
                if new_data:
                    # 更新缓存
                    self.set_cached_data(cache_key, new_data, expiry=3600)
                    logger.info("已更新创业板期权波动率指数QVIX日线数据缓存")
                    return new_data
                else:
                    # 如果获取新数据失败，返回缓存数据
                    logger.warning("获取最新数据失败，返回缓存数据")
                    return cached_data
            else:
                return cached_data

        try:
            # 获取实时数据
            result = self._fetch_cyb_qvix()
            
            # 缓存数据1小时
            if result:
                self.set_cached_data(cache_key, result, expiry=3600)
            return result
        except Exception as e:
            logger.error(f"获取创业板期权波动率指数QVIX日线数据时发生错误：{str(e)}")
            return []

    def _fetch_cyb_qvix(self) -> List[Dict]:
        """ 获取创业板期权波动率指数QVIX日线数据的实际实现 """
        try:
            df = index_option_qvix.index_option_cyb_qvix(proxy=self.proxy)
            
            if df.empty:
                return []
            
            # 确保日期列是字符串格式
            df['date'] = df['date'].astype(str)
            
            return df.to_dict(orient='records')
        except Exception as e:
            logger.error(f"获取创业板期权波动率指数QVIX日线数据失败: {str(e)}")
            return []

    def get_cyb_min_qvix(self) -> List[Dict]:
        """ 获取创业板期权波动率指数QVIX分钟线数据(带缓存支持)
        
        返回:
            List[Dict]: 包含创业板期权波动率指数QVIX分钟线数据
        """
        # 生成缓存键
        cache_key = "option_qvix:cyb_min"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取创业板期权波动率指数QVIX分钟线数据: {cache_key}")
            # 检查是否需要更新（基于交易日变化）
            if self._need_incremental_update(cached_data):
                logger.info("检测到需要更新创业板期权波动率指数QVIX分钟线数据")
                # 获取最新数据
                new_data = self._fetch_cyb_min_qvix()
                if new_data:
                    # 更新缓存
                    self.set_cached_data(cache_key, new_data, expiry=300)  # 实时数据缓存5分钟
                    logger.info("已更新创业板期权波动率指数QVIX分钟线数据缓存")
                    return new_data
                else:
                    # 如果获取新数据失败，返回缓存数据
                    logger.warning("获取最新数据失败，返回缓存数据")
                    return cached_data
            else:
                return cached_data

        try:
            # 获取实时数据
            result = self._fetch_cyb_min_qvix()
            
            # 缓存数据5分钟
            if result:
                self.set_cached_data(cache_key, result, expiry=300)
            return result
        except Exception as e:
            logger.error(f"获取创业板期权波动率指数QVIX分钟线数据时发生错误：{str(e)}")
            return []

    def _fetch_cyb_min_qvix(self) -> List[Dict]:
        """ 获取创业板期权波动率指数QVIX分钟线数据的实际实现 """
        try:
            df = index_option_qvix.index_option_cyb_min_qvix(proxy=self.proxy)
            
            if df.empty:
                return []
            
            return df.to_dict(orient='records')
        except Exception as e:
            logger.error(f"获取创业板期权波动率指数QVIX分钟线数据失败: {str(e)}")
            return []

    def get_kcb_qvix(self) -> List[Dict]:
        """ 获取科创板期权波动率指数QVIX日线数据(带缓存支持)
        
        返回:
            List[Dict]: 包含科创板期权波动率指数QVIX日线数据
        """
        # 生成缓存键
        cache_key = "option_qvix:kcb"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取科创板期权波动率指数QVIX日线数据: {cache_key}")
            # 检查是否需要更新（基于交易日变化）
            if self._need_incremental_update(cached_data):
                logger.info("检测到需要更新科创板期权波动率指数QVIX日线数据")
                # 获取最新数据
                new_data = self._fetch_kcb_qvix()
                if new_data:
                    # 更新缓存
                    self.set_cached_data(cache_key, new_data, expiry=3600)
                    logger.info("已更新科创板期权波动率指数QVIX日线数据缓存")
                    return new_data
                else:
                    # 如果获取新数据失败，返回缓存数据
                    logger.warning("获取最新数据失败，返回缓存数据")
                    return cached_data
            else:
                return cached_data

        try:
            # 获取实时数据
            result = self._fetch_kcb_qvix()
            
            # 缓存数据1小时
            if result:
                self.set_cached_data(cache_key, result, expiry=3600)
            return result
        except Exception as e:
            logger.error(f"获取科创板期权波动率指数QVIX日线数据时发生错误：{str(e)}")
            return []

    def _fetch_kcb_qvix(self) -> List[Dict]:
        """ 获取科创板期权波动率指数QVIX日线数据的实际实现 """
        try:
            df = index_option_qvix.index_option_kcb_qvix(proxy=self.proxy)
            
            if df.empty:
                return []
            
            # 确保日期列是字符串格式
            df['date'] = df['date'].astype(str)
            
            return df.to_dict(orient='records')
        except Exception as e:
            logger.error(f"获取科创板期权波动率指数QVIX日线数据失败: {str(e)}")
            return []

    def get_kcb_min_qvix(self) -> List[Dict]:
        """ 获取科创板期权波动率指数QVIX分钟线数据(带缓存支持)
        
        返回:
            List[Dict]: 包含科创板期权波动率指数QVIX分钟线数据
        """
        # 生成缓存键
        cache_key = "option_qvix:kcb_min"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取科创板期权波动率指数QVIX分钟线数据: {cache_key}")
            # 检查是否需要更新（基于交易日变化）
            if self._need_incremental_update(cached_data):
                logger.info("检测到需要更新科创板期权波动率指数QVIX分钟线数据")
                # 获取最新数据
                new_data = self._fetch_kcb_min_qvix()
                if new_data:
                    # 更新缓存
                    self.set_cached_data(cache_key, new_data, expiry=300)  # 实时数据缓存5分钟
                    logger.info("已更新科创板期权波动率指数QVIX分钟线数据缓存")
                    return new_data
                else:
                    # 如果获取新数据失败，返回缓存数据
                    logger.warning("获取最新数据失败，返回缓存数据")
                    return cached_data
            else:
                return cached_data

        try:
            # 获取实时数据
            result = self._fetch_kcb_min_qvix()
            
            # 缓存数据5分钟
            if result:
                self.set_cached_data(cache_key, result, expiry=300)
            return result
        except Exception as e:
            logger.error(f"获取科创板期权波动率指数QVIX分钟线数据时发生错误：{str(e)}")
            return []

    def _fetch_kcb_min_qvix(self) -> List[Dict]:
        """ 获取科创板期权波动率指数QVIX分钟线数据的实际实现 """
        try:
            df = index_option_qvix.index_option_kcb_min_qvix(proxy=self.proxy)
            
            if df.empty:
                return []
            
            return df.to_dict(orient='records')
        except Exception as e:
            logger.error(f"获取科创板期权波动率指数QVIX分钟线数据失败: {str(e)}")
            return []

    def get_100etf_qvix(self) -> List[Dict]:
        """ 获取深证100ETF期权波动率指数QVIX日线数据(带缓存支持)
        
        返回:
            List[Dict]: 包含深证100ETF期权波动率指数QVIX日线数据
        """
        # 生成缓存键
        cache_key = "option_qvix:100etf"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取深证100ETF期权波动率指数QVIX日线数据: {cache_key}")
            # 检查是否需要更新（基于交易日变化）
            if self._need_incremental_update(cached_data):
                logger.info("检测到需要更新深证100ETF期权波动率指数QVIX日线数据")
                # 获取最新数据
                new_data = self._fetch_100etf_qvix()
                if new_data:
                    # 更新缓存
                    self.set_cached_data(cache_key, new_data, expiry=3600)
                    logger.info("已更新深证100ETF期权波动率指数QVIX日线数据缓存")
                    return new_data
                else:
                    # 如果获取新数据失败，返回缓存数据
                    logger.warning("获取最新数据失败，返回缓存数据")
                    return cached_data
            else:
                return cached_data

        try:
            # 获取实时数据
            result = self._fetch_100etf_qvix()
            
            # 缓存数据1小时
            if result:
                self.set_cached_data(cache_key, result, expiry=3600)
            return result
        except Exception as e:
            logger.error(f"获取深证100ETF期权波动率指数QVIX日线数据时发生错误：{str(e)}")
            return []

    def _fetch_100etf_qvix(self) -> List[Dict]:
        """ 获取深证100ETF期权波动率指数QVIX日线数据的实际实现 """
        try:
            df = index_option_qvix.index_option_100etf_qvix(proxy=self.proxy)
            
            if df.empty:
                return []
            
            # 确保日期列是字符串格式
            df['date'] = df['date'].astype(str)
            
            return df.to_dict(orient='records')
        except Exception as e:
            logger.error(f"获取深证100ETF期权波动率指数QVIX日线数据失败: {str(e)}")
            return []

    def get_100etf_min_qvix(self) -> List[Dict]:
        """ 获取深证100ETF期权波动率指数QVIX分钟线数据(带缓存支持)
        
        返回:
            List[Dict]: 包含深证100ETF期权波动率指数QVIX分钟线数据
        """
        # 生成缓存键
        cache_key = "option_qvix:100etf_min"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取深证100ETF期权波动率指数QVIX分钟线数据: {cache_key}")
            # 检查是否需要更新（基于交易日变化）
            if self._need_incremental_update(cached_data):
                logger.info("检测到需要更新深证100ETF期权波动率指数QVIX分钟线数据")
                # 获取最新数据
                new_data = self._fetch_100etf_min_qvix()
                if new_data:
                    # 更新缓存
                    self.set_cached_data(cache_key, new_data, expiry=300)  # 实时数据缓存5分钟
                    logger.info("已更新深证100ETF期权波动率指数QVIX分钟线数据缓存")
                    return new_data
                else:
                    # 如果获取新数据失败，返回缓存数据
                    logger.warning("获取最新数据失败，返回缓存数据")
                    return cached_data
            else:
                return cached_data

        try:
            # 获取实时数据
            result = self._fetch_100etf_min_qvix()
            
            # 缓存数据5分钟
            if result:
                self.set_cached_data(cache_key, result, expiry=300)
            return result
        except Exception as e:
            logger.error(f"获取深证100ETF期权波动率指数QVIX分钟线数据时发生错误：{str(e)}")
            return []

    def _fetch_100etf_min_qvix(self) -> List[Dict]:
        """ 获取深证100ETF期权波动率指数QVIX分钟线数据的实际实现 """
        try:
            df = index_option_qvix.index_option_100etf_min_qvix(proxy=self.proxy)
            
            if df.empty:
                return []
            
            return df.to_dict(orient='records')
        except Exception as e:
            logger.error(f"获取深证100ETF期权波动率指数QVIX分钟线数据失败: {str(e)}")
            return []

    def get_300index_qvix(self) -> List[Dict]:
        """ 获取中证300股指期权波动率指数QVIX日线数据(带缓存支持)
        
        返回:
            List[Dict]: 包含中证300股指期权波动率指数QVIX日线数据
        """
        # 生成缓存键
        cache_key = "option_qvix:300index"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取中证300股指期权波动率指数QVIX日线数据: {cache_key}")
            # 检查是否需要更新（基于交易日变化）
            if self._need_incremental_update(cached_data):
                logger.info("检测到需要更新中证300股指期权波动率指数QVIX日线数据")
                # 获取最新数据
                new_data = self._fetch_300index_qvix()
                if new_data:
                    # 更新缓存
                    self.set_cached_data(cache_key, new_data, expiry=3600)
                    logger.info("已更新中证300股指期权波动率指数QVIX日线数据缓存")
                    return new_data
                else:
                    # 如果获取新数据失败，返回缓存数据
                    logger.warning("获取最新数据失败，返回缓存数据")
                    return cached_data
            else:
                return cached_data

        try:
            # 获取实时数据
            result = self._fetch_300index_qvix()
            
            # 缓存数据1小时
            if result:
                self.set_cached_data(cache_key, result, expiry=3600)
            return result
        except Exception as e:
            logger.error(f"获取中证300股指期权波动率指数QVIX日线数据时发生错误：{str(e)}")
            return []

    def _fetch_300index_qvix(self) -> List[Dict]:
        """ 获取中证300股指期权波动率指数QVIX日线数据的实际实现 """
        try:
            df = index_option_qvix.index_option_300index_qvix(proxy=self.proxy)
            
            if df.empty:
                return []
            
            # 确保日期列是字符串格式
            df['date'] = df['date'].astype(str)
            
            return df.to_dict(orient='records')
        except Exception as e:
            logger.error(f"获取中证300股指期权波动率指数QVIX日线数据失败: {str(e)}")
            return []

    def get_300index_min_qvix(self) -> List[Dict]:
        """ 获取中证300股指期权波动率指数QVIX分钟线数据(带缓存支持)
        
        返回:
            List[Dict]: 包含中证300股指期权波动率指数QVIX分钟线数据
        """
        # 生成缓存键
        cache_key = "option_qvix:300index_min"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取中证300股指期权波动率指数QVIX分钟线数据: {cache_key}")
            # 检查是否需要更新（基于交易日变化）
            if self._need_incremental_update(cached_data):
                logger.info("检测到需要更新中证300股指期权波动率指数QVIX分钟线数据")
                # 获取最新数据
                new_data = self._fetch_300index_min_qvix()
                if new_data:
                    # 更新缓存
                    self.set_cached_data(cache_key, new_data, expiry=300)  # 实时数据缓存5分钟
                    logger.info("已更新中证300股指期权波动率指数QVIX分钟线数据缓存")
                    return new_data
                else:
                    # 如果获取新数据失败，返回缓存数据
                    logger.warning("获取最新数据失败，返回缓存数据")
                    return cached_data
            else:
                return cached_data

        try:
            # 获取实时数据
            result = self._fetch_300index_min_qvix()
            
            # 缓存数据5分钟
            if result:
                self.set_cached_data(cache_key, result, expiry=300)
            return result
        except Exception as e:
            logger.error(f"获取中证300股指期权波动率指数QVIX分钟线数据时发生错误：{str(e)}")
            return []

    def _fetch_300index_min_qvix(self) -> List[Dict]:
        """ 获取中证300股指期权波动率指数QVIX分钟线数据的实际实现 """
        try:
            df = index_option_qvix.index_option_300index_min_qvix(proxy=self.proxy)
            
            if df.empty:
                return []
            
            return df.to_dict(orient='records')
        except Exception as e:
            logger.error(f"获取中证300股指期权波动率指数QVIX分钟线数据失败: {str(e)}")
            return []

    def get_1000index_qvix(self) -> List[Dict]:
        """ 获取中证1000股指期权波动率指数QVIX日线数据(带缓存支持)
        
        返回:
            List[Dict]: 包含中证1000股指期权波动率指数QVIX日线数据
        """
        # 生成缓存键
        cache_key = "option_qvix:1000index"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取中证1000股指期权波动率指数QVIX日线数据: {cache_key}")
            # 检查是否需要更新（基于交易日变化）
            if self._need_incremental_update(cached_data):
                logger.info("检测到需要更新中证1000股指期权波动率指数QVIX日线数据")
                # 获取最新数据
                new_data = self._fetch_1000index_qvix()
                if new_data:
                    # 更新缓存
                    self.set_cached_data(cache_key, new_data, expiry=3600)
                    logger.info("已更新中证1000股指期权波动率指数QVIX日线数据缓存")
                    return new_data
                else:
                    # 如果获取新数据失败，返回缓存数据
                    logger.warning("获取最新数据失败，返回缓存数据")
                    return cached_data
            else:
                return cached_data

        try:
            # 获取实时数据
            result = self._fetch_1000index_qvix()
            
            # 缓存数据1小时
            if result:
                self.set_cached_data(cache_key, result, expiry=3600)
            return result
        except Exception as e:
            logger.error(f"获取中证1000股指期权波动率指数QVIX日线数据时发生错误：{str(e)}")
            return []

    def _fetch_1000index_qvix(self) -> List[Dict]:
        """ 获取中证1000股指期权波动率指数QVIX日线数据的实际实现 """
        try:
            df = index_option_qvix.index_option_1000index_qvix(proxy=self.proxy)
            
            if df.empty:
                return []
            
            # 确保日期列是字符串格式
            df['date'] = df['date'].astype(str)
            
            return df.to_dict(orient='records')
        except Exception as e:
            logger.error(f"获取中证1000股指期权波动率指数QVIX日线数据失败: {str(e)}")
            return []

    def get_1000index_min_qvix(self) -> List[Dict]:
        """ 获取中证1000股指期权波动率指数QVIX分钟线数据(带缓存支持)
        
        返回:
            List[Dict]: 包含中证1000股指期权波动率指数QVIX分钟线数据
        """
        # 生成缓存键
        cache_key = "option_qvix:1000index_min"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取中证1000股指期权波动率指数QVIX分钟线数据: {cache_key}")
            # 检查是否需要更新（基于交易日变化）
            if self._need_incremental_update(cached_data):
                logger.info("检测到需要更新中证1000股指期权波动率指数QVIX分钟线数据")
                # 获取最新数据
                new_data = self._fetch_1000index_min_qvix()
                if new_data:
                    # 更新缓存
                    self.set_cached_data(cache_key, new_data, expiry=300)  # 实时数据缓存5分钟
                    logger.info("已更新中证1000股指期权波动率指数QVIX分钟线数据缓存")
                    return new_data
                else:
                    # 如果获取新数据失败，返回缓存数据
                    logger.warning("获取最新数据失败，返回缓存数据")
                    return cached_data
            else:
                return cached_data

        try:
            # 获取实时数据
            result = self._fetch_1000index_min_qvix()
            
            # 缓存数据5分钟
            if result:
                self.set_cached_data(cache_key, result, expiry=300)
            return result
        except Exception as e:
            logger.error(f"获取中证1000股指期权波动率指数QVIX分钟线数据时发生错误：{str(e)}")
            return []

    def _fetch_1000index_min_qvix(self) -> List[Dict]:
        """ 获取中证1000股指期权波动率指数QVIX分钟线数据的实际实现 """
        try:
            df = index_option_qvix.index_option_1000index_min_qvix(proxy=self.proxy)
            
            if df.empty:
                return []
            
            return df.to_dict(orient='records')
        except Exception as e:
            logger.error(f"获取中证1000股指期权波动率指数QVIX分钟线数据失败: {str(e)}")
            return []

    def get_50index_qvix(self) -> List[Dict]:
        """ 获取上证50股指期权波动率指数QVIX日线数据(带缓存支持)
        
        返回:
            List[Dict]: 包含上证50股指期权波动率指数QVIX日线数据
        """
        # 生成缓存键
        cache_key = "option_qvix:50index"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取上证50股指期权波动率指数QVIX日线数据: {cache_key}")
            # 检查是否需要更新（基于交易日变化）
            if self._need_incremental_update(cached_data):
                logger.info("检测到需要更新上证50股指期权波动率指数QVIX日线数据")
                # 获取最新数据
                new_data = self._fetch_50index_qvix()
                if new_data:
                    # 更新缓存
                    self.set_cached_data(cache_key, new_data, expiry=3600)
                    logger.info("已更新上证50股指期权波动率指数QVIX日线数据缓存")
                    return new_data
                else:
                    # 如果获取新数据失败，返回缓存数据
                    logger.warning("获取最新数据失败，返回缓存数据")
                    return cached_data
            else:
                return cached_data

        try:
            # 获取实时数据
            result = self._fetch_50index_qvix()
            
            # 缓存数据1小时
            if result:
                self.set_cached_data(cache_key, result, expiry=3600)
            return result
        except Exception as e:
            logger.error(f"获取上证50股指期权波动率指数QVIX日线数据时发生错误：{str(e)}")
            return []

    def _fetch_50index_qvix(self) -> List[Dict]:
        """ 获取上证50股指期权波动率指数QVIX日线数据的实际实现 """
        try:
            df = index_option_qvix.index_option_50index_qvix(proxy=self.proxy)
            
            if df.empty:
                return []
            
            # 确保日期列是字符串格式
            df['date'] = df['date'].astype(str)
            
            return df.to_dict(orient='records')
        except Exception as e:
            logger.error(f"获取上证50股指期权波动率指数QVIX日线数据失败: {str(e)}")
            return []

    def get_50index_min_qvix(self) -> List[Dict]:
        """ 获取上证50股指期权波动率指数QVIX分钟线数据(带缓存支持)
        
        返回:
            List[Dict]: 包含上证50股指期权波动率指数QVIX分钟线数据
        """
        # 生成缓存键
        cache_key = "option_qvix:50index_min"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取上证50股指期权波动率指数QVIX分钟线数据: {cache_key}")
            # 检查是否需要更新（基于交易日变化）
            if self._need_incremental_update(cached_data):
                logger.info("检测到需要更新上证50股指期权波动率指数QVIX分钟线数据")
                # 获取最新数据
                new_data = self._fetch_50index_min_qvix()
                if new_data:
                    # 更新缓存
                    self.set_cached_data(cache_key, new_data, expiry=300)  # 实时数据缓存5分钟
                    logger.info("已更新上证50股指期权波动率指数QVIX分钟线数据缓存")
                    return new_data
                else:
                    # 如果获取新数据失败，返回缓存数据
                    logger.warning("获取最新数据失败，返回缓存数据")
                    return cached_data
            else:
                return cached_data

        try:
            # 获取实时数据
            result = self._fetch_50index_min_qvix()
            
            # 缓存数据5分钟
            if result:
                self.set_cached_data(cache_key, result, expiry=300)
            return result
        except Exception as e:
            logger.error(f"获取上证50股指期权波动率指数QVIX分钟线数据时发生错误：{str(e)}")
            return []

    def _fetch_50index_min_qvix(self) -> List[Dict]:
        """ 获取上证50股指期权波动率指数QVIX分钟线数据的实际实现 """
        try:
            df = index_option_qvix.index_option_50index_min_qvix(proxy=self.proxy)
            
            if df.empty:
                return []
            
            return df.to_dict(orient='records')
        except Exception as e:
            logger.error(f"获取上证50股指期权波动率指数QVIX分钟线数据失败: {str(e)}")
            return []