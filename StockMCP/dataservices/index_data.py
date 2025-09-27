import logging
from typing import Dict, List, Optional, Any, Union
import pandas as pd
from datetime import datetime, timedelta
import requests
from pyquery import PyQuery as pq
import qstock as qs
import sys
import os
import traceback
from ..common_utils import get_stock_indicators

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import stock_rzrq as rzrq
from .data_service import DataService, get_date, adjust_start_date, adjust_end_date

logger = logging.getLogger(__name__)

class IndexDataService(DataService):
    """指数数据类，封装指数相关数据获取逻辑"""
    def __init__(self):
        super().__init__()
        
    def get_index_realtime_data(self, codes: Union[list, str], market: str = "沪深A") -> list:
        """ 获取中国金融市场指定指数或股票、期权、期货的实时数据
        参数：
        - codes: 指数或股票、期权、期货代码/名称，类型可以是字符串或列表；如果是字符串，多个代码用逗号分隔，为空时默认获取指定的市场指数
        - market: 市场类型，可选：["沪深A", "港股", "美股", "期货", "外汇", "债券"]，默认为沪深A
        """
        # 生成缓存键
        cache_key = f"index_realtime_data:{market}:{str(codes)}"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取指数实时数据: {cache_key}")
            # 检查是否需要增量更新
            if self._need_incremental_update(cached_data):
                logger.info("检测到需要更新指数实时数据")
                # 获取最新数据
                new_data = self._fetch_index_realtime_data(codes, market)
                if new_data:
                    # 更新缓存
                    self.set_cached_data(cache_key, new_data, expiry=1800)
                    logger.info("已更新指数实时数据缓存")
                    return new_data
                else:
                    # 如果获取新数据失败，返回缓存数据
                    logger.warning("获取最新数据失败，返回缓存数据")
                    return cached_data
            else:
                return cached_data

        # 如果缓存中没有数据，获取新数据
        result = self._fetch_index_realtime_data(codes, market)
        if result:
            # 缓存数据30分钟
            self.set_cached_data(cache_key, result, expiry=1800)
        return result
    
    def _fetch_index_realtime_data(self, codes: Union[list, str], market: str = "沪深A") -> list:
        """ 获取指数实时数据的实际实现 """
        if isinstance(codes, str):
            if not codes:
                index_codes = ['上证指数', '深证成指', '创业板指', '沪深300', '中证500', '上证50', '中证1000', '科创50', "恒生指数", "恒生科技指数"]  # 默认上证指数
            else:
                index_codes = codes.split(',')
        elif isinstance(codes, list):
            index_codes = codes
        else:
            logger.error("参数 'codes' 必须是字符串或列表")
            return []
        
        # 对market参数进行处理
        if market not in ["沪深A", "港股", "美股", "期货", "外汇", "债券"]:
            logger.info(f"当前输入的市场: {market}")
            logger.warning(f"参数 'market' 必须是以下值之一: 沪深A, 港股, 美股, 期货, 外汇, 债券")
            market = "沪深A"  # 默认市场为沪深A
        
        # 使用qstock获取指数实时数据
        try:
            df = qs.realtime_data(market=market, code=index_codes)
            logger.info(f"获取指数实时数据: {df}")
            if df.empty:
                logger.warning("获取的指数数据为空")
                return []
            logger.info(f"指数数据获取成功: {df.shape[0]} 条记录")
            logger.info(f"指数数据列名: {df.columns.tolist()}")
            
            result = df.to_dict(orient='records')
            return result
        except Exception as e:
            logger.error(f"获取指数实时数据失败: {str(e)}")
            # traceback.print_exc()
        return []
    
    def get_board_trade_realtime_data(self) -> Dict:
        """ 获取沪深京三市场的总成交数据
        返回：
        - Dict: 包含日期和总成交额的字典，格式为 {'日期': 日期, '总成交额': 总成交额'}
        """
        # 生成缓存键
        cache_key = "board_trade_realtime_data"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取成交数据: {cache_key}")
            # 检查是否需要增量更新
            if self._need_incremental_update(cached_data):
                logger.info("检测到需要更新成交数据")
                # 获取最新数据
                new_data = self._fetch_board_trade_realtime_data()
                if new_data:
                    # 更新缓存
                    self.set_cached_data(cache_key, new_data, expiry=600)
                    logger.info("已更新成交数据缓存")
                    return new_data
                else:
                    # 如果获取新数据失败，返回缓存数据
                    logger.warning("获取最新数据失败，返回缓存数据")
                    return cached_data
            else:
                return cached_data

        # 如果缓存中没有数据，获取新数据
        result = self._fetch_board_trade_realtime_data()
        if result:
            # 缓存数据10分钟
            self.set_cached_data(cache_key, result, expiry=600)
        return result
    
    def _fetch_board_trade_realtime_data(self) -> Dict:
        """ 获取成交数据的实际实现 """
        try:
            realtime_df = qs.realtime_data(market="沪深A", code=["上证指数", "深证成指", "北证50"])
            # 把时间列转换为日期YYYY-MM-DD格式
            realtime_df["日期"] = pd.to_datetime(realtime_df["时间"]).dt.date
            # 确保日期列是字符串格式
            if '日期' in realtime_df.columns:
                realtime_df['日期'] = realtime_df['日期'].astype(str)
            total_turnover = realtime_df["成交额"].sum()
            result = {"日期": realtime_df["日期"].iloc[0], "总成交额": total_turnover}
            return result
        except Exception as e:
            logger.error(f"获取成交数据失败: {str(e)}")
            # traceback.print_exc()
            return {}
    
    def get_turnover_history_data(self, start_date: Optional[str] = '19000101', end_date: Optional[str] = None, 
                                  freq: Optional[str|int] = 'd', fqt: Optional[int] = 1, use_chinese_fields: bool = True) -> List[Dict]:
        """ 获取沪深京三市场的历史总成交数据
        参数：
        - start_date: 开始日期，格式为'YYYY-MM-DD'，默认为None
        - end_date: 结束日期，格式为'YYYY-MM-DD'，默认为None
        - freq: 数据频率，'d'表示日频，'m'表示月频，默认为日频
        - fqt: 复权类型，默认为1，可选值为0，1，2
        - use_chinese_fields: 是否使用中文字段名，默认为True
        """
        # 生成缓存键
        cache_key = f"turnover_history_data:{freq}:{fqt}:{use_chinese_fields}"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取历史成交数据: {cache_key}")
            # 检查是否需要增量更新
            if self._need_incremental_update(cached_data, start_date=start_date, end_date= end_date, freq=freq):
                logger.info("检测到需要增量更新成交历史数据")
                incremental_data = self._get_incremental_turnover_data(cached_data, start_date=start_date, end_date=end_date, freq=freq, fqt=fqt, use_chinese_fields=use_chinese_fields)
                if incremental_data:
                    # 合并数据
                    merged_data = self._merge_and_deduplicate_data(cached_data, incremental_data)
                    # 更新缓存
                    self.set_cached_data(cache_key, merged_data, expiry=3600)
                    logger.info(f"更新缓存数据，新增{len(incremental_data)}条记录")
                    return merged_data
            
            return cached_data

        try:
            # 调用统一封装的方法获取数据
            result = self._fetch_turnover_impl(start_date, end_date, freq, fqt, use_chinese_fields)
            
            # 缓存数据1小时
            self.set_cached_data(cache_key, result, expiry=3600)
            return result
        except Exception as e:
            logger.error(f"获取成交数据失败: {str(e)}")
            # traceback.print_exc()
            return []
    
    def _fetch_turnover_impl(self, start_date: Optional[str], end_date: Optional[str], freq : Optional[str|int] = 'd', fqt: Optional[int] = 1,
                             use_chinese_fields: bool = True) -> List[Dict]:
        """统一封装的获取成交数据方法
        
        参数：
        - start_date: 开始日期，格式为'YYYY-MM-DD'
        - end_date: 结束日期，格式为'YYYY-MM-DD'
        - freq: 数据频率，'d'表示日频，'m'表示月频，默认为日频
        - fqt: 复权类型，默认为1，可选值为0，1，2
        - use_chinese_fields: 是否使用中文字段名
        """
        hisdata = qs.get_data(code_list=["上证指数", "深证成指", "北证50"], 
                              start=start_date.replace('-', '') if start_date else '19000101', 
                              end=end_date.replace('-', '') if end_date else None,
                              freq=freq, fqt=fqt)
        hisdata = hisdata.reset_index()

        # 如果是1分钟的数据，检查vol列，改名为volume
        if freq == 1:
            if 'vol' in hisdata.columns:
                hisdata.rename(columns={'vol': 'volume'}, inplace=True)

        # 按照date列进行分组，计算每个日期的成交额总和
        grouped_data = hisdata.groupby("date")["turnover"].sum().reset_index()
        if use_chinese_fields:
            grouped_data = grouped_data.rename(columns={"date": "日期", "turnover": "总成交额"})
        # 把日期列转为字符串格式
        if '日期' in grouped_data.columns:
            grouped_data["日期"] = grouped_data["日期"].astype(str)
        if 'date' in grouped_data.columns:
            grouped_data["date"] = grouped_data["date"].astype(str)

        return grouped_data.to_dict(orient='records')
    
    def _get_rzrq_turnover_ratio_impl(self, start_date='19000101', end_date=None, page: int = 1, page_size: int = 10, 
                                use_chinese_fields: bool = True) -> List[Dict]:
        try:
            hisdata = qs.get_data(code_list=["上证指数", "深证成指", "北证50"], 
                                  start=start_date.replace('-', '') if start_date else '19000101', 
                                  end=end_date.replace('-', '') if end_date else None)
            hisdata = hisdata.reset_index()

            szzs_df = hisdata[hisdata["name"] == "上证指数"]
            szzs_df.reset_index(inplace=True, drop=True)

            # 按照date列进行分组，计算每个日期的成交额总和
            grouped_data = hisdata.groupby("date")["turnover"].sum().reset_index()
            grouped_data_df = pd.DataFrame(grouped_data)
            #统一日期列
            grouped_data_df["日期"] = pd.to_datetime(grouped_data_df["date"]).dt.date

            # 获取融资融券历史数据
            data = rzrq.get_rzrq_history(start_date=start_date, end_date=end_date, page=page, page_size=page_size, use_chinese_fields=True)
            rzrq_df = pd.DataFrame(data.get('result', {}).get('data', []))
            if not rzrq_df.empty:
                # 把日期列转换为日期格式
                rzrq_df["日期"] = pd.to_datetime(rzrq_df["交易日期"]).dt.date

            # 把grouped_data_df和rzrq_df按照日期列进行合并
            result_df = pd.merge(grouped_data_df, rzrq_df, on="日期", how="left")
            result_df["上证指数"] = szzs_df["close"]

            # 按照日期计算融资融券成交比例
            rzrq_turnover_ratio = (result_df["总融资买入额"] + result_df["总融券余额"]) / result_df["turnover"] * 100

            # 融资融券额额与上证指数偏离率
            rzrq_szzs_ratio = (result_df["总融资买入额"] + result_df["总融券余额"]) / 100000000 / result_df["上证指数"] * 100

            rzrq_turnover_df = pd.DataFrame(rzrq_turnover_ratio, columns=["融资融券成交比例"])
            rzrq_turnover_df["总融资余额与上证指数偏离率"] = rzrq_szzs_ratio
            rzrq_turnover_df["上证指数"] = result_df["上证指数"]
            rzrq_turnover_df.index = result_df["日期"]

            # 删除总融资余额与上证指数偏离率列包含Nan值的行，日期列不为空的行
            rzrq_turnover_df = rzrq_turnover_df.dropna(subset=["总融资余额与上证指数偏离率"])

            rzrq_turnover_df.reset_index(inplace=True)
            
            rzrq_turnover_df["日期"] = rzrq_turnover_df["日期"].astype(str)
            result = rzrq_turnover_df.to_dict(orient='records')
            return result
        except Exception as e:
            logger.error(f"获取融资融券成交比例数据失败: {str(e)}")
            # traceback.print_exc()
            return []

    def get_rzrq_turnover_ratio(self, start_date='19000101', end_date=None, page: int = 1, page_size: int = 10, 
                                use_chinese_fields: bool = True) -> List[Dict]:
        """ 获取融资融券占总成交比例数据(含总融资余额与上证指数偏离率)
        参数：
        - start_date: 开始日期，格式为'YYYY-MM-DD'，默认为19000101
        - end_date: 结束日期，格式为'YYYY-MM-DD'，默认为None
        - page: 页码，默认为1
        - page_size: 每页数量，默认为10
        - use_chinese_fields: 是否使用中文字段名，默认为True
        """
        # 生成缓存键
        cache_key = f"rzrq_turnover_ratio:{use_chinese_fields}"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取融资融券数据: {cache_key}")
            # 检查是否需要增量更新
            if self._need_incremental_update(cached_data, start_date=start_date, end_date=end_date):
                logger.info("检测到需要增量更新融资融券数据")
                incremental_data = self._get_incremental_rzrq_data(cached_data, start_date=start_date, end_date=end_date, page=page, page_size=page_size, use_chinese_fields=use_chinese_fields)
                if incremental_data:
                    # 合并数据
                    merged_data = self._merge_and_deduplicate_data(cached_data, incremental_data)
                    # 更新缓存
                    self.set_cached_data(cache_key, merged_data, expiry=7200)
                    logger.info(f"更新缓存数据，新增{len(incremental_data)}条记录")
                    return merged_data
            
            return cached_data

        try: 
            result = self._get_rzrq_turnover_ratio_impl(start_date, end_date, page, page_size, use_chinese_fields)
            # 缓存数据2小时
            self.set_cached_data(cache_key, result, expiry=7200)
            return result
        except Exception as e:
            logger.error(f"获取融资融券成交比例数据失败: {str(e)}")
            # traceback.print_exc()
            return []
    
    def get_usd_index_data(self) -> list:
        """ 获取美元指数实时数据 """
        # 生成缓存键
        cache_key = "usd_index_data"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取美元指数数据: {cache_key}")
            # 检查是否需要增量更新
            if self._need_incremental_update(cached_data):
                logger.info("检测到需要更新美元指数数据")
                # 获取最新数据
                new_data = self._fetch_usd_index_data()
                if new_data:
                    # 更新缓存
                    self.set_cached_data(cache_key, new_data, expiry=1800)
                    logger.info("已更新美元指数数据缓存")
                    return new_data
                else:
                    # 如果获取新数据失败，返回缓存数据
                    logger.warning("获取最新数据失败，返回缓存数据")
                    return cached_data
            else:
                return cached_data
        
        # 使用qstock获取美元指数实时数据
        result = self._fetch_usd_index_data()
        if result:
            # 缓存数据30分钟
            self.set_cached_data(cache_key, result, expiry=1800)
        return result
    
    def _fetch_usd_index_data(self) -> list:
        """ 获取美元指数实时数据的实际实现 """
        try:
            # 使用美股市场获取美元指数数据
            df = qs.realtime_data(market="美股", code="美元指数")
            if not df.empty:
                logger.info(f"美元指数数据获取成功: {df.shape[0]} 条记录")
                logger.info(f"美元指数数据列名: {df.columns.tolist()}")
                
                result = df.to_dict(orient='records')
                return result
        except Exception as e:
            logger.error(f"获取美元指数实时数据失败: {str(e)}")
            # traceback.print_exc()
        return []
    
    def get_ftse_a50_futures_data(self) -> list:
        """ 获取富时A50期货指数实时数据 """
        # 生成缓存键
        cache_key = "ftse_a50_futures_data"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取富时A50期货数据: {cache_key}")
            # 检查是否需要增量更新
            if self._need_incremental_update(cached_data):
                logger.info("检测到需要更新富时A50期货数据")
                # 获取最新数据
                new_data = self._fetch_ftse_a50_futures_data()
                if new_data:
                    # 更新缓存
                    self.set_cached_data(cache_key, new_data, expiry=1800)
                    logger.info("已更新富时A50期货数据缓存")
                    return new_data
                else:
                    # 如果获取新数据失败，返回缓存数据
                    logger.warning("获取最新数据失败，返回缓存数据")
                    return cached_data
            else:
                return cached_data

        # 使用qstock获取富时A50期货指数实时数据
        result = self._fetch_ftse_a50_futures_data()
        if result:
            # 缓存数据30分钟
            self.set_cached_data(cache_key, result, expiry=1800)
        return result
    
    def _fetch_ftse_a50_futures_data(self) -> list:
        """ 获取富时A50期货指数实时数据的实际实现 """
        try:
            df = qs.realtime_data(market="期货", code="富时A50期指主连")
            if not df.empty:    
                logger.info(f"富时A50期货指数数据获取成功: {df.shape[0]} 条记录")
                logger.info(f"富时A50期货指数数据列名: {df.columns.tolist()}")
                
                result = df.to_dict(orient='records')
                return result
        except Exception as e:
            logger.error(f"获取富时A50期货指数实时数据失败: {str(e)}")
            # traceback.print_exc()
        return []
    
    def get_usd_cnh_futures_data(self) -> list:
        """ 获取美元兑离岸人民币主连实时数据 """
        # 生成缓存键
        cache_key = "usd_cnh_futures_data"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取美元兑离岸人民币数据: {cache_key}")
            # 检查是否需要增量更新
            if self._need_incremental_update(cached_data):
                logger.info("检测到需要更新美元兑离岸人民币数据")
                # 获取最新数据
                new_data = self._fetch_usd_cnh_futures_data()
                if new_data:
                    # 更新缓存
                    self.set_cached_data(cache_key, new_data, expiry=1800)
                    logger.info("已更新美元兑离岸人民币数据缓存")
                    return new_data
                else:
                    # 如果获取新数据失败，返回缓存数据
                    logger.warning("获取最新数据失败，返回缓存数据")
                    return cached_data
            else:
                return cached_data

        # 使用qstock获取美元兑离岸人民币主连实时数据
        result = self._fetch_usd_cnh_futures_data()
        if result:
            # 缓存数据30分钟
            self.set_cached_data(cache_key, result, expiry=1800)
        return result
    
    def _fetch_usd_cnh_futures_data(self) -> list:
        """ 获取美元兑离岸人民币主连实时数据的实际实现 """
        try:
            df = qs.realtime_data(market="期货", code="美元兑离岸人民币")
            if not df.empty:             
                logger.info(f"美元兑离岸人民币主连数据获取成功: {df.shape[0]} 条记录")
                logger.info(f"美元兑离岸人民币主连数据列名: {df.columns.tolist()}")
                
                result = df.to_dict(orient='records')
                return result
        except Exception as e:
            logger.error(f"获取美元兑离岸人民币主连实时数据失败: {str(e)}")
            # traceback.print_exc()
        return []
    
    def get_thirty_year_bond_futures_data(self) -> list:
        """ 获取三十年国债主连实时数据 """
        # 生成缓存键
        cache_key = "thirty_year_bond_futures_data"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取三十年国债数据: {cache_key}")
            # 检查是否需要增量更新
            if self._need_incremental_update(cached_data):
                logger.info("检测到需要更新三十年国债数据")
                # 获取最新数据
                new_data = self._fetch_thirty_year_bond_futures_data()
                if new_data:
                    # 更新缓存
                    self.set_cached_data(cache_key, new_data, expiry=1800)
                    logger.info("已更新三十年国债数据缓存")
                    return new_data
                else:
                    # 如果获取新数据失败，返回缓存数据
                    logger.warning("获取最新数据失败，返回缓存数据")
                    return cached_data
            else:
                return cached_data

        # 使用qstock获取三十年国债主连实时数据
        result = self._fetch_thirty_year_bond_futures_data()
        if result:
            # 缓存数据30分钟
            self.set_cached_data(cache_key, result, expiry=1800)
        return result
    
    def _fetch_thirty_year_bond_futures_data(self) -> list:
        """ 获取三十年国债主连实时数据的实际实现 """
        try:
            df = qs.realtime_data(market="期货", code="三十年国债主连")
            if not df.empty:         
                logger.info(f"三十年国债主连数据获取成功: {df.shape[0]} 条记录")
                logger.info(f"三十年国债主连数据列名: {df.columns.tolist()}")
                
                result = df.to_dict(orient='records')
                return result
        except Exception as e:
            logger.error(f"获取三十年国债主连实时数据失败: {str(e)}")
            # traceback.print_exc()
        return []
    
    def get_economic_calendar(self, start_date: str = None, end_date: str = None, country: str = None) -> List[Dict]:
        """ 获取未来7天的全球经济报告日历
        参数：
        - start_date: 开始日期，格式为'YYYY-MM-DD'，默认为None（表示当前日期）
        - end_date: 结束日期，格式为'YYYY-MM-DD'，默认为None（表示当前日期+7天）
        - country: 国家代码，默认为None（表示所有国家）
        """
        # 生成缓存键
        cache_key = f"economic_calendar:{country}"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取经济日历数据: {cache_key}")
            # 检查是否需要增量更新
            if self._need_incremental_update(cached_data, start_date=start_date, end_date=end_date):
                logger.info("检测到需要更新经济日历数据")
                # 获取最新数据
                new_data = self._fetch_economic_calendar(start_date, end_date, country)
                if new_data:
                    # 更新缓存
                    self.set_cached_data(cache_key, new_data, expiry=3600)
                    logger.info("已更新经济日历数据缓存")
                    return new_data
                else:
                    # 如果获取新数据失败，返回缓存数据
                    logger.warning("获取最新数据失败，返回缓存数据")
                    return cached_data
            else:
                return cached_data

        result = self._fetch_economic_calendar(start_date, end_date, country)
        if result:
            # 缓存数据1小时
            self.set_cached_data(cache_key, result, expiry=3600)
        return result
    
    def _fetch_economic_calendar(self, start_date: str = None, end_date: str = None, country: str = None) -> List[Dict]:
        """ 获取全球经济报告日历的实际实现 """
        try:
            if not start_date:
                start_date = datetime.now().strftime('%Y-%m-%d')
            if not end_date:
                end_date = (datetime.now() + timedelta(days=7)).strftime('%Y-%m-%d')
            
            url = f"https://forex.eastmoney.com/fc.html?Date={start_date},{end_date}&country={country if country else ''}"
            response = requests.get(url)
            response.raise_for_status()
            
            doc = pq(response.text)
            events = []
            
            for row in doc('.dataList tr').items():
                if row.attr('class') == 'title':
                    continue
                
                seq = row('td:nth-child(1)').text()
                date = row('td:nth-child(2)').text()
                time = row('td:nth-child(3)').text()
                country_name = row('td:nth-child(4)').text()
                event = row('td:nth-child(5)').text()
                report_period = row('td:nth-child(6)').text()
                actual = row('td:nth-child(7)').text()
                forecast = row('td:nth-child(8)').text()
                previous = row('td:nth-child(9)').text()
                importance = row('td:nth-child(10)').text()
                trend = row('td:nth-child(11)').text()

                if not seq:
                    # 无数据忽略
                    continue
                
                events.append({
                    '序号': seq,
                    '公布日': date,
                    '时间': time,
                    '国家/地区': country_name,
                    '事件': event,
                    '报告期': report_period,
                    '公布值': actual,
                    '预测值': forecast,
                    '前值': previous,
                    '重要性': importance,
                    '趋势': trend
                })
            
            return events
        except Exception as e:
            logger.error(f"获取经济日历数据失败: {str(e)}")
            # traceback.print_exc()
            return []
    
    def _get_incremental_turnover_data(self, cached_data: List[Dict], start_date: Optional[str], 
                                       end_date: Optional[str], freq: Optional[str|int] = 'd', fqt: Optional[int] = 1,
                                       use_chinese_fields: bool = True) -> List[Dict]:
        """获取增量的成交历史数据"""
        try:
            # 处理不同类型的数据
            if isinstance(cached_data, pd.DataFrame):
                # 如果是DataFrame类型，转换为字典列表
                cached_data_list = cached_data.to_dict(orient='records')
            elif isinstance(cached_data, list):
                # 如果已经是列表格式
                cached_data_list = cached_data
            else:
                # 其他情况尝试转换为列表
                logger.warning(f"Unexpected cached_data type: {type(cached_data)}")
                cached_data_list = list(cached_data) if cached_data else []
            
            # 获取缓存数据的最新日期
            latest_cached_date = None
            # 获取缓存数据的最早日期
            earliest_cached_date = None
            
            for item in cached_data_list:
                if "日期" in item:
                    if isinstance(item["日期"], str):
                        item_date = get_date(item)
                    elif isinstance(item["日期"], datetime):
                        item_date = item["日期"]
                    else:
                        continue
                elif "date" in item:
                    if isinstance(item["date"], str):
                        item_date = get_date(item)
                    elif isinstance(item["date"], datetime):
                        item_date = item["date"]
                    else:
                        continue
                else:
                    return []
                    
                # 根据freq调整日期精度
                item_date = self._adjust_date_by_freq(item_date, freq)
                
                if latest_cached_date is None or item_date > latest_cached_date:
                    latest_cached_date = item_date
                    
                if earliest_cached_date is None or item_date < earliest_cached_date:
                    earliest_cached_date = item_date
            
            # 确定增量开始日期：如果指定了start_date且早于缓存中的最早日期，则使用start_date
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
                # 如果缓存中没有数据，则从开始日期开始获取数据  
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
                incremental_end_date = earliest_cached_date- timedelta(days=1)
            
            # 根据freq调整日期精度
            incremental_start_date = self._adjust_date_by_freq(incremental_start_date, freq)
            incremental_end_date = self._adjust_date_by_freq(incremental_end_date, freq)
            
            # 如果不需要获取更早的数据且最新缓存日期已是最新的，则返回空列表
            if latest_cached_date and incremental_start_date > incremental_end_date:
                return []
            
            incremental_start_str = incremental_start_date.strftime("%Y-%m-%d")
            incremental_end_str = incremental_end_date.strftime("%Y-%m-%d")

            logger.info(f"获取指定日期范围（{incremental_start_str}至{incremental_end_str}）的成交数据")
            
            # 获取增量的融资融券占比数据
            data = self._fetch_turnover_impl(start_date=incremental_start_str, 
                                         end_date=incremental_end_str, 
                                         freq=freq, fqt=fqt, use_chinese_fields=use_chinese_fields)
            return data
        except Exception as e:
            logger.error(f"获取增量成交数据失败: {str(e)}")
            traceback.print_exc()
            return []
    
    def _get_incremental_rzrq_data(self, cached_data: List[Dict], start_date: Optional[str], 
                                   end_date: Optional[str], page: Optional[int] = 1, page_size: Optional[int] = 100, 
                                   use_chinese_fields: bool = True) -> List[Dict]:
        """获取增量的融资融券数据"""
        try:
            # 处理不同类型的数据
            if isinstance(cached_data, pd.DataFrame):
                # 如果是DataFrame类型，转换为字典列表
                cached_data_list = cached_data.to_dict(orient='records')
            elif isinstance(cached_data, list):
                # 如果已经是列表格式
                cached_data_list = cached_data
            else:
                # 其他情况尝试转换为列表
                logger.warning(f"Unexpected cached_data type: {type(cached_data)}")
                cached_data_list = list(cached_data) if cached_data else []
            
            # 获取缓存数据的最新日期
            latest_cached_date = None
            # 获取缓存数据的最早日期
            earliest_cached_date = None
            
            for item in cached_data_list:
                if "日期" in item:
                    if isinstance(item["日期"], str):
                        item_date = get_date(item)
                    elif isinstance(item["日期"], datetime):
                        item_date = item["日期"]
                    else:
                        continue
                elif "date" in item:
                    if isinstance(item["date"], str):
                        item_date = get_date(item)
                    elif isinstance(item["date"], datetime):
                        item_date = item["date"]
                    else:
                        continue
                else:
                    return []
                    
                # 根据freq调整日期精度（融资融券数据默认为日频）
                item_date = self._adjust_date_by_freq(item_date, 'd')
                
                if latest_cached_date is None or item_date > latest_cached_date:
                    latest_cached_date = item_date
                    
                if earliest_cached_date is None or item_date < earliest_cached_date:
                    earliest_cached_date = item_date
            
            # 确定增量开始日期：如果指定了start_date且早于缓存中的最早日期，则使用start_date
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
                # 如果缓存中没有数据，则从开始日期开始获取数据  
                incremental_start_date = requested_start_date
            
            # 增量结束日期为今天或指定的结束日期
            incremental_end_date = datetime.now()

            if end_date:
                incremental_end_date = datetime.strptime(end_date, "%Y-%m-%d")
            else:
                incremental_end_date = adjust_end_date(incremental_end_date)
            # 如果请求的结束时间比缓存中最早的日期晚，则只需要获取更早的数据
            if earliest_cached_date and incremental_end_date > earliest_cached_date:
                incremental_end_date = earliest_cached_date- timedelta(days=1)

            # 根据freq调整日期精度（融资融券数据默认为日频）
            incremental_start_date = self._adjust_date_by_freq(incremental_start_date, 'd')
            incremental_end_date = self._adjust_date_by_freq(incremental_end_date, 'd')
            
            # 如果不需要获取更早的数据且最新缓存日期已是最新的，则返回空列表
            if latest_cached_date and incremental_start_date > incremental_end_date:
                return []
            
            incremental_start_str = incremental_start_date.strftime("%Y-%m-%d")
            incremental_end_str = incremental_end_date.strftime("%Y-%m-%d")

            logger.info(f"获取指定日期范围（{incremental_start_str}至{incremental_end_str}）的融资融券占比数据")
            
            # 获取增量的融资融券占比数据
            data = self._get_rzrq_turnover_ratio_impl(start_date=incremental_start_str, 
                                         end_date=incremental_end_str, 
                                         page=page, page_size=page_size, use_chinese_fields=use_chinese_fields)
            return data
        except Exception as e:
            logger.error(f"获取增量融资融券数据失败: {str(e)}")
            return []

    def get_stock_history_data(self, code: Union[list, str], freq: Union[int, str] = "d", fqt: int=1, start_date: str = '19000101', end_date: str = None, indicator: bool = False) -> list:
        """ 获取指定股票代码的历史数据（带缓存支持）
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
        """
        # 生成缓存键
        cache_key = f"stock_history_data:{str(code)}:{freq}:{fqt}:{indicator}"
        
        # 尝试从缓存获取数据
        cached_data = self.get_cached_data(cache_key)
        if cached_data is not None:
            logger.info(f"从缓存获取股票历史数据: {cache_key}")
            # 检查是否需要增量更新
            if self._need_incremental_update(cached_data, start_date=start_date, end_date=end_date, freq=freq):
                logger.info("检测到需要增量更新股票历史数据")
                incremental_data = self._get_incremental_stock_data(cached_data, code, freq, fqt, start_date, end_date, indicator)
                if incremental_data:
                    # 合并数据
                    merged_data = self._merge_and_deduplicate_data(cached_data, incremental_data)
                    # 更新缓存
                    self.set_cached_data(cache_key, merged_data, expiry=3600)
                    logger.info(f"更新缓存数据，新增{len(incremental_data)}条记录")
                    return merged_data
            
            return cached_data

        try:
            # 调用统一封装的方法获取数据
            result = self._fetch_stock_history_impl(code, freq, fqt, start_date, end_date, indicator)
            
            # 缓存数据1小时
            self.set_cached_data(cache_key, result, expiry=3600)
            return result
        except Exception as e:
            traceback.print_exc()
            logger.error(f"获取股票历史数据失败: {str(e)}")
            return []

    def _fetch_stock_history_impl(self, code: Union[list,str], freq: Union[int, str] = "d", fqt: int=1, start_date: str = '19000101', end_date: str = None, indicator: bool = False) -> list:
        """统一封装的获取股票历史数据方法"""
        # 参数验证
        if not code:
            logger.error("参数 'code' 不能为空")
            return []
        
        # 验证频率参数
        valid_freqs = [1, 5, 15, 30, 60, 'd', 'D', 'w', 'W', 'm', 'M', 101, 102, 103]
        try:
            freq = int(freq)
        except ValueError:
            pass
        if freq not in valid_freqs:
            logger.warning(f"参数 'freq' 必须是以下值之一: {', '.join([str(v) for v in valid_freqs])}，当前值为: {freq}，将使用默认值'daily'")
            freq = "d"
        
        # 使用qstock获取股票历史数据
        try:
            # 调用qstock.get_data函数获取历史数据
            df = qs.get_data(code_list=code, start=start_date, end=end_date, freq=freq, fqt=fqt)
            
            logger.info(f"获取股票 {code} 的历史数据: {df}")
            if df.empty:
                logger.warning(f"获取的股票 {code} 历史数据为空")
                return []
            
            # 如果是1分钟的数据，检查vol列，改名为volume
            if freq == 1:
                if 'vol' in df.columns:
                    df.rename(columns={'vol': 'volume'}, inplace=True)
            
            # 如果需要计算技术指标，则调用get_stock_indicators函数计算技术指标
            if indicator:
                df = get_stock_indicators(df)

            # 重置索引
            df = df.reset_index()
                    
            logger.info(f"股票 {code} 历史数据获取成功: {df.shape[0]} 条记录")
            logger.info(f"股票历史数据列名: {df.columns.tolist()}")
            
            # 确保日期列是字符串格式
            if '日期' in df.columns:
                df['日期'] = df['日期'].astype(str)
            if 'date' in df.columns:
                df['date'] = df['date'].astype(str)

            return df.to_dict(orient='records')
        except Exception as e:
            logger.error(f"获取股票 {code} 的历史数据失败: {str(e)}")
            traceback.print_exc()
        return []

    def _get_incremental_stock_data(self, cached_data: List[Dict], code: Union[list, str], freq: str = "d", fqt: int=1, 
                                    start_date: str = '19000101', end_date: str = None, indicator: bool = False) -> List[Dict]:
        """获取增量的股票历史数据"""
        try:
            # 处理不同类型的数据
            if isinstance(cached_data, pd.DataFrame):
                # 如果是DataFrame类型，转换为字典列表
                cached_data_list = cached_data.to_dict(orient='records')
            elif isinstance(cached_data, list):
                # 如果已经是列表格式
                cached_data_list = cached_data
            else:
                # 其他情况尝试转换为列表
                logger.warning(f"Unexpected cached_data type: {type(cached_data)}")
                cached_data_list = list(cached_data) if cached_data else []
            
            # 获取缓存数据的最新日期
            latest_cached_date = None
            # 获取缓存数据的最早日期
            earliest_cached_date = None
            
            for item in cached_data_list:
                if "日期" in item:
                    if isinstance(item["日期"], str):
                        item_date = get_date(item)
                    elif isinstance(item["日期"], datetime):
                        item_date = item["日期"]
                    else:
                        continue
                elif "date" in item:
                    if isinstance(item["date"], str):
                        item_date = get_date(item)
                    elif isinstance(item["date"], datetime):
                        item_date = item["date"]
                    else:
                        continue
                else:
                    return []
                    
                # 根据freq调整日期精度
                item_date = self._adjust_date_by_freq(item_date, freq)
                
                if latest_cached_date is None or item_date > latest_cached_date:
                    latest_cached_date = item_date
                    
                if earliest_cached_date is None or item_date < earliest_cached_date:
                    earliest_cached_date = item_date
            
            # 确定增量开始日期：如果指定了start_date且早于缓存中的最早日期，则使用start_date
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
                # 如果缓存中没有数据，则从开始日期开始获取数据  
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
            
            # 根据freq调整日期精度
            incremental_start_date = self._adjust_date_by_freq(incremental_start_date, freq)
            incremental_end_date = self._adjust_date_by_freq(incremental_end_date, freq)
            
            # 如果不需要获取更早的数据且最新缓存日期已是最新的，则返回空列表
            if latest_cached_date and incremental_start_date > incremental_end_date:
                return []
            
            incremental_start_str = incremental_start_date.strftime("%Y-%m-%d")
            incremental_end_str = incremental_end_date.strftime("%Y-%m-%d")

            logger.info(f"获取指定日期范围（{incremental_start_str}至{incremental_end_str}）的股票数据")
            
            # 获取增量的股票历史数据
            data = self._fetch_stock_history_impl(code=code, 
                                                  freq=freq, fqt=fqt, 
                                                  start_date=incremental_start_str, 
                                                  end_date=incremental_end_str, 
                                                  indicator=indicator)
            return data
        except Exception as e:
            logger.error(f"获取增量股票数据失败: {str(e)}")
            traceback.print_exc()
            return []
    