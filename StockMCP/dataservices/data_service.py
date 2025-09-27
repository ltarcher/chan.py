import logging
from datetime import datetime, timedelta
import traceback
from qstock.data import trade
from typing import Dict, List, Optional, Any
import pandas as pd

logger = logging.getLogger(__name__)

def get_date(item):
    date_str = item.get("日期") or item.get("date") or item.get("时间") or item.get("公布日")
    if isinstance(date_str, str):
        # 尝试解析不同的日期格式
        for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
    return datetime.min

def adjust_start_date(incremental_start_date: datetime)-> datetime:
    # 确定目标起始日期是否为交易日（周末、法定节假日不交易）
    if incremental_start_date.weekday() == 5:
        # 调整为后两个交易日
        incremental_start_date += timedelta(days=2)
    elif incremental_start_date.weekday() == 6:
        # 调整为后一个交易日
        incremental_start_date += timedelta(days=1)
    return incremental_start_date

def adjust_end_date(incremental_end_date: datetime)-> datetime:
    # 如果结束日期是周六周日，则调整为之前最近的工作日
    if incremental_end_date.weekday() == 5:
        incremental_end_date -= timedelta(days=1)  # 周六
    elif incremental_end_date.weekday() == 6:
        incremental_end_date -= timedelta(days=2)  # 周日
    return incremental_end_date

class DataService:
    """数据服务类，封装数据获取和缓存逻辑"""
    
    def __init__(self):
        try:
            from .cache_manager import get_cache_manager
            self.cache_manager = get_cache_manager()
        except ImportError:
            self.cache_manager = None
        # 初始化最后一个交易日
        self.last_trade_date = datetime.strptime(trade.latest_trade_date(), "%Y-%m-%d").date()
    
    def set_cache_manager(self, cache_manager):
        """设置缓存管理器"""
        self.cache_manager = cache_manager

    def get_cached_data(self, cache_key: str) -> Optional[Any]:
        """从缓存获取数据"""
        if self.cache_manager is None:
            return None
        return self.cache_manager.get_cached_data(cache_key)
    
    def set_cached_data(self, cache_key: str, data: Any, expiry: int = 3600) -> bool:
        """设置缓存数据"""
        if self.cache_manager is None:
            return False
        return self.cache_manager.set_cached_data(cache_key, data, expiry)
    
    def _need_incremental_update(self, cached_data: Any, start_date: Optional[str] = None, end_date: Optional[str] = None, freq: Optional[str|int] = 'd', market: Optional[str] = '沪深A') -> bool:
        """判断是否需要增量更新"""
        if cached_data is None or (not isinstance(cached_data, list) and not isinstance(cached_data, pd.DataFrame)):
            return False
            
        try:
            # 如果是DataFrame，转换为字典列表处理
            if isinstance(cached_data, pd.DataFrame):
                data_list = cached_data.to_dict(orient='records')
            else:
                data_list = cached_data
                
            if not data_list or not isinstance(data_list, list):
                # 缓存数据为空或无效，返回True
                return True
            
            # 获取缓存数据的最新日期和最早日期
            latest_cached_date = None
            earliest_cached_date = None
            for item in data_list:
                # 查找日期相关字段
                date_key = None
                date_value = None
                
                # 按优先级检查不同的日期字段
                for key in ["日期", "date", "时间", "公布日", "交易日期", "time"]:
                    if key in item:
                        date_key = key
                        date_value = item[key]
                        break
                
                if date_key is None or date_value is None:
                    continue
                    
                # 解析日期
                item_date = None
                if isinstance(date_value, str):
                    # 尝试不同的日期格式
                    for fmt in ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%H:%M:%S"]:
                        try:
                            item_date = datetime.strptime(date_value, fmt)
                            break
                        except ValueError:
                            continue
                elif isinstance(date_value, datetime):
                    item_date = date_value
                elif isinstance(date_value, datetime.date):
                    item_date = datetime.combine(date_value, datetime.time.min)
                    
                if item_date is None:
                    continue

                if date_key == "time":
                    # 如果是时间字段，设置为当天的日期
                    item_date = item_date.replace(hour=0, minute=0, second=0, microsecond=0)
                    freq = 1
                    
                # 根据freq调整日期精度
                item_date = self._adjust_date_by_freq(item_date, freq)
                
                if latest_cached_date is None or item_date > latest_cached_date:
                    latest_cached_date = item_date
                    
                if earliest_cached_date is None or item_date < earliest_cached_date:
                    earliest_cached_date = item_date
            
            if latest_cached_date is None or earliest_cached_date is None:
                return False
            
            # 确定目标结束日期
            target_end_date = datetime.now()

            # 获取最后一个交易日
            last_trading_day = datetime.strptime(trade.latest_trade_date(), "%Y-%m-%d")
            if target_end_date > last_trading_day:
                # 如果目标结束日期晚于最后一个交易日，则使用最后一个交易日作为结束日期
                target_end_date = last_trading_day

            if end_date:
                target_end_date = datetime.strptime(end_date, "%Y-%m-%d")
                target_end_date = adjust_end_date(target_end_date)
            else:
                target_end_date = adjust_end_date(target_end_date)

            # 考虑A股交易时间，将结束日期设为当天15:00
            if market == '沪深A':
                # 如果是A股，将结束日期设为当天15:00
                if freq in [1, 5, 15, 30, 60]:
                    target_end_date = target_end_date.replace(hour=15, minute=0, second=0, microsecond=0)
            elif market in ['港股']:
                # 如果是港股，将结束日期设为当天16:00
                if freq in [1, 5, 15, 30, 60]:
                    target_end_date = target_end_date.replace(hour=16, minute=0, second=0, microsecond=0)
                
            # 确定目标起始日期
            target_start_date = earliest_cached_date
            if start_date:
                target_start_date = datetime.strptime(start_date, "%Y-%m-%d")
                # 确定目标起始日期是否为交易日（周末、法定节假日不交易）
                target_start_date = adjust_start_date(target_start_date)
                    
                # 考虑A股交易时间，将开始日期设为当天9:30+freq
                if freq in [1, 5, 15]:
                    target_start_date = target_start_date.replace(hour=9, minute=30+freq, second=0, microsecond=0)
                elif freq in [30, 60]:
                    target_start_date = target_start_date.replace(hour=10, minute=freq-30, second=0, microsecond=0)
            else:
                target_start_date = adjust_start_date(target_start_date)

            # 根据freq调整日期精度
            target_end_date = self._adjust_date_by_freq(target_end_date, freq)
            target_start_date = self._adjust_date_by_freq(target_start_date, freq)
            latest_cached_date = self._adjust_date_by_freq(latest_cached_date, freq)
            earliest_cached_date = self._adjust_date_by_freq(earliest_cached_date, freq)

            # 如果缓存数据的最新日期早于目标结束日期，或者缓存数据的最早日期晚于目标起始日期，则需要增量更新
            return latest_cached_date < target_end_date or earliest_cached_date > target_start_date
        except Exception as e:
            logger.error(f"判断是否需要增量更新时出错: {e}")
            return False
    
    def _adjust_date_by_freq(self, date: datetime, freq: Optional[str|int] = 'd') -> datetime:
        """根据频率调整日期精度"""
        if freq in [1, 5, 15, 30, 60]:
            # 分钟级别数据，保留到分钟
            return date.replace(second=0, microsecond=0)
        elif freq == 'd' or freq == 'D':
            # 日级别数据，保留到日
            return date.replace(hour=0, minute=0, second=0, microsecond=0)
        elif freq == 'w' or freq == 'W':
            # 周级别数据，保留到周（周一）
            days_since_monday = date.weekday()
            return date.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days_since_monday)
        elif freq == 'm' or freq == 'M':
            # 月级别数据，保留到月首日
            return date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        else:
            # 默认按日处理
            return date.replace(hour=0, minute=0, second=0, microsecond=0)
        
    def _merge_and_deduplicate_data(self, cached_data: List[Dict], incremental_data: List[Dict]) -> List[Dict]:
        """合并并去重数据"""
        try:
            # 处理缓存数据的不同类型
            if isinstance(cached_data, pd.DataFrame):
                cached_data_list = cached_data.to_dict(orient='records')
            elif isinstance(cached_data, list):
                cached_data_list = cached_data
            else:
                logger.warning(f"Unexpected cached_data type: {type(cached_data)}")
                cached_data_list = list(cached_data) if cached_data else []
            
            # 处理增量数据的不同类型
            if isinstance(incremental_data, pd.DataFrame):
                incremental_data_list = incremental_data.to_dict(orient='records')
            elif isinstance(incremental_data, list):
                incremental_data_list = incremental_data
            else:
                logger.warning(f"Unexpected incremental_data type: {type(incremental_data)}")
                incremental_data_list = list(incremental_data) if incremental_data else []
            
            # 直接合并所有数据
            merged_list = cached_data_list + incremental_data_list
            
            # 转换为DataFrame以便去重
            if merged_list:
                df = pd.DataFrame(merged_list)
                # 去除完全重复的行
                df = df.drop_duplicates()
                # 转换回列表
                merged_list = df.to_dict(orient='records')
            
            # 按日期排序，升序
            merged_list.sort(key=get_date, reverse=False)

            # 把日期列转换为字符串格式，统一为%Y-%m-%d %H:%M:%S
            for item in merged_list:
                if '日期' in item:
                    if isinstance(item['日期'], datetime):
                        item['日期'] = item['日期'].strftime("%Y-%m-%d %H:%M:%S")
                    elif isinstance(item['日期'], str):
                        item['日期'] = get_date(item).strftime("%Y-%m-%d %H:%M:%S")
                if '时间' in item:
                    if isinstance(item['时间'], datetime):
                        item['时间'] = item['时间'].strftime("%Y-%m-%d %H:%M:%S")
                    elif isinstance(item['时间'], str):
                        item['时间'] = get_date(item).strftime("%Y-%m-%d %H:%M:%S")
                if 'date' in item:
                    if isinstance(item['date'], datetime):
                        item['date'] = item['date'].strftime("%Y-%m-%d %H:%M:%S")
                    elif isinstance(item['date'], str):
                        item['date'] = get_date(item).strftime("%Y-%m-%d %H:%M:%S")

            return merged_list
        except Exception as e:
            logger.error(f"合并数据时出错: {str(e)}")
            traceback.print_exc()
            # 返回原始缓存数据，确保返回类型一致性
            if isinstance(cached_data, pd.DataFrame):
                return cached_data.to_dict(orient='records')
            elif isinstance(cached_data, list):
                return cached_data
            else:
                return list(cached_data) if cached_data else []