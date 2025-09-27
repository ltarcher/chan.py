import redis
import json
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import sqlite3
import pandas as pd

logger = logging.getLogger(__name__)

class DatabaseInterface(ABC):
    """数据库接口"""
    
    @abstractmethod
    def save_data(self, key: str, data: Any) -> bool:
        pass
    
    @abstractmethod
    def load_data(self, key: str) -> Optional[Any]:
        pass
    
    @abstractmethod
    def delete_data(self, key: str) -> bool:
        pass

class SQLiteDatabase(DatabaseInterface):
    """SQLite数据库实现"""
    
    def __init__(self, db_path: str = "cache.db"):
        self.db_path = db_path
        self._init_db()
    
    def _init_db(self):
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS cache (
                        key TEXT PRIMARY KEY,
                        data TEXT
                    )
                """)
                conn.commit()
        except Exception as e:
            logger.error(f"初始化SQLite数据库失败: {e}")
    
    def save_data(self, key: str, data: Any) -> bool:
        try:
            # 确保保存的数据是可JSON序列化的
            if isinstance(data, pd.DataFrame):
                serializable_data = data.to_dict(orient='records')
            elif isinstance(data, list):
                # 检查列表中的元素是否为DataFrame，如果是则转换
                serializable_data = []
                for item in data:
                    if isinstance(item, pd.DataFrame):
                        serializable_data.append(item.to_dict(orient='records'))
                    else:
                        serializable_data.append(item)
            else:
                serializable_data = data
                
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT OR REPLACE INTO cache (key, data)
                    VALUES (?, ?)
                """, (key, json.dumps(serializable_data, default=str)))
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"保存数据到SQLite失败: {e}")
            return False
    
    def load_data(self, key: str) -> Optional[Any]:
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT data FROM cache WHERE key = ?
                """, (key,))
                result = cursor.fetchone()
                if result:
                    return json.loads(result[0])
                return None
        except Exception as e:
            logger.error(f"从SQLite加载数据失败: {e}")
            return None
    
    def delete_data(self, key: str) -> bool:
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM cache WHERE key = ?", (key,))
                conn.commit()
                return True
        except Exception as e:
            logger.error(f"从SQLite删除数据失败: {e}")
            return False

class CacheManager:
    """缓存管理器，支持Redis和数据库"""
    
    def __init__(self, redis_host: str = "localhost", redis_port: int = 6379, 
                 db_type: str = "sqlite", db_config: Dict[str, Any] = None):
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        self.db = self._create_database(db_type, db_config or {})
    
    def _create_database(self, db_type: str, db_config: Dict[str, Any]) -> DatabaseInterface:
        if db_type == "sqlite":
            return SQLiteDatabase(db_config.get("db_path", "cache.db"))
        # 可以在这里添加其他数据库支持，如MySQL、MongoDB等
        else:
            raise ValueError(f"不支持的数据库类型: {db_type}")
    
    def get_cached_data(self, key: str) -> Optional[Any]:
        """获取缓存数据，先查Redis，再查数据库"""
        try:
            # 先从Redis获取
            cached_data = self.redis_client.get(key)
            if cached_data:
                data = json.loads(cached_data)
                # 如果数据是列表且不为空，尝试转换为DataFrame
                if isinstance(data, list) and len(data) > 0:
                    # 检查是否是字典列表（可转换为DataFrame的结构）
                    if isinstance(data[0], dict):
                        data = pd.DataFrame(data)
                        # 对数据进行排序
                        data = self._sort_data(data)
                        # 确保日期列是字符串
                        if '日期' in data:
                            data['日期'] = data['日期'].apply(lambda x: str(x))
                        if 'date' in data:
                            data['date'] = data['date'].apply(lambda x: str(x))
                        
                        return data.to_dict(orient='records')
                # 对列表数据进行排序
                if isinstance(data, list):
                    data = self._sort_data(data)

                return data
            
            # Redis没有则从数据库获取
            db_data = self.db.load_data(key)
            if db_data:
                # 同步到Redis
                self.redis_client.setex(key, 3600, json.dumps(db_data, default=str))  # 默认缓存1小时
                # 如果数据是列表且不为空，尝试转换为DataFrame
                if isinstance(db_data, list) and len(db_data) > 0:
                    # 检查是否是字典列表（可转换为DataFrame的结构）
                    if isinstance(db_data[0], dict):
                        data = pd.DataFrame(db_data)
                        # 对数据进行排序
                        data = self._sort_data(data)
                        return data.to_dict(orient='records')
                # 对列表数据进行排序
                if isinstance(db_data, list):
                    db_data = self._sort_data(db_data)
                return db_data
        except Exception as e:
            logger.error(f"获取缓存数据失败: {e}")
        return None
    
    def _sort_data(self, data):
        """对数据按日期/时间字段进行排序"""
        try:
            # 如果是DataFrame
            if isinstance(data, pd.DataFrame):
                # 查找日期或时间相关字段
                date_columns = [col for col in data.columns if '日期' in col or '时间' in col or 'date' in col.lower() or 'time' in col.lower()]
                if date_columns:
                    # 使用第一个找到的日期/时间列进行排序
                    date_col = date_columns[0]
                    # 转换为datetime类型以便正确排序
                    data[date_col] = pd.to_datetime(data[date_col], errors='ignore')
                    # 按日期/时间列升序排序（最新的在后）
                    data = data.sort_values(by=date_col, ascending=True)
                return data
            
            # 如果是列表字典
            elif isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
                # 查找日期或时间相关字段
                first_item = data[0]
                date_keys = [key for key in first_item.keys() if '日期' in key or '时间' in key or 'date' in key.lower() or 'time' in key.lower()]
                if date_keys:
                    # 使用第一个找到的日期/时间键进行排序
                    date_key = date_keys[0]
                    # 按日期/时间字段升序排序（最新的在后）
                    data.sort(key=lambda x: x.get(date_key, ''), reverse=False)
                return data
            
            # 其他情况直接返回原数据
            return data
        except Exception as e:
            logger.warning(f"数据排序时出错: {e}")
            return data  # 出错时返回原始数据
    
    def set_cached_data(self, key: str, data: Any, expiry: int = 3600) -> bool:
        """设置缓存数据，同时保存到Redis和数据库"""
        try:
            # 先对数据进行排序
            sorted_data = self._sort_data(data)
            
            # 确保保存的数据是可JSON序列化的
            if isinstance(sorted_data, pd.DataFrame):
                serializable_data = sorted_data.to_dict(orient='records')
            elif isinstance(sorted_data, list):
                # 检查列表中的元素是否为DataFrame，如果是则转换
                serializable_data = []
                for item in sorted_data:
                    if isinstance(item, pd.DataFrame):
                        serializable_data.append(item.to_dict(orient='records'))
                    else:
                        serializable_data.append(item)
            else:
                serializable_data = sorted_data
            
            # 保存到Redis
            self.redis_client.setex(key, expiry, json.dumps(serializable_data, default=str))
            # 保存到数据库(不包含过期时间)
            return self.db.save_data(key, serializable_data)
        except Exception as e:
            logger.error(f"设置缓存数据失败: {e}")
            return False
    
    def invalidate_cache(self, key: str) -> bool:
        """清除缓存"""
        try:
            self.redis_client.delete(key)
            return self.db.delete_data(key)
        except Exception as e:
            logger.error(f"清除缓存失败: {e}")
            return False
    
    # 添加新的方法用于清除所有缓存
    def clear_all_cache(self) -> bool:
        """清除所有缓存数据"""
        try:
            # 清除Redis中的所有数据
            self.redis_client.flushdb()
            
            # 清除数据库中的所有缓存数据
            with sqlite3.connect(self.db.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM cache")
                conn.commit()
            
            logger.info("所有缓存数据已清除")
            return True
        except Exception as e:
            logger.error(f"清除所有缓存数据失败: {e}")
            return False

# 全局缓存管理器实例
cache_manager = None

def init_cache_manager(redis_host: str = "localhost", redis_port: int = 6379,
                       db_type: str = "sqlite", db_config: Dict[str, Any] = None):
    """初始化缓存管理器"""
    global cache_manager
    logger.info("Initializing CacheManager with Redis %s:%s and database configuration %s...", redis_host, redis_port, db_config)
    cache_manager = CacheManager(redis_host, redis_port, db_type, db_config)

def get_cache_manager() -> CacheManager:
    """获取缓存管理器实例"""
    global cache_manager
    if not cache_manager:
        logger.error("CacheManager has not been initialized. Call init_cache_manager first.")
        return None
        #cache_manager = CacheManager()
    return cache_manager

# 添加新的函数用于清除所有缓存
def clear_all_cache() -> bool:
    """快速清除所有缓存内容的便捷函数"""
    cache_manager = get_cache_manager()
    if cache_manager:
        return cache_manager.clear_all_cache()
    return False
