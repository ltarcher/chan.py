import unittest
from unittest.mock import Mock, patch, MagicMock
import sys
import os
import pandas as pd
from datetime import datetime, timedelta

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dataservices.data_service import DataService, get_date, adjust_start_date, adjust_end_date

class TestDataService(unittest.TestCase):
    
    def setUp(self):
        """测试前的准备工作"""
        self.data_service = DataService()
        # 模拟缓存管理器
        self.mock_cache_manager = Mock()
        self.data_service.set_cache_manager(self.mock_cache_manager)
    
    def test_get_date(self):
        """测试日期解析函数"""
        # 测试正常日期格式
        item1 = {"日期": "2023-01-01"}
        result = get_date(item1)
        self.assertEqual(result, datetime(2023, 1, 1))
        
        item2 = {"date": "2023-01-01 10:00:00"}
        result = get_date(item2)
        self.assertEqual(result, datetime(2023, 1, 1, 10, 0, 0))
        
        # 测试无效日期
        item3 = {"日期": "invalid"}
        result = get_date(item3)
        self.assertEqual(result, datetime.min)
        
        # 测试无日期字段
        item4 = {"name": "test"}
        result = get_date(item4)
        self.assertEqual(result, datetime.min)
    
    def test_adjust_start_date(self):
        """测试开始日期调整函数"""
        # 正常工作日
        date1 = datetime(2023, 1, 2)  # 周一
        result = adjust_start_date(date1)
        self.assertEqual(result, date1)
        
        # 周六
        date2 = datetime(2023, 1, 7)  # 周六
        result = adjust_start_date(date2)
        expected = datetime(2023, 1, 9)  # 下周一
        self.assertEqual(result, expected)
        
        # 周日
        date3 = datetime(2023, 1, 8)  # 周日
        result = adjust_start_date(date3)
        expected = datetime(2023, 1, 9)  # 下周一
        self.assertEqual(result, expected)
    
    def test_adjust_end_date(self):
        """测试结束日期调整函数"""
        # 正常工作日
        date1 = datetime(2023, 1, 2)  # 周一
        result = adjust_end_date(date1)
        self.assertEqual(result, date1)
        
        # 周六
        date2 = datetime(2023, 1, 7)  # 周六
        result = adjust_end_date(date2)
        expected = datetime(2023, 1, 6)  # 周五
        self.assertEqual(result, expected)
        
        # 周日
        date3 = datetime(2023, 1, 8)  # 周日
        result = adjust_end_date(date3)
        expected = datetime(2023, 1, 6)  # 周五
        self.assertEqual(result, expected)
    
    def test_get_cached_data(self):
        """测试从缓存获取数据"""
        test_data = [{"name": "test", "value": 100}]
        self.mock_cache_manager.get_cached_data.return_value = test_data
        
        result = self.data_service.get_cached_data("test_key")
        self.assertEqual(result, test_data)
        self.mock_cache_manager.get_cached_data.assert_called_once_with("test_key")
    
    def test_set_cached_data(self):
        """测试设置缓存数据"""
        test_data = [{"name": "test", "value": 100}]
        self.mock_cache_manager.set_cached_data.return_value = True
        
        result = self.data_service.set_cached_data("test_key", test_data, 1800)
        self.assertTrue(result)
        self.mock_cache_manager.set_cached_data.assert_called_once_with("test_key", test_data, 1800)
    
    def test_adjust_date_by_freq(self):
        """测试根据频率调整日期"""
        test_date = datetime(2023, 1, 1, 10, 30, 45)
        
        # 日级别
        result = self.data_service._adjust_date_by_freq(test_date, 'd')
        expected = datetime(2023, 1, 1)
        self.assertEqual(result, expected)
        
        # 周级别
        result = self.data_service._adjust_date_by_freq(test_date, 'w')
        # 2023年1月1日是周日，周一应该是2022年12月26日
        expected = datetime(2022, 12, 26)
        self.assertEqual(result, expected)
        
        # 月级别
        result = self.data_service._adjust_date_by_freq(test_date, 'm')
        expected = datetime(2023, 1, 1).replace(day=1)
        self.assertEqual(result, expected)
        
        # 分钟级别
        result = self.data_service._adjust_date_by_freq(test_date, 5)
        expected = datetime(2023, 1, 1, 10, 30)
        self.assertEqual(result, expected)
    
    def test_merge_and_deduplicate_data(self):
        """测试合并和去重数据"""
        # 测试列表数据合并
        cached_data = [
            {"日期": "2023-01-01", "value": 100},
            {"日期": "2023-01-02", "value": 110}
        ]
        
        incremental_data = [
            {"日期": "2023-01-02", "value": 120},  # 重复日期，应该被覆盖
            {"日期": "2023-01-03", "value": 130}
        ]
        
        result = self.data_service._merge_and_deduplicate_data(cached_data, incremental_data)
        
        # 应该有3条记录，且2023-01-02的值为120（被覆盖）
        self.assertEqual(len(result), 3)
        self.assertEqual(result[1]["value"], 120)
        self.assertEqual(result[2]["日期"], "2023-01-03 00:00:00")

if __name__ == '__main__':
    unittest.main()