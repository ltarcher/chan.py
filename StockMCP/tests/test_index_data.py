import unittest
from unittest.mock import Mock, patch, MagicMock
import sys
import os
import pandas as pd
from datetime import datetime

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dataservices.index_data import IndexDataService

class TestIndexDataService(unittest.TestCase):
    
    def setUp(self):
        """测试前的准备工作"""
        self.index_service = IndexDataService()
        # 模拟缓存管理器
        self.mock_cache_manager = Mock()
        self.index_service.set_cache_manager(self.mock_cache_manager)
    
    @patch('dataservices.index_data.qs.realtime_data')
    def test_fetch_index_realtime_data(self, mock_realtime_data):
        """测试获取指数实时数据"""
        # 模拟返回数据
        mock_df = pd.DataFrame({
            'code': ['000001', '000002'],
            'name': ['上证指数', '深证成指'],
            'price': [3000.0, 12000.0]
        })
        mock_realtime_data.return_value = mock_df
        
        result = self.index_service._fetch_index_realtime_data(['上证指数', '深证成指'])
        
        # 验证返回结果
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['name'], '上证指数')
        
        # 验证qs.realtime_data被正确调用
        mock_realtime_data.assert_called_once()
    
    @patch('dataservices.index_data.qs.realtime_data')
    def test_fetch_board_trade_realtime_data(self, mock_realtime_data):
        """测试获取市场总成交数据"""
        # 模拟返回数据
        mock_df = pd.DataFrame({
            'code': ['000001', '000002', '000003'],
            'name': ['上证指数', '深证成指', '北证50'],
            '时间': ['2023-01-01 10:00:00'] * 3,
            '成交额': [1000000000, 2000000000, 500000000]
        })
        mock_realtime_data.return_value = mock_df
        
        result = self.index_service._fetch_board_trade_realtime_data()
        
        # 验证返回结果
        self.assertIsInstance(result, dict)
        self.assertIn('日期', result)
        self.assertIn('总成交额', result)
        self.assertEqual(result['总成交额'], 3500000000)
    
    @patch('dataservices.index_data.qs.get_data')
    def test_fetch_turnover_impl(self, mock_get_data):
        """测试获取历史成交数据"""
        # 模拟返回数据
        mock_df = pd.DataFrame({
            'code': ['000001', '000002'] * 2,
            'name': ['上证指数', '深证成指'] * 2,
            'date': [datetime(2023, 1, 1), datetime(2023, 1, 1), datetime(2023, 1, 2), datetime(2023, 1, 2)],
            'turnover': [1000000000, 2000000000, 1100000000, 2100000000]
        })
        mock_get_data.return_value = mock_df.set_index('code')
        
        result = self.index_service._fetch_turnover_impl('2023-01-01', '2023-01-02')
        
        # 验证返回结果
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['总成交额'], 3000000000)  # 第一天总成交额
        self.assertEqual(result[1]['总成交额'], 3200000000)  # 第二天总成交额
    
    def test_need_incremental_update(self):
        """测试是否需要增量更新判断"""
        # 模拟缓存数据
        cached_data = [
            {"日期": "2023-01-01", "value": 100},
            {"日期": "2023-01-02", "value": 110}
        ]
        
        # 最新数据应该是2023-01-03，缓存只到2023-01-02，应该需要更新
        with patch('dataservices.data_service.trade') as mock_trade:
            mock_trade.latest_trade_date.return_value = "2023-01-03"
            
            result = self.index_service._need_incremental_update(cached_data)
            self.assertTrue(result)
    
    @patch('dataservices.index_data.qs.realtime_data')
    def test_get_index_realtime_data_with_cache(self, mock_realtime_data):
        """测试带缓存的指数实时数据获取"""
        # 模拟缓存中有数据且不需要更新
        cached_data = [
            {"code": "000001", "name": "上证指数", "price": 3000.0}
        ]
        self.mock_cache_manager.get_cached_data.return_value = cached_data
        
        # 模拟不需要增量更新
        with patch.object(self.index_service, '_need_incremental_update', return_value=False):
            result = self.index_service.get_index_realtime_data(['上证指数'])
            
            # 验证返回了缓存数据
            self.assertEqual(result, cached_data)
            # 验证没有调用实际获取数据的方法
            mock_realtime_data.assert_not_called()

if __name__ == '__main__':
    unittest.main()