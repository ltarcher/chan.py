import qstock as qs
import pandas as pd
import datetime

from Common.CEnum import AUTYPE, DATA_FIELD, KL_TYPE
from Common.CTime import CTime
from Common.func_util import kltype_lt_day, str2float
from KLine.KLine_Unit import CKLine_Unit

from .CommonStockAPI import CCommonStockApi

class CQStock(CCommonStockApi):
    
    def __init__(self, code, k_type=KL_TYPE.K_DAY, begin_date=None, end_date=None, autype=AUTYPE.QFQ):
        # 如果code带前缀或者后缀，如sh.600000、600000.sh,则去掉
        # 先转为小写
        self.code = code.lower()
        self.code = self.code.replace("sh.", "").replace("sz.", "").replace(".sh", "").replace(".sz", "")
        super(CQStock, self).__init__(self.code, k_type, begin_date, end_date, autype)

    # 获取字段名称对应关系
    # ['date', 'name', 'code', 'open', 'high', 'low', 'close', 'volume', 'turnover',
    #   'turnover_rate']
    def GetColumnNameFromFieldList(self, fileds: str):
        _dict = {
            "date": DATA_FIELD.FIELD_TIME,
            "open": DATA_FIELD.FIELD_OPEN,
            "high": DATA_FIELD.FIELD_HIGH,
            "low": DATA_FIELD.FIELD_LOW,
            "close": DATA_FIELD.FIELD_CLOSE,
            "volume": DATA_FIELD.FIELD_VOLUME,
            "turnover": DATA_FIELD.FIELD_TURNOVER,
            "turnover_rate": DATA_FIELD.FIELD_TURNRATE,
        }
        return [_dict[x] for x in fileds if x in _dict]
    
    # 获取K线频率
    def GetKLType(self, k_type):
        _dict = {
            KL_TYPE.K_1M: 1,
            KL_TYPE.K_5M: 5,
            KL_TYPE.K_15M: 15,
            KL_TYPE.K_30M: 30,
            KL_TYPE.K_60M: 60,
            KL_TYPE.K_DAY: 101,
            KL_TYPE.K_WEEK: 102,
            KL_TYPE.K_MON: 103,
        }
        if k_type not in _dict:
            raise Exception(f"unknown k_type:{k_type}")
        
        return _dict[k_type]
    
    # 获取复权类型
    def GetAutype(self, autype):
        _dict = {
            AUTYPE.QFQ: 1,
            AUTYPE.HFQ: 2,
            AUTYPE.NONE: 0,
        }
        if autype not in _dict:
            raise Exception(f"unknown autype:{autype}")
        
        return _dict[autype]
    
    def create_item_dict(self, data, column_name):
        for i in range(len(data)):
            # 处理时间列，datetime转换为CTime对象
            if i == 0:
                data[i] = self.parse_time_column(data[i])
            # 第2、3列分别是股票名称和代码，不处理
            elif i > 2:
                str2float(data[i])
        return dict(zip(column_name, data))
    
    def parse_time_column(self, timestamp):
        # datetime转换为CTime对象
        return CTime(timestamp.year, 
                     timestamp.month, 
                     timestamp.day,
                     timestamp.hour, 
                     timestamp.minute, 
                     timestamp.second)

    def get_kl_data(self):
        # 获取数据源类
        data = qs.get_data(code_list=self.code, 
                           start=self.begin_date, 
                           end=self.end_date, 
                           freq=self.GetKLType(self.k_type), 
                           fqt=self.GetAutype(self.autype))
        data.reset_index(inplace=True)
        #DataFrame转换为KLine_Unit
        #data是DataFrame
        for index, item in data.iterrows():
            item_data = [
                item['date'],
                item['open'],
                item['high'],
                item['low'],
                item['close'],
                item['volume'],
                item['turnover'],
                item['turnover_rate'],
            ]
            yield CKLine_Unit(self.create_item_dict(item_data, self.GetColumnNameFromFieldList(data.columns)), autofix=True)


    @classmethod
    def do_init(cls):
        pass

    
    @classmethod
    def do_close(cls):
        pass

    def SetBasciInfo(self):
        # 获取股票基本信息
        """
            stock_info_dict = {
                'f57': '代码',
                'f58': '名称',
                'f162': '市盈率(动)',
                'f167': '市净率',
                'f127': '所处行业',
                'f116': '总市值',
                'f117': '流通市值',
                'f173': 'ROE',
                'f187': '净利率',
                'f105': '净利润',
                'f186': '毛利率'
                }
        """
        sr = qs.stock_info(self.code)
        self.name = sr['名称']

