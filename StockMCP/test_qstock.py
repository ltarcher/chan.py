#!/usr/bin/env python
# -*- encoding=utf8 -*-

import qstock as qs
#import stock_rzrq as rzrq
import pandas as pd
from typing import Dict, List, Optional

import sys
#from mcp_index import get_economic_calendar
#from mcp_macro import get_customs_import_export_data, get_macro_data
#from mcp_money import get_index_hist_money, get_realtime_index_money
#import asyncio

df = qs.get_data("588000")
df.reset_index(inplace=True)
print(df.columns)
print(df)
sys.exit()

realtime_data = asyncio.run(get_realtime_index_money())
print("实时资金流向数据:")
print(realtime_data)

# 获取日度资金流向数据
daily_data = asyncio.run(get_index_hist_money("D"))
print("日度资金流向数据:")
print(daily_data)

# 获取月度资金流向数据
monthly_data = asyncio.run(get_index_hist_money("M"))
print("月度资金流向数据:")
print(monthly_data)

# 获取年度资金流向数据
yearly_data = asyncio.run(get_index_hist_money("Y"))
print("年度资金流向数据:")
print(yearly_data)

sys.exit()

