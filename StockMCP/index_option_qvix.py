# -*- coding:utf-8 -*-
# !/usr/bin/env python
"""
Date: 2023/10/19 16:00
Desc: 50 ETF 期权波动率指数 QVIX
300 ETF 期权波动率指数 QVIX
http://1.optbbs.com/s/vix.shtml?50ETF
http://1.optbbs.com/s/vix.shtml?300ETF
"""

import pandas as pd
from functools import lru_cache
import os
import requests
from io import StringIO

headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36",
    }
    
def _format_proxy(proxy):
    """
    格式化代理参数，支持字符串和字典格式
    :param proxy: 代理配置，支持 dict 格式或字符串格式
    :return: 格式化后的代理配置字典
    """
    if proxy is None:
        return None
    if isinstance(proxy, str):
        return {"http": proxy, "https": proxy}
    return proxy

@lru_cache
def __get_optbbs_daily(proxy=None) -> pd.DataFrame:
    """
    读取原始数据
    http://1.optbbs.com/d/csv/d/k.csv
    :param proxy: 代理配置，支持 dict 格式，如 {"http": "http://127.0.0.1:1080", "https": "http://127.0.0.1:1080"}
                 或字符串格式，如 "http://127.0.0.1:1080"
                 或字符串格式，如 "http://127.0.0.1:1080"
    :return: 原始数据
    :rtype: pandas.DataFrame
    """
    
    url = "http://1.optbbs.com/d/csv/d/k.csv"
    # 处理代理参数
    proxies = _format_proxy(proxy)
            
    try:
        if proxies:
            response = requests.get(url, proxies=proxies, headers=headers)
        else:
            response = requests.get(url, headers=headers)
        response.encoding = "gbk"
        temp_df = pd.read_csv(StringIO(response.text))
    except Exception as e:
        if proxies:
            response = requests.get(url, proxies=proxies, headers=headers)
        else:
            response = requests.get(url, headers=headers)
        response.encoding = "utf-8"
        temp_df = pd.read_csv(StringIO(response.text))
        if temp_df.empty:
            raise ValueError(f"无法读取数据，请检查 URL 是否正确: {url}")
    return temp_df


def index_option_50etf_qvix(proxy=None) -> pd.DataFrame:
    """
    50ETF 期权波动率指数 QVIX
    http://1.optbbs.com/s/vix.shtml?50ETF
    :param proxy: 代理配置，支持 dict 格式，如 {"http": "http://127.0.0.1:1080", "https": "http://127.0.0.1:1080"}
                 或字符串格式，如 "http://127.0.0.1:1080"
    :return: 50ETF 期权波动率指数 QVIX
    :rtype: pandas.DataFrame
    """
    temp_df = __get_optbbs_daily(proxy).iloc[:, :5]
    temp_df.columns = [
        "date",
        "open",
        "high",
        "low",
        "close",
    ]
    temp_df.loc[:, "date"] = pd.to_datetime(temp_df["date"], errors="coerce").dt.date
    temp_df.loc[:, "open"] = pd.to_numeric(temp_df["open"], errors="coerce")
    temp_df.loc[:, "high"] = pd.to_numeric(temp_df["high"], errors="coerce")
    temp_df.loc[:, "low"] = pd.to_numeric(temp_df["low"], errors="coerce")
    temp_df.loc[:, "close"] = pd.to_numeric(temp_df["close"], errors="coerce")
    return temp_df


def index_option_50etf_min_qvix(proxy=None) -> pd.DataFrame:
    """
    50 ETF 期权波动率指数 QVIX
    http://1.optbbs.com/s/vix.shtml?50ETF
    :param proxy: 代理配置，支持 dict 格式，如 {"http": "http://127.0.0.1:1080", "https": "http://127.0.0.1:1080"}
                 或字符串格式，如 "http://127.0.0.1:1080"
                 或字符串格式，如 "http://127.0.0.1:1080"
    :return: 50 ETF 期权波动率指数 QVIX
    :rtype: pandas.DataFrame
    """
    url = "http://1.optbbs.com/d/csv/d/vix50.csv"
    # 处理代理参数
    proxies = _format_proxy(proxy)
            
    if proxies:
        response = requests.get(url, proxies=proxies, headers=headers)
    else:
        response = requests.get(url, headers=headers)
    temp_df = pd.read_csv(StringIO(response.text)).iloc[:, :2]
    temp_df.columns = [
        "time",
        "qvix",
    ]
    temp_df.loc[:, "qvix"] = pd.to_numeric(temp_df["qvix"], errors="coerce")
    return temp_df


def index_option_300etf_qvix(proxy=None) -> pd.DataFrame:
    """
    300 ETF 期权波动率指数 QVIX
    http://1.optbbs.com/s/vix.shtml?300ETF
    :param proxy: 代理配置，支持 dict 格式，如 {"http": "http://127.0.0.1:1080", "https": "http://127.0.0.1:1080"}
                 或字符串格式，如 "http://127.0.0.1:1080"
    :return: 300 ETF 期权波动率指数 QVIX
    :rtype: pandas.DataFrame
    """
    temp_df = __get_optbbs_daily(proxy).iloc[:, [0, 9, 10, 11, 12]]
    temp_df.columns = [
        "date",
        "open",
        "high",
        "low",
        "close",
    ]
    temp_df.loc[:, "date"] = pd.to_datetime(temp_df["date"], errors="coerce").dt.date
    temp_df.loc[:, "open"] = pd.to_numeric(temp_df["open"], errors="coerce")
    temp_df.loc[:, "high"] = pd.to_numeric(temp_df["high"], errors="coerce")
    temp_df.loc[:, "low"] = pd.to_numeric(temp_df["low"], errors="coerce")
    temp_df.loc[:, "close"] = pd.to_numeric(temp_df["close"], errors="coerce")
    return temp_df


def index_option_300etf_min_qvix(proxy=None) -> pd.DataFrame:
    """
    300 ETF 期权波动率指数 QVIX-分时
    http://1.optbbs.com/s/vix.shtml?300ETF
    :param proxy: 代理配置，支持 dict 格式，如 {"http": "http://127.0.0.1:1080", "https": "http://127.0.0.1:1080"}
                 或字符串格式，如 "http://127.0.0.1:1080"
                 或字符串格式，如 "http://127.0.0.1:1080"
    :return: 300 ETF 期权波动率指数 QVIX-分时
    :rtype: pandas.DataFrame
    """
    url = "http://1.optbbs.com/d/csv/d/vix300.csv"
    # 处理代理参数
    proxies = _format_proxy(proxy)
            
    if proxies:
        response = requests.get(url, proxies=proxies, headers=headers)
    else:
        response = requests.get(url, headers=headers)
    temp_df = pd.read_csv(StringIO(response.text)).iloc[:, :2]
    temp_df.columns = [
        "time",
        "qvix",
    ]
    temp_df.loc[:, "qvix"] = pd.to_numeric(temp_df["qvix"], errors="coerce")
    return temp_df


def index_option_500etf_qvix(proxy=None) -> pd.DataFrame:
    """
    500 ETF 期权波动率指数 QVIX
    http://1.optbbs.com/s/vix.shtml?500ETF
    :param proxy: 代理配置，支持 dict 格式，如 {"http": "http://127.0.0.1:1080", "https": "http://127.0.0.1:1080"}
                 或字符串格式，如 "http://127.0.0.1:1080"
    :return: 500 ETF 期权波动率指数 QVIX
    :rtype: pandas.DataFrame
    """
    temp_df = __get_optbbs_daily(proxy).iloc[:, [0, 67, 68, 69, 70]]
    temp_df.columns = [
        "date",
        "open",
        "high",
        "low",
        "close",
    ]
    temp_df.loc[:, "date"] = pd.to_datetime(temp_df["date"], errors="coerce").dt.date
    temp_df.loc[:, "open"] = pd.to_numeric(temp_df["open"], errors="coerce")
    temp_df.loc[:, "high"] = pd.to_numeric(temp_df["high"], errors="coerce")
    temp_df.loc[:, "low"] = pd.to_numeric(temp_df["low"], errors="coerce")
    temp_df.loc[:, "close"] = pd.to_numeric(temp_df["close"], errors="coerce")
    return temp_df


def index_option_500etf_min_qvix(proxy=None) -> pd.DataFrame:
    """
    500 ETF 期权波动率指数 QVIX-分时
    http://1.optbbs.com/s/vix.shtml?500ETF
    :param proxy: 代理配置，支持 dict 格式，如 {"http": "http://127.0.0.1:1080", "https": "http://127.0.0.1:1080"}
                 或字符串格式，如 "http://127.0.0.1:1080"
                 或字符串格式，如 "http://127.0.0.1:1080"
    :return: 500 ETF 期权波动率指数 QVIX-分时
    :rtype: pandas.DataFrame
    """
    url = "http://1.optbbs.com/d/csv/d/vix500.csv"
    # 处理代理参数
    proxies = _format_proxy(proxy)
            
    if proxies:
        response = requests.get(url, proxies=proxies, headers=headers)
    else:
        response = requests.get(url, headers=headers)
    temp_df = pd.read_csv(StringIO(response.text)).iloc[:, :2]
    temp_df.columns = [
        "time",
        "qvix",
    ]
    temp_df.loc[:, "qvix"] = pd.to_numeric(temp_df["qvix"], errors="coerce")
    return temp_df


def index_option_cyb_qvix(proxy=None) -> pd.DataFrame:
    """
    创业板 期权波动率指数 QVIX
    http://1.optbbs.com/s/vix.shtml?CYB
    :param proxy: 代理配置，支持 dict 格式，如 {"http": "http://127.0.0.1:1080", "https": "http://127.0.0.1:1080"}
                 或字符串格式，如 "http://127.0.0.1:1080"
    :return: 创业板 期权波动率指数 QVIX
    :rtype: pandas.DataFrame
    """
    temp_df = __get_optbbs_daily(proxy).iloc[:, [0, 71, 72, 73, 74]]
    temp_df.columns = [
        "date",
        "open",
        "high",
        "low",
        "close",
    ]
    temp_df.loc[:, "date"] = pd.to_datetime(temp_df["date"], errors="coerce").dt.date
    temp_df.loc[:, "open"] = pd.to_numeric(temp_df["open"], errors="coerce")
    temp_df.loc[:, "high"] = pd.to_numeric(temp_df["high"], errors="coerce")
    temp_df.loc[:, "low"] = pd.to_numeric(temp_df["low"], errors="coerce")
    temp_df.loc[:, "close"] = pd.to_numeric(temp_df["close"], errors="coerce")
    return temp_df


def index_option_cyb_min_qvix(proxy=None) -> pd.DataFrame:
    """
    创业板 期权波动率指数 QVIX-分时
    http://1.optbbs.com/s/vix.shtml?CYB
    :param proxy: 代理配置，支持 dict 格式，如 {"http": "http://127.0.0.1:1080", "https": "http://127.0.0.1:1080"}
                 或字符串格式，如 "http://127.0.0.1:1080"
    :return: 创业板 期权波动率指数 QVIX-分时
    :rtype: pandas.DataFrame
    """
    url = "http://1.optbbs.com/d/csv/d/vixcyb.csv"
    # 处理代理参数
    proxies = _format_proxy(proxy)
            
    if proxies:
        response = requests.get(url, proxies=proxies, headers=headers)
    else:
        response = requests.get(url, headers=headers)
    temp_df = pd.read_csv(StringIO(response.text)).iloc[:, :2]
    temp_df.columns = [
        "time",
        "qvix",
    ]
    temp_df.loc[:, "qvix"] = pd.to_numeric(temp_df["qvix"], errors="coerce")
    return temp_df


def index_option_kcb_qvix(proxy=None) -> pd.DataFrame:
    """
    科创板 期权波动率指数 QVIX
    http://1.optbbs.com/s/vix.shtml?KCB
    :param proxy: 代理配置，支持 dict 格式，如 {"http": "http://127.0.0.1:1080", "https": "http://127.0.0.1:1080"}
                 或字符串格式，如 "http://127.0.0.1:1080"
    :return: 科创板 期权波动率指数 QVIX
    :rtype: pandas.DataFrame
    """
    temp_df = __get_optbbs_daily(proxy).iloc[:, [0, 83, 84, 85, 86]]
    temp_df.columns = [
        "date",
        "open",
        "high",
        "low",
        "close",
    ]
    temp_df.loc[:, "date"] = pd.to_datetime(temp_df["date"], errors="coerce").dt.date
    temp_df.loc[:, "open"] = pd.to_numeric(temp_df["open"], errors="coerce")
    temp_df.loc[:, "high"] = pd.to_numeric(temp_df["high"], errors="coerce")
    temp_df.loc[:, "low"] = pd.to_numeric(temp_df["low"], errors="coerce")
    temp_df.loc[:, "close"] = pd.to_numeric(temp_df["close"], errors="coerce")
    return temp_df


def index_option_kcb_min_qvix(proxy=None) -> pd.DataFrame:
    """
    科创板 期权波动率指数 QVIX-分时
    http://1.optbbs.com/s/vix.shtml?KCB
    :param proxy: 代理配置，支持 dict 格式，如 {"http": "http://127.0.0.1:1080", "https": "http://127.0.0.1:1080"}
                 或字符串格式，如 "http://127.0.0.1:1080"
    :return: 科创板 期权波动率指数 QVIX-分时
    :rtype: pandas.DataFrame
    """
    url = "http://1.optbbs.com/d/csv/d/vixkcb.csv"
    # 处理代理参数
    proxies = _format_proxy(proxy)
            
    if proxies:
        response = requests.get(url, proxies=proxies, headers=headers)
    else:
        response = requests.get(url, headers=headers)
    temp_df = pd.read_csv(StringIO(response.text)).iloc[:, :2]
    temp_df.columns = [
        "time",
        "qvix",
    ]
    temp_df.loc[:, "qvix"] = pd.to_numeric(temp_df["qvix"], errors="coerce")
    return temp_df


def index_option_100etf_qvix(proxy=None) -> pd.DataFrame:
    """
    深证100ETF 期权波动率指数 QVIX
    http://1.optbbs.com/s/vix.shtml?100ETF
    :param proxy: 代理配置，支持 dict 格式，如 {"http": "http://127.0.0.1:1080", "https": "http://127.0.0.1:1080"}
                 或字符串格式，如 "http://127.0.0.1:1080"
    :return: 深证100ETF 期权波动率指数 QVIX
    :rtype: pandas.DataFrame
    """
    temp_df = __get_optbbs_daily(proxy).iloc[:, [0, 75, 76, 77, 78]]
    temp_df.columns = [
        "date",
        "open",
        "high",
        "low",
        "close",
    ]
    temp_df.loc[:, "date"] = pd.to_datetime(temp_df["date"], errors="coerce").dt.date
    temp_df.loc[:, "open"] = pd.to_numeric(temp_df["open"], errors="coerce")
    temp_df.loc[:, "high"] = pd.to_numeric(temp_df["high"], errors="coerce")
    temp_df.loc[:, "low"] = pd.to_numeric(temp_df["low"], errors="coerce")
    temp_df.loc[:, "close"] = pd.to_numeric(temp_df["close"], errors="coerce")
    return temp_df


def index_option_100etf_min_qvix(proxy=None) -> pd.DataFrame:
    """
    深证100ETF 期权波动率指数 QVIX-分时
    http://1.optbbs.com/s/vix.shtml?100ETF
    :param proxy: 代理配置，支持 dict 格式，如 {"http": "http://127.0.0.1:1080", "https": "http://127.0.0.1:1080"}
                 或字符串格式，如 "http://127.0.0.1:1080"
    :return: 深证100ETF 期权波动率指数 QVIX-分时
    :rtype: pandas.DataFrame
    """
    url = "http://1.optbbs.com/d/csv/d/vix100.csv"
    # 处理代理参数
    proxies = _format_proxy(proxy)
            
    if proxies:
        response = requests.get(url, proxies=proxies, headers=headers)
    else:
        response = requests.get(url, headers=headers)
    temp_df = pd.read_csv(StringIO(response.text)).iloc[:, :2]
    temp_df.columns = [
        "time",
        "qvix",
    ]
    temp_df.loc[:, "qvix"] = pd.to_numeric(temp_df["qvix"], errors="coerce")
    return temp_df


def index_option_300index_qvix(proxy=None) -> pd.DataFrame:
    """
    中证300股指 期权波动率指数 QVIX
    http://1.optbbs.com/s/vix.shtml?Index
    :param proxy: 代理配置，支持 dict 格式，如 {"http": "http://127.0.0.1:1080", "https": "http://127.0.0.1:1080"}
                 或字符串格式，如 "http://127.0.0.1:1080"
    :return: 中证300股指 期权波动率指数 QVIX
    :rtype: pandas.DataFrame
    """
    temp_df = __get_optbbs_daily(proxy).iloc[:, [0, 17, 18, 19, 20]]
    temp_df.columns = [
        "date",
        "open",
        "high",
        "low",
        "close",
    ]
    temp_df.loc[:, "date"] = pd.to_datetime(temp_df["date"], errors="coerce").dt.date
    temp_df.loc[:, "open"] = pd.to_numeric(temp_df["open"], errors="coerce")
    temp_df.loc[:, "high"] = pd.to_numeric(temp_df["high"], errors="coerce")
    temp_df.loc[:, "low"] = pd.to_numeric(temp_df["low"], errors="coerce")
    temp_df.loc[:, "close"] = pd.to_numeric(temp_df["close"], errors="coerce")
    return temp_df


def index_option_300index_min_qvix(proxy=None) -> pd.DataFrame:
    """
    中证300股指 期权波动率指数 QVIX-分时
    http://1.optbbs.com/s/vix.shtml?Index
    :param proxy: 代理配置，支持 dict 格式，如 {"http": "http://127.0.0.1:1080", "https": "http://127.0.0.1:1080"}
                 或字符串格式，如 "http://127.0.0.1:1080"
    :return: 中证300股指 期权波动率指数 QVIX-分时
    :rtype: pandas.DataFrame
    """
    url = "http://1.optbbs.com/d/csv/d/vixindex.csv"
    # 处理代理参数
    proxies = _format_proxy(proxy)
            
    if proxies:
        response = requests.get(url, proxies=proxies, headers=headers)
    else:
        response = requests.get(url, headers=headers)
    temp_df = pd.read_csv(StringIO(response.text)).iloc[:, :2]
    temp_df.columns = [
        "time",
        "qvix",
    ]
    temp_df.loc[:, "qvix"] = pd.to_numeric(temp_df["qvix"], errors="coerce")
    return temp_df


def index_option_1000index_qvix(proxy=None) -> pd.DataFrame:
    """
    中证1000股指 期权波动率指数 QVIX
    http://1.optbbs.com/s/vix.shtml?Index1000
    :param proxy: 代理配置，支持 dict 格式，如 {"http": "http://127.0.0.1:1080", "https": "http://127.0.0.1:1080"}
                 或字符串格式，如 "http://127.0.0.1:1080"
    :return: 中证1000股指 期权波动率指数 QVIX
    :rtype: pandas.DataFrame
    """
    temp_df = __get_optbbs_daily(proxy).iloc[:, [0, 25, 26, 27, 28]]
    temp_df.columns = [
        "date",
        "open",
        "high",
        "low",
        "close",
    ]
    temp_df.loc[:, "date"] = pd.to_datetime(temp_df["date"], errors="coerce").dt.date
    temp_df.loc[:, "open"] = pd.to_numeric(temp_df["open"], errors="coerce")
    temp_df.loc[:, "high"] = pd.to_numeric(temp_df["high"], errors="coerce")
    temp_df.loc[:, "low"] = pd.to_numeric(temp_df["low"], errors="coerce")
    temp_df.loc[:, "close"] = pd.to_numeric(temp_df["close"], errors="coerce")
    return temp_df


def index_option_1000index_min_qvix(proxy=None) -> pd.DataFrame:
    """
    中证1000股指 期权波动率指数 QVIX-分时
    http://1.optbbs.com/s/vix.shtml?Index1000
    :param proxy: 代理配置，支持 dict 格式，如 {"http": "http://127.0.0.1:1080", "https": "http://127.0.0.1:1080"}
                 或字符串格式，如 "http://127.0.0.1:1080"
    :return: 中证1000股指 期权波动率指数 QVIX-分时
    :rtype: pandas.DataFrame
    """
    url = "http://1.optbbs.com/d/csv/d/vixindex1000.csv"
    # 处理代理参数
    proxies = _format_proxy(proxy)
            
    if proxies:
        response = requests.get(url, proxies=proxies, headers=headers)
    else:
        response = requests.get(url, headers=headers)
    temp_df = pd.read_csv(StringIO(response.text)).iloc[:, :2]
    temp_df.columns = [
        "time",
        "qvix",
    ]
    temp_df.loc[:, "qvix"] = pd.to_numeric(temp_df["qvix"], errors="coerce")
    return temp_df


def index_option_50index_qvix(proxy=None) -> pd.DataFrame:
    """
    上证50股指 期权波动率指数 QVIX
    http://1.optbbs.com/s/vix.shtml?50index
    :param proxy: 代理配置，支持 dict 格式，如 {"http": "http://127.0.0.1:1080", "https": "http://127.0.0.1:1080"}
                 或字符串格式，如 "http://127.0.0.1:1080"
    :return: 上证50股指 期权波动率指数 QVIX
    :rtype: pandas.DataFrame
    """
    temp_df = __get_optbbs_daily(proxy).iloc[:, [0, 79, 80, 81, 82]]
    temp_df.columns = [
        "date",
        "open",
        "high",
        "low",
        "close",
    ]
    temp_df.loc[:, "date"] = pd.to_datetime(temp_df["date"], errors="coerce").dt.date
    temp_df.loc[:, "open"] = pd.to_numeric(temp_df["open"], errors="coerce")
    temp_df.loc[:, "high"] = pd.to_numeric(temp_df["high"], errors="coerce")
    temp_df.loc[:, "low"] = pd.to_numeric(temp_df["low"], errors="coerce")
    temp_df.loc[:, "close"] = pd.to_numeric(temp_df["close"], errors="coerce")
    return temp_df


def index_option_50index_min_qvix(proxy=None) -> pd.DataFrame:
    """
    上证50股指 期权波动率指数 QVIX-分时
    http://1.optbbs.com/s/vix.shtml?50index
    :param proxy: 代理配置，支持 dict 格式，如 {"http": "http://127.0.0.1:1080", "https": "http://127.0.0.1:1080"}
                 或字符串格式，如 "http://127.0.0.1:1080"
    :return: 上证50股指 期权波动率指数 QVIX-分时
    :rtype: pandas.DataFrame
    """
    url = "http://1.optbbs.com/d/csv/d/vix50index.csv"
    # 处理代理参数
    proxies = _format_proxy(proxy)
            
    if proxies:
        response = requests.get(url, proxies=proxies, headers=headers)
    else:
        response = requests.get(url, headers=headers)
    temp_df = pd.read_csv(StringIO(response.text)).iloc[:, :2]
    temp_df.columns = [
        "time",
        "qvix",
    ]
    temp_df.loc[:, "qvix"] = pd.to_numeric(temp_df["qvix"], errors="coerce")
    return temp_df


if __name__ == "__main__":
    proxy = "http://192.168.8.212:21006"
    index_option_50etf_qvix_df = index_option_50etf_qvix(proxy=proxy)
    print(index_option_50etf_qvix_df)

    index_option_50etf_min_qvix_df = index_option_50etf_min_qvix(proxy=proxy)
    print(index_option_50etf_min_qvix_df)

    index_option_300etf_qvix_df = index_option_300etf_qvix(proxy=proxy)
    print(index_option_300etf_qvix_df)

    index_option_300etf_min_qvix_df = index_option_300etf_min_qvix(proxy=proxy)
    print(index_option_300etf_min_qvix_df)

    index_option_500etf_qvix_df = index_option_500etf_qvix(proxy=proxy)
    print(index_option_500etf_qvix_df)

    index_option_500etf_min_qvix_df = index_option_500etf_min_qvix(proxy=proxy)
    print(index_option_500etf_min_qvix_df)

    index_option_cyb_qvix_df = index_option_cyb_qvix(proxy=proxy)
    print(index_option_cyb_qvix_df)

    index_option_cyb_min_qvix_df = index_option_cyb_min_qvix(proxy=proxy)
    print(index_option_cyb_min_qvix_df)

    index_option_kcb_qvix_df = index_option_kcb_qvix(proxy=proxy)
    print(index_option_kcb_qvix_df)

    index_option_kcb_min_qvix_df = index_option_kcb_min_qvix(proxy=proxy)
    print(index_option_kcb_min_qvix_df)

    index_option_100etf_qvix_df = index_option_100etf_qvix(proxy=proxy)
    print(index_option_100etf_qvix_df)

    index_option_100etf_min_qvix_df = index_option_100etf_min_qvix(proxy=proxy)
    print(index_option_100etf_min_qvix_df)

    index_option_300index_qvix_df = index_option_300index_qvix(proxy=proxy)
    print(index_option_300index_qvix_df)

    index_option_300index_min_qvix_df = index_option_300index_min_qvix(proxy=proxy)
    print(index_option_300index_min_qvix_df)

    index_option_1000index_qvix_df = index_option_1000index_qvix(proxy=proxy)
    print(index_option_1000index_qvix_df)

    index_option_1000index_min_qvix_df = index_option_1000index_min_qvix(proxy=proxy)
    print(index_option_1000index_min_qvix_df)

    index_option_50index_qvix_df = index_option_50index_qvix(proxy=proxy)
    print(index_option_50index_qvix_df)

    index_option_50index_min_qvix_df = index_option_50index_min_qvix(proxy=proxy)
    print(index_option_50index_min_qvix_df)