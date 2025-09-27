#!/usr/bin/env python
# -*- encoding=utf8 -*-

import time
import os
import sys
import json
from datetime import datetime, timedelta
from threading import Timer, Lock
from Chan import CChan
from ChanConfig import CChanConfig
from Common.CEnum import AUTYPE, DATA_SRC, KL_TYPE, BSP_TYPE
from Plot.PlotlyDriver import CPlotlyDriver
from WeChatNotify.wxpusher import Wxpusher
import qstock as qs

try:
    import asyncio
    import websockets
    from threading import Thread
    WEBSOCKET_AVAILABLE = True
except ImportError:
    WEBSOCKET_AVAILABLE = False
    print("WebSocket依赖未安装，将不启用WebSocket服务器功能")

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import StockMCP.mcp_index as index

class RealtimeMonitor:
    def __init__(self, codes, data_src=DATA_SRC.QSTOCK, debug=False, generate_visualization=True, 
                 enable_websocket=False, websocket_host="localhost", websocket_port=8765,
                 send_wechat_message=True):
        self.codes = codes if isinstance(codes, list) else [codes]
        self.data_src = data_src
        self.debug = debug
        self.generate_visualization = generate_visualization
        self.send_wechat_message = send_wechat_message
        self.enable_websocket = enable_websocket
        self.websocket_host = websocket_host
        self.websocket_port = websocket_port
        self.lv_list = [
            KL_TYPE.K_MON,
            KL_TYPE.K_WEEK,
            KL_TYPE.K_DAY,
            KL_TYPE.K_60M,
            KL_TYPE.K_30M,
            KL_TYPE.K_15M,
            KL_TYPE.K_5M,
            KL_TYPE.K_1M
        ]
        
        # 统一管理K线级别与中文名称的映射关系
        self.kline_level_name_map = {
            "K_1M": "1分钟",
            "K_5M": "5分钟",
            "K_15M": "15分钟",
            "K_30M": "30分钟",
            "K_60M": "60分钟",
            "K_DAY": "日线",
            "K_WEEK": "周线",
            "K_MON": "月线"
        }
        
        # K线级别枚举映射
        self.level_enum_map = {
            "K_1M": KL_TYPE.K_1M,
            "K_5M": KL_TYPE.K_5M,
            "K_15M": KL_TYPE.K_15M,
            "K_30M": KL_TYPE.K_30M,
            "K_60M": KL_TYPE.K_60M,
            "K_DAY": KL_TYPE.K_DAY,
            "K_WEEK": KL_TYPE.K_WEEK,
            "K_MON": KL_TYPE.K_MON
        }
        
        # 定义K线级别的优先级，从大到小排列
        self.level_priority = {
            KL_TYPE.K_MON: 8,
            KL_TYPE.K_WEEK: 7,
            KL_TYPE.K_DAY: 6,
            KL_TYPE.K_60M: 5,
            KL_TYPE.K_30M: 4,
            KL_TYPE.K_15M: 3,
            KL_TYPE.K_5M: 2,
            KL_TYPE.K_1M: 1
        }
        
        # 消失信号中K线级别的优先级映射
        self.level_priority_map = {
            "K_MON": 8,
            "K_WEEK": 7,
            "K_DAY": 6,
            "K_60M": 5,
            "K_30M": 4,
            "K_15M": 3,
            "K_5M": 2,
            "K_1M": 1
        }
        
        # 信号类型映射
        self.bsp_type_map = {
            "1": "第一类买卖点",
            "1p": "次级别盘整背驰第一类买卖点",
            "2": "第二类买卖点",
            "2s": "次级别趋势背驰第二类买卖点",
            "3a": "第三类买卖点(中枢在1类后面)",
            "3b": "第三类买卖点(中枢在1类前面)"
        }
        
        # 初始化微信推送
        try:
            self.wxpusher = Wxpusher()
            self.wxpusher.get_users()
        except Exception as e:
            print(f"微信推送初始化失败: {e}")
            self.wxpusher = None

        # 用于记录已发送的买卖点，避免重复推送
        self.sent_bsp = set()
        
        # 用于记录上一次的买卖点，检测变化
        self.previous_bsp = {}
        
        # WebSocket客户端连接集合
        self.websocket_clients = set()
        
        # 配置锁，确保线程安全
        self.config_lock = Lock()
        
        # 创建输出目录
        self.output_path = os.path.join(os.getcwd(), "output")
        if not os.path.exists(self.output_path):
            os.makedirs(self.output_path)
            
        # 启动WebSocket服务器
        if self.enable_websocket and WEBSOCKET_AVAILABLE:
            self.start_websocket_server()

    def get_kline_level_name(self, level):
        """
        将K线级别的枚举值转换为中文文字表示
        """
        level_names = {
            KL_TYPE.K_1M: "1分钟",
            KL_TYPE.K_5M: "5分钟",
            KL_TYPE.K_15M: "15分钟",
            KL_TYPE.K_30M: "30分钟",
            KL_TYPE.K_60M: "60分钟",
            KL_TYPE.K_DAY: "日线",
            KL_TYPE.K_WEEK: "周线",
            KL_TYPE.K_MON: "月线"
        }
        return level_names.get(level, level.value)

    def get_begin_time_by_level(self, level):
        """
        根据K线级别确定开始时间
        K_DAY: 最近一年
        K_WEEK: 最近10年
        K_MON: 所有数据(从1900年开始)
        分钟级别(K_60M及以下): 最近一个月
        """
        now = datetime.now()
        if level == KL_TYPE.K_DAY:
            return (now - timedelta(days=365)).strftime("%Y-%m-%d")
        elif level == KL_TYPE.K_WEEK:
            return (now - timedelta(days=365*10)).strftime("%Y-%m-%d")
        elif level == KL_TYPE.K_MON:
            return "1900-01-01"
        elif level in [KL_TYPE.K_1M, KL_TYPE.K_3M, KL_TYPE.K_5M, KL_TYPE.K_15M, KL_TYPE.K_30M, KL_TYPE.K_60M]:
            return (now - timedelta(days=30)).strftime("%Y-%m-%d")
        else:
            # 默认返回最近一年
            return (now - timedelta(days=365)).strftime("%Y-%m-%d")

    def analyze_single_code(self, code):
        """
        分析单个股票代码的缠论指标
        """
        print(f"开始分析股票: {code}")

        config = CChanConfig({
            "bi_strict": True,
            "trigger_step": False,
            "skip_step": 0,
            "divergence_rate": float("inf"),
            "bsp2_follow_1": True,
            "bsp3_follow_1": True,
            "min_zs_cnt": 0,
            "bs1_peak": True,
            "macd_algo": "peak",
            "bs_type": '1,2,3a,1p,2s,3b',
            "print_warning": True,
            "zs_algo": "normal",
            "cal_rsi": True,
            "cal_kdj": True,
            "cal_demark": True,
            "boll_n": 20,
        })

        plot_config = {
            "plot_kline": True,
            "plot_kline_combine": True,
            "plot_bi": True,
            "plot_seg": True,
            "plot_eigen": False,
            "plot_zs": True,
            "plot_macd": True,
            "plot_mean": False,
            "plot_channel": False,
            "plot_bsp": True,
            "plot_extrainfo": False,
            "plot_demark": False,
            "plot_marker": False,
            "plot_rsi": False,
            "plot_kdj": False,
            "plot_boll": True,
        }

        # 收集所有级别的买卖点信息
        all_latest_bsps = []
        
        # 分别获取和分析每个级别的数据
        for lv in self.lv_list:
            begin_time = self.get_begin_time_by_level(lv)
            
            try:
                chan = CChan(
                    code=code,
                    begin_time=begin_time,
                    end_time=None,
                    data_src=self.data_src,
                    lv_list=[lv],  # 只获取当前级别的数据
                    config=config,
                    autype=AUTYPE.QFQ,
                )
                
                # 获取当前级别的买卖点（最多3个）
                latest_bsps = chan[lv].bs_point_lst.get_latest_bsp(3)
                all_latest_bsps.extend([(lv, bsp) for bsp in latest_bsps])
                
                # 生成当前级别的可视化图表（受控于generate_visualization参数）
                if self.generate_visualization:
                    self.generate_visualization_for_level(code, chan, plot_config, lv)
                
            except Exception as e:
                print(f"分析股票 {code} 的 {lv.name} 级别时出错: {e}")

        # 检查并发送买卖点信号和变化（整合发送）
        self.check_and_send_bsp_signals_with_changes(code, all_latest_bsps)

        # 更新previous_bsp，保存当前所有买卖点的详细信息
        current_bsps = {}
        for lv, bsp in all_latest_bsps:
            key = f"{code}_{lv.value}"  # 使用lv.value作为key的一部分
            bsp_detail = {
                'key': self.get_bsp_key(code, lv, bsp),
                'is_buy': bsp.is_buy,
                'time': bsp.klu.time.strftime("%Y-%m-%d %H:%M:%S") if hasattr(bsp.klu.time, 'strftime') else str(bsp.klu.time),
                'type': [t.value for t in bsp.type]  # 保存买卖点类型，避免重复调用
            }
            
            if key not in current_bsps:
                current_bsps[key] = []
            current_bsps[key].append(bsp_detail)
        
        self.previous_bsp.update(current_bsps)

    def get_bsp_description(self, bsp):
        """
        获取买卖点描述信息
        """
        bsp_type_map = {
            BSP_TYPE.T1: "第一类买卖点",
            BSP_TYPE.T2: "第二类买卖点",
            BSP_TYPE.T3A: "第三类买卖点(中枢在1类后面)",
            BSP_TYPE.T3B: "第三类买卖点(中枢在1类前面)",
            BSP_TYPE.T1P: "次级别盘整背驰第一类买卖点",
            BSP_TYPE.T2S: "次级别趋势背驰第二类买卖点",
        }
        
        bsp_types = [bsp_type_map.get(t, t.value) for t in bsp.type]
        return ", ".join(bsp_types)

    def get_bsp_key(self, code, lv, bsp):
        """
        生成买卖点的唯一标识符
        """
        return f"{code}_{lv.value}_{bsp.klu.time}_{bsp.type[0].value}"

    def check_and_send_bsp_signals_single(self, code, chan):
        """
        检查并发送买卖点信号（单个）
        """
        if not self.wxpusher:
            return
            
        for lv_idx, lv in enumerate(self.lv_list):
            if lv not in chan.kl_datas:
                continue
                
            # 获取最新的买卖点（最多3个）
            latest_bsps = chan[lv].bs_point_lst.get_latest_bsp(3)
            
            for bsp in latest_bsps:
                # 创建唯一标识符防止重复发送
                bsp_id = self.get_bsp_key(code, lv, bsp)
                
                if bsp_id in self.sent_bsp:
                    continue
                    
                # 获取K线价格信息
                price = bsp.klu.close
                bsp_desc = self.get_bsp_description(bsp)
                
                # 构造消息内容 (Markdown格式)
                msg = f"## 🟢 新信号通知\n\n"
                msg += f"- **股票代码**: {code}\n"
                msg += f"- **级别**: {lv.value}\n"
                msg += f"- **时间**: {bsp.klu.time}\n"
                msg += f"- **价格**: {price}\n"
                msg += f"- **信号类型**: {bsp_desc}\n"
                msg += f"- **操作建议**: {'📈 买入' if bsp.is_buy else '📉 卖出'}"
                
                title = f"{code} {lv.value} {bsp_desc}"
                
                # 发送微信推送 (设置contentType为markdown格式)
                try:
                    result = self.wxpusher.wx_send_msg(msg, title=title, contentType=3)
                    print(f"已发送信号: {title}")
                    self.sent_bsp.add(bsp_id)
                except Exception as e:
                    print(f"发送微信推送失败: {e}")

    def check_and_send_bsp_signals_batch(self, code, all_latest_bsps):
        """
        批量检查并发送买卖点信号
        """
        if not self.wxpusher:
            return
            
        # 收集所有新出现的买卖点
        new_bsps = []
        
        for lv, bsp in all_latest_bsps:
            # 创建唯一标识符防止重复发送
            bsp_id = self.get_bsp_key(code, lv, bsp)
            
            if bsp_id not in self.sent_bsp:
                new_bsps.append((lv, bsp))
                self.sent_bsp.add(bsp_id)
        
        # 如果有新的买卖点，则发送汇总通知
        if new_bsps:
            # 构造消息内容 (Markdown格式)
            msg = f"## 🟢 {code} 新信号通知\n\n"
            msg += "| 级别 | 时间 | 价格 | 信号类型 | 操作建议 |\n"
            msg += "|------|------|------|----------|----------|\n"
            
            for lv, bsp in new_bsps:
                price = bsp.klu.close
                bsp_desc = self.get_bsp_description(bsp)
                operation = '📈 买入' if bsp.is_buy else '📉 卖出'
                msg += f"| {lv.value} | {bsp.klu.time} | {price} | {bsp_desc} | {operation} |\n"
            
            title = f"{code} 出现 {len(new_bsps)} 个新信号"
            
            # 发送微信推送 (设置contentType为markdown格式)
            try:
                self.wxpusher.wx_send_topicname_group_msg(msg, topicname="缠论指标", title=title, contentType=3)
                print(f"已发送信号: {title}")
            except Exception as e:
                print(f"发送微信推送失败: {e}")

    def check_bsp_changes(self, code, all_latest_bsps):
        """
        检查买卖点变化情况
        """
        if not self.wxpusher:
            return

        current_bsps = {}

        # 处理传入的买卖点数据
        for lv, bsp in all_latest_bsps:
            key = f"{code}_{lv.value}"
            bsp_key = self.get_bsp_key(code, lv, bsp)

            if key not in current_bsps:
                current_bsps[key] = []
            current_bsps[key].append(bsp_key)

        # 检查是否有买卖点消失
        for key in list(self.previous_bsp.keys()):
            if key in current_bsps:
                previous_set = set(self.previous_bsp[key])
                current_set = set(current_bsps[key])

                # 查找消失的买卖点
                disappeared_bsps = previous_set - current_set
                if disappeared_bsps:
                    for bsp_key in disappeared_bsps:
                        # 解析bsp_key获取详细信息
                        parts = bsp_key.split("_")
                        if len(parts) >= 4:
                            bsp_code, bsp_lv, bsp_time, bsp_type = parts[0], parts[1], parts[2], parts[3]
                            msg = f"## 🔴 信号消失通知\n\n"
                            msg += f"- **股票代码**: {bsp_code}\n"
                            msg += f"- **级别**: {bsp_lv}\n"
                            msg += f"- **时间**: {bsp_time}\n"
                            msg += f"- **信号类型**: {bsp_type}\n"
                            msg += f"- **原因**: 随着行情变化，该买卖点不再满足条件，已被系统取消"

                            title = f"{bsp_code} {bsp_lv} 信号消失"

                            try:
                                self.wxpusher.wx_send_topicname_group_msg(msg, topicname="缠论指标", title=title, contentType=3)
                                print(f"已发送信号消失通知: {title}")
                                # 从sent_bsp中移除，以便如果重新出现可以再次发送
                                self.sent_bsp.discard(bsp_key)
                            except Exception as e:
                                print(f"发送信号消失通知失败: {e}")
            # 如果当前没有该级别的数据，但之前有，则认为所有该级别的信号都消失了
            elif key.startswith(f"{code}_"):
                for bsp_key in self.previous_bsp[key]:
                    parts = bsp_key.split("_")
                    if len(parts) >= 4:
                        bsp_code, bsp_lv, bsp_time, bsp_type = parts[0], parts[1], parts[2], parts[3]
                        msg = f"## 🔴 信号消失通知\n\n"
                        msg += f"- **股票代码**: {bsp_code}\n"
                        msg += f"- **级别**: {bsp_lv}\n"
                        msg += f"- **时间**: {bsp_time}\n"
                        msg += f"- **信号类型**: {bsp_type}\n"
                        msg += f"- **原因**: 随着行情变化，该买卖点不再满足条件，已被系统取消"

                        title = f"{bsp_code} {bsp_lv} 信号消失"

                        try:
                            self.wxpusher.wx_send_topicname_group_msg(msg, topicname="缠论指标", title=title, contentType=3)
                            print(f"已发送信号消失通知: {title}")
                            # 从sent_bsp中移除，以便如果重新出现可以再次发送
                            self.sent_bsp.discard(bsp_key)
                        except Exception as e:
                            print(f"发送信号消失通知失败: {e}")

        # 更新previous_bsp
        self.previous_bsp.update(current_bsps)

    def check_and_send_bsp_signals_with_changes(self, code, all_latest_bsps):
        """
        整合检查并发送买卖点信号和变化
        """
        # 使用配置锁确保线程安全
        with self.config_lock:
            send_wechat = self.send_wechat_message
            generate_viz = self.generate_visualization
            debug_mode = self.debug
            
        if not self.wxpusher and send_wechat:
            return
            
        # 收集所有新出现的买卖点
        new_bsps = []
        
        for lv, bsp in all_latest_bsps:
            # 创建唯一标识符防止重复发送
            bsp_id = self.get_bsp_key(code, lv, bsp)
            
            if bsp_id not in self.sent_bsp:
                new_bsps.append((lv, bsp))
                self.sent_bsp.add(bsp_id)
        
        # 检查买卖点变化
        current_bsps = {}
        
        # 重构当前买卖点数据结构
        for lv, bsp in all_latest_bsps:
            key = f"{code}_{lv.value}"
            bsp_key = self.get_bsp_key(code, lv, bsp)

            if key not in current_bsps:
                current_bsps[key] = []
            current_bsps[key].append(bsp_key)
        
        # 查找消失的买卖点
        disappeared_bsps = []
        disappeared_bsp_details = []  # 保存消失的买卖点详细信息
        
        for key in list(self.previous_bsp.keys()):
            if key in current_bsps:
                # 获取当前的买卖点key集合
                current_bsp_keys = set(current_bsps[key])
                
                # 检查之前保存的买卖点详情
                for prev_bsp_detail in self.previous_bsp[key]:
                    if prev_bsp_detail['key'] not in current_bsp_keys:
                        # 这个买卖点消失了
                        disappeared_bsps.append(prev_bsp_detail['key'])
                        disappeared_bsp_details.append(prev_bsp_detail)
            # 如果当前没有该级别的数据，但之前有，则认为所有该级别的信号都消失了
            elif key.startswith(f"{code}_"):
                for prev_bsp_detail in self.previous_bsp[key]:
                    disappeared_bsps.append(prev_bsp_detail['key'])
                    disappeared_bsp_details.append(prev_bsp_detail)
        
        # 从sent_bsp中移除消失的买卖点，以便如果重新出现可以再次发送
        for bsp_key in disappeared_bsps:
            self.sent_bsp.discard(bsp_key)
        
        # 对新信号按照时间和K线级别从大到小排序
        if new_bsps:
            # 按照时间从新到旧，级别从大到小排序
            new_bsps.sort(key=lambda x: (x[1].klu.time, self.level_priority[x[0]]), reverse=True)
        
        # 对消失的信号按照时间和K线级别从大到小排序
        if disappeared_bsp_details:
            # 解析消失的信号并排序
            parsed_disappeared_bsps = []
            for bsp_detail in disappeared_bsp_details:
                bsp_key = bsp_detail['key']
                parts = bsp_key.split("_")
                if len(parts) >= 4:
                    bsp_code, bsp_lv, bsp_time_str, bsp_type = parts[0], parts[1], parts[2], parts[3]
                    # 尝试解析时间
                    try:
                        bsp_time = datetime.strptime(bsp_time_str, "%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        try:
                            bsp_time = datetime.strptime(bsp_time_str, "%Y-%m-%d")
                        except ValueError:
                            bsp_time = datetime.min  # 无法解析时间时使用最小时间
                    
                    parsed_disappeared_bsps.append((bsp_code, bsp_lv, bsp_time, bsp_type, bsp_detail))
            
            # 按照时间从新到旧，级别从大到小排序
            parsed_disappeared_bsps.sort(key=lambda x: (x[2], self.level_priority_map.get(x[1], 0)), reverse=True)
            disappeared_bsp_details = [item[4] for item in parsed_disappeared_bsps]
        
        # 如果有新的买卖点或消失的买卖点，则发送汇总通知
        if new_bsps or disappeared_bsp_details:
            # 构造消息内容 (Markdown格式)
            msg = f"## 📊 {code} 信号通知\n\n"
            
            if new_bsps:
                msg += f"### 🟢 新信号 ({len(new_bsps)}个)\n\n"
                msg += "| 级别 | 时间 | 价格 | 信号类型 | 操作建议 |\n"
                msg += "|------|------|------|----------|----------|\n"
                
                for lv, bsp in new_bsps:
                    price = bsp.klu.close
                    bsp_desc = self.get_bsp_description(bsp)
                    operation = '📈 买入' if bsp.is_buy else '📉 卖出'
                    level_name = self.get_kline_level_name(lv)  # 使用中文级别名称
                    msg += f"| {level_name} | {bsp.klu.time} | {price} | {bsp_desc} | {operation} |\n"
            
            if disappeared_bsp_details:
                msg += f"\n### 🔴 消失信号 ({len(disappeared_bsp_details)}个)\n\n"
                msg += "| 股票代码 | 级别 | 时间 | 信号类型 | 操作建议 | 原因 |\n"
                msg += "|----------|------|------|----------|------------|------|\n"
                
                for bsp_detail in disappeared_bsp_details:
                    bsp_key = bsp_detail['key']
                    # 解析bsp_key获取详细信息
                    parts = bsp_key.split("_")
                    if len(parts) >= 4:
                        bsp_code, bsp_lv, bsp_time, bsp_type = parts[0], parts[1], parts[2], parts[3]
                        # 将级别代码转换为中文名称
                        level_name = self.kline_level_name_map.get(bsp_lv, bsp_lv)
                        
                        # 如果通过kline_level_name_map没有找到匹配，则尝试使用get_kline_level_name方法
                        if level_name == bsp_lv:
                            level_enum = self.level_enum_map.get(bsp_lv)
                            if level_enum:
                                level_name = self.get_kline_level_name(level_enum)
                        
                        # 将信号类型代码转换为中文名称
                        bsp_type_name = self.bsp_type_map.get(bsp_type, bsp_type)
                        
                        # 获取买卖点类型（买入/卖出）
                        operation = '📈 买入' if bsp_detail.get('is_buy', False) else '📉 卖出'
                        
                        msg += f"| {bsp_code} | {level_name} | {bsp_time} | {bsp_type_name} | {operation} | 信号不再满足条件 |\n"
            
            title = f"{code} 信号更新: {len(new_bsps)}个新信号, {len(disappeared_bsp_details)}个信号消失"
            
            # 发送微信推送 (设置contentType为markdown格式)
            if send_wechat:
                try:
                    self.wxpusher.wx_send_topicname_group_msg(msg, topicname="缠论指标", title=title, contentType=3)
                    print(f"已发送信号通知: {title}")
                except Exception as e:
                    print(f"发送微信推送失败: {e}")
            else:
                print(f"微信消息推送已禁用: {title}")
                
            # 通过WebSocket推送JSON格式的消息
            if self.enable_websocket:
                # 构造JSON格式的信号数据
                signal_data = {
                    "type": "bsp_signal_update",
                    "code": code,
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "new_signals": [],
                    "disappeared_signals": []
                }
                
                # 添加新信号数据
                for lv, bsp in new_bsps:
                    price = bsp.klu.close
                    bsp_types = [self.bsp_type_map.get(t.value, t.value) for t in bsp.type]
                    signal_data["new_signals"].append({
                        "level": self.get_kline_level_name(lv),
                        "time": str(bsp.klu.time),
                        "price": price,
                        "bsp_type": bsp_types,
                        "is_buy": bsp.is_buy,
                        "operation": "买入" if bsp.is_buy else "卖出"
                    })
                
                # 添加消失的信号数据
                for bsp_detail in disappeared_bsp_details:
                    bsp_key = bsp_detail['key']
                    parts = bsp_key.split("_")
                    if len(parts) >= 4:
                        bsp_code, bsp_lv, bsp_time, bsp_type = parts[0], parts[1], parts[2], parts[3]
                        level_name = self.kline_level_name_map.get(bsp_lv, bsp_lv)
                        if level_name == bsp_lv:
                            level_enum = self.level_enum_map.get(bsp_lv)
                            if level_enum:
                                level_name = self.get_kline_level_name(level_enum)
                        
                        bsp_type_name = self.bsp_type_map.get(bsp_type, bsp_type)
                        signal_data["disappeared_signals"].append({
                            "code": bsp_code,
                            "level": level_name,
                            "time": bsp_time,
                            "bsp_type": bsp_type_name,
                            "is_buy": bsp_detail.get('is_buy', False),
                            "operation": "买入" if bsp_detail.get('is_buy', False) else "卖出",
                            "reason": "信号不再满足条件"
                        })
                
                # 广播JSON数据到WebSocket客户端
                try:
                    json_message = json.dumps(signal_data, ensure_ascii=False)
                    self.broadcast_websocket_message(json_message)
                except Exception as e:
                    print(f"WebSocket消息推送失败: {e}")
            
            # 生成可视化图表（受控于generate_visualization参数）
            if generate_viz:
                # 这里可以添加生成可视化图表的代码
                pass

    def generate_visualization_for_level(self, code, chan, plot_config, level):
        """
        为单个级别生成可视化图表
        """
        try:
            plot_para = {
                "seg": {
                    "plot_trendline": True,
                },
                "bi": {
                    "disp_end": True,
                },
                "figure": {
                    "x_range": chan.get_max_kline_range(),
                }
            }

            output_name = f"{code}-{level.name}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

            # 创建子目录
            lv_output_path = os.path.join(self.output_path, level.name)
            if not os.path.exists(lv_output_path):
                os.makedirs(lv_output_path)

            # 生成HTML图表
            plotly_driver = CPlotlyDriver(chan, plot_config=plot_config, plot_para=plot_para)
            html_path = os.path.join(lv_output_path, f"{output_name}.html")
            plotly_driver.savefig(html_path)
            print(f"已生成图表: {html_path}")

        except Exception as e:
            print(f"生成{level.name}级别可视化图表时出错: {e}")

    def generate_visualization(self, code, all_chans, plot_config):
        """
        生成可视化图表（所有级别）
        """
        try:
            for lv, chan in all_chans.items():
                self.generate_visualization_for_level(code, chan, plot_config, lv)
        except Exception as e:
            print(f"生成可视化图表时出错: {e}")

    def is_trading_time(self):
        """
        判断当前是否为交易时间和交易日
        交易时间：周一至周五 9:25-11:30, 13:00-15:00
        """
        # 如果debug模式启用，忽略交易日和交易时间限制
        if self.debug:
            return True
            
        try:
            # 获取当前时间和日期
            now = datetime.now()
            weekday = now.weekday()  # 周一为0，周日为6
            
            # 判断是否为周末
            if weekday >= 5:  # 周六(5)和周日(6)
                return False
            
            # 判断是否为交易日（通过qstock获取最新交易日）
            latest_trade_date = qs.latest_trade_date()
            latest_trade_date = datetime.strptime(latest_trade_date, "%Y-%m-%d")
            
            # 如果最新交易日不是今天，则今天不是交易日
            if latest_trade_date.date() != now.date():
                return False
            
            # 判断是否为交易时间
            current_time = now.time()
            morning_start = datetime.strptime("09:25", "%H:%M").time()
            morning_end = datetime.strptime("11:30", "%H:%M").time()
            afternoon_start = datetime.strptime("13:00", "%H:%M").time()
            afternoon_end = datetime.strptime("15:00", "%H:%M").time()
            
            # 在交易时间范围内
            if (morning_start <= current_time <= morning_end) or \
               (afternoon_start <= current_time <= afternoon_end):
                return True
            
            return False
        except Exception as e:
            print(f"判断交易时间时出错: {e}")
            # 出错时默认返回True，避免影响正常监控
            return True

    def run_analysis(self):
        """
        运行一次完整的分析
        """
        # 判断是否为交易时间和交易日
        if not self.is_trading_time():
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 非交易时间或非交易日，跳过本次分析")
            return
            
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 开始新一轮分析...")
        for code in self.codes:
            self.analyze_single_code(code)
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 本轮分析完成")

    def start_monitoring(self, interval=30):
        """
        开始定时监控
        interval: 间隔时间（秒）
        """
        print(f"开始定时监控，间隔 {interval} 秒")
        
        def run_periodically():
            # 获取行情信息
            self.get_market_data()
            # 缠论分析
            self.run_analysis()
            # 设置下一次执行
            timer = Timer(interval, run_periodically)
            timer.daemon = True
            timer.start()
        
        # 立即执行一次
        run_periodically()

    # 获取行情信息
    def get_market_data(self):
        # 调用mcp获取行情信息
        codes = ["上证指数", "深证成指", "沪深300", "中证500", "中证1000", "科创50"]
        market_index = index.data_service.get_index_realtime_data(codes)
        
        # 如果有WebSocket客户端连接，则广播行情数据
        if self.enable_websocket and market_index:
            # 转换数据格式以匹配前端期望的格式
            converted_data = []
            for item in market_index:
                converted_item = {
                    "code": item.get("代码", ""),
                    "name": item.get("名称", ""),
                    "change_percent": item.get("涨幅", 0.0),
                    "price": item.get("最新", 0.0),
                    "high": item.get("最高", 0.0),
                    "low": item.get("最低", 0.0),
                    "open": item.get("今开", 0.0),
                    "turnover_rate": item.get("换手率", 0.0) or item.get("换手 率", 0.0),  # 处理空格问题
                    "volume_ratio": item.get("量比", 0.0),
                    "pe_ratio": item.get("市盈率", 0.0),
                    "volume": item.get("成交量", 0),
                    "amount": item.get("成交额", 0.0),
                    "previous_close": item.get("昨收", 0.0),
                    "market_cap": item.get("总市值", 0),
                    "circulating_market_cap": item.get("流通市值", 0),
                    "market": item.get("市场", ""),
                    "time": item.get("时间", "")
                }
                converted_data.append(converted_item)
            
            market_update = {
                "type": "market_data_update",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "data": converted_data
            }
            json_message = json.dumps(market_update, ensure_ascii=False)
            print(f"广播行情数据到WebSocket客户端:{json_message}")
            self.broadcast_websocket_message(json_message)

    def start_websocket_server(self):
        """
        启动WebSocket服务器
        """
        if not WEBSOCKET_AVAILABLE:
            print("WebSocket依赖未安装，无法启动WebSocket服务器")
            return

        def run_websocket_server():
            try:
                # 使用asyncio.run()启动服务器（Python 3.7+）
                asyncio.run(self._websocket_server_coroutine())
            except AttributeError:
                # Python 3.6及以下版本的兼容处理
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                start_server = websockets.serve(self.websocket_handler, self.websocket_host, self.websocket_port)
                print(f"WebSocket服务器已启动，监听 {self.websocket_host}:{self.websocket_port}")
                loop.run_until_complete(start_server)
                loop.run_forever()
            except Exception as e:
                print(f"WebSocket服务器启动失败: {e}")

        # 在新线程中启动WebSocket服务器
        websocket_thread = Thread(target=run_websocket_server, daemon=True)
        websocket_thread.start()

    async def _websocket_server_coroutine(self):
        """
        WebSocket服务器协程
        """
        server = await websockets.serve(self.websocket_handler, self.websocket_host, self.websocket_port)
        print(f"WebSocket服务器已启动，监听 {self.websocket_host}:{self.websocket_port}")
        await server.wait_closed()

    async def websocket_handler(self, websocket):
        """
        WebSocket连接处理函数
        """
        # 将新连接添加到客户端集合
        self.websocket_clients.add(websocket)
        print(f"新的WebSocket客户端已连接: {websocket.remote_address}")
        
        try:
            async for message in websocket:
                # 处理客户端发送的消息
                try:
                    data = json.loads(message)
                    if data.get("type") == "config_update":
                        # 更新配置
                        with self.config_lock:
                            if "debug" in data:
                                self.debug = data["debug"]
                            if "generate_visualization" in data:
                                self.generate_visualization = data["generate_visualization"]
                            if "send_wechat_message" in data:
                                self.send_wechat_message = data["send_wechat_message"]
                        print(f"配置已更新: debug={self.debug}, generate_visualization={self.generate_visualization}, send_wechat_message={self.send_wechat_message}")
                except json.JSONDecodeError:
                    pass  # 忽略无法解析的JSON消息
        except websockets.exceptions.ConnectionClosed:
            print(f"WebSocket连接已关闭: {websocket.remote_address}")
        except websockets.exceptions.ConnectionClosedError:
            print(f"WebSocket连接错误: {websocket.remote_address}")
        except Exception as e:
            print(f"WebSocket处理过程中发生异常: {e}")
        finally:
            # 连接关闭时从客户端集合中移除
            try:
                self.websocket_clients.remove(websocket)
                print(f"WebSocket客户端已断开连接: {websocket.remote_address}")
            except KeyError:
                # 客户端可能已经被移除
                pass

    def broadcast_websocket_message(self, message):
        """
        向所有WebSocket客户端广播消息
        """
        if not self.enable_websocket or not WEBSOCKET_AVAILABLE:
            return
            
        # 在新线程中发送消息，避免阻塞
        def send_messages():
            disconnected_clients = set()
            for client in self.websocket_clients:
                try:
                    # 直接在新线程中创建事件循环并发送消息
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    loop.run_until_complete(client.send(message))
                    loop.close()
                except websockets.exceptions.ConnectionClosed:
                    print(f"WebSocket连接已关闭，将从客户端列表中移除")
                    disconnected_clients.add(client)
                except websockets.exceptions.ConnectionClosedError:
                    print(f"WebSocket连接错误，将从客户端列表中移除")
                    disconnected_clients.add(client)
                except Exception as e:
                    print(f"WebSocket消息发送失败: {e}")
                    disconnected_clients.add(client)
            
            # 移除已断开的客户端
            for client in disconnected_clients:
                try:
                    self.websocket_clients.remove(client)
                except KeyError:
                    # 客户端可能已经被移除
                    pass
                
        # 启动发送线程
        send_thread = Thread(target=send_messages, daemon=True)
        send_thread.start()


def main():

    index.intitialize_data_service()

    # 配置需要监控的股票代码
    codes = ["上证指数", "510050", "510500", "510300"]
    
    # 创建监控实例（debug模式默认关闭，可视化生成默认关闭）
    monitor = RealtimeMonitor(codes, debug=False, generate_visualization=False, enable_websocket=True)
    
    # 开始定时监控（每5秒）
    monitor.start_monitoring(5)
    
    # 保持程序运行
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("程序已退出")


if __name__ == "__main__":
    main()