import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
from typing import Dict, List, Union, Optional, Tuple
from Chan import CChan
from Common.CEnum import KL_TYPE, BI_DIR, FX_TYPE, KLINE_DIR, TREND_TYPE
from .PlotMeta import CChanPlotMeta, CBi_meta, CSeg_meta, CZS_meta, CBS_Point_meta
import numpy as np


def y_axis_name(row_idx):
    """
    获取指定行的y轴名称
    """
    return f'yaxis{row_idx}' if row_idx > 1 else 'yaxis'


def parse_single_lv_plot_config(plot_config: Union[str, dict, list]) -> Dict[str, bool]:
    """
    返回单一级别的plot_config配置
    """
    def _format(s):
        return s if s.startswith("plot_") else f"plot_{s}"

    if isinstance(plot_config, dict):
        return {_format(k): v for k, v in plot_config.items()}
    elif isinstance(plot_config, str):
        return {_format(k.strip().lower()): True for k in plot_config.split(",")}
    elif isinstance(plot_config, list):
        return {_format(k.strip().lower()): True for k in plot_config}
    else:
        raise Exception("plot_config only support list/str/dict")


def parse_plot_config(plot_config: Union[str, dict, list], lv_list: List[KL_TYPE]) -> Dict[KL_TYPE, Dict[str, bool]]:
    """
    解析绘图配置
    """
    if isinstance(plot_config, dict):
        if all(isinstance(_key, str) for _key in plot_config.keys()):  # 单层字典
            return {lv: parse_single_lv_plot_config(plot_config) for lv in lv_list}
        elif all(isinstance(_key, KL_TYPE) for _key in plot_config.keys()):  # key为KL_TYPE
            for lv in lv_list:
                assert lv in plot_config
            return {lv: parse_single_lv_plot_config(plot_config[lv]) for lv in lv_list}
        else:
            raise Exception("plot_config if is dict, key must be str/KL_TYPE")
    return {lv: parse_single_lv_plot_config(plot_config) for lv in lv_list}


def cal_x_limit(meta: CChanPlotMeta, x_range):
    X_LEN = meta.klu_len
    return [X_LEN - x_range, X_LEN - 1] if x_range and X_LEN > x_range else [0, X_LEN - 1]


def GetPlotMeta(chan: CChan, figure_config) -> List[CChanPlotMeta]:
    plot_metas = [CChanPlotMeta(chan[kl_type]) for kl_type in chan.lv_list]
    if figure_config.get("only_top_lv", False):
        plot_metas = [plot_metas[0]]
    return plot_metas


class CPlotlyDriver:
    def __init__(self, chan: CChan, plot_config: Union[str, dict, list] = '', plot_para=None):
        if plot_para is None:
            plot_para = {}
        figure_config: dict = plot_para.get('figure', {})

        plot_config = parse_plot_config(plot_config, chan.lv_list)
        plot_metas = GetPlotMeta(chan, figure_config)
        self.lv_lst = chan.lv_list[:len(plot_metas)]
        self.chan = chan  # 保存chan对象用于标题显示

        x_range = self.GetRealXrange(figure_config, plot_metas[0])
        plot_macd: Dict[KL_TYPE, bool] = {kl_type: conf.get("plot_macd", False) for kl_type, conf in plot_config.items()}
        
        # 创建子图
        self.figure = self.create_figure(plot_metas, plot_macd, figure_config)
        
        sseg_begin = 0
        slv_seg_cnt = plot_para.get('seg', {}).get('sub_lv_cnt', None)
        sbi_begin = 0
        slv_bi_cnt = plot_para.get('bi', {}).get('sub_lv_cnt', None)
        srange_begin = 0
        assert slv_seg_cnt is None or slv_bi_cnt is None, "you can set at most one of seg_sub_lv_cnt/bi_sub_lv_cnt"

        # 绘制每个级别的图形
        current_row = 0
        
        for idx, (meta, lv) in enumerate(zip(plot_metas, self.lv_lst)):
            current_row += 1
            row_idx = current_row
            
            x_limits = cal_x_limit(meta, x_range)
            if lv != self.lv_lst[0]:
                if sseg_begin != 0 or sbi_begin != 0:
                    x_limits[0] = max(sseg_begin, sbi_begin)
                elif srange_begin != 0:
                    x_limits[0] = srange_begin
                    
            # 设置x轴范围
            self.figure.update_xaxes(range=x_limits, row=row_idx, col=1)
            
            # 设置x轴标签（日期，-45度展示）
            self.set_x_tick(meta, row_idx, x_limits)
            
            # 设置标题（代码-名称-级别）
            self.figure.update_layout(**{f'yaxis{row_idx}_title': f"{chan.code}/{self.format_kl_type(lv)}"})
            
            # 绘制元素
            self.DrawElement(plot_config[lv], meta, row_idx, plot_para, x_limits)
            
            # 如果需要绘制MACD，在单独的子图中绘制
            if plot_macd[lv]:
                current_row += 1
                macd_row_idx = current_row
                self.draw_macd(meta, macd_row_idx, x_limits, **plot_para.get('macd', {}))
                # 设置MACD子图的x轴范围
                self.figure.update_xaxes(range=x_limits, row=macd_row_idx, col=1)
                # 设置MACD子图的x轴标签
                self.set_x_tick(meta, macd_row_idx, x_limits)

            if lv != self.lv_lst[-1]:
                if slv_seg_cnt is not None:
                    sseg_begin = meta.sub_last_kseg_start_idx(slv_seg_cnt)
                if slv_bi_cnt is not None:
                    sbi_begin = meta.sub_last_kbi_start_idx(slv_bi_cnt)
                if x_range != 0:
                    srange_begin = meta.sub_range_start_idx(x_range)


    def format_kl_type(self, kl_type: KL_TYPE) -> str:
        """
        格式化K线类型显示名称
        """
        return kl_type.name.split('K_')[1] if 'K_' in kl_type.name else kl_type.name

    def GetRealXrange(self, figure_config, meta: CChanPlotMeta):
        x_range = figure_config.get("x_range", 0)
        bi_cnt = figure_config.get("x_bi_cnt", 0)
        seg_cnt = figure_config.get("x_seg_cnt", 0)
        x_begin_date = figure_config.get("x_begin_date", 0)
        if x_range != 0:
            assert bi_cnt == 0 and seg_cnt == 0 and x_begin_date == 0, "x_range/x_bi_cnt/x_seg_cnt/x_begin_date can not be set at the same time"
            return x_range
        if bi_cnt != 0:
            assert x_range == 0 and seg_cnt == 0 and x_begin_date == 0, "x_range/x_bi_cnt/x_seg_cnt/x_begin_date can not be set at the same time"
            X_LEN = meta.klu_len
            if len(meta.bi_list) < bi_cnt:
                return 0
            x_range = X_LEN-meta.bi_list[-bi_cnt].begin_x
            return x_range
        if seg_cnt != 0:
            assert x_range == 0 and bi_cnt == 0 and x_begin_date == 0, "x_range/x_bi_cnt/x_seg_cnt/x_begin_date can not be set at the same time"
            X_LEN = meta.klu_len
            if len(meta.seg_list) < seg_cnt:
                return 0
            x_range = X_LEN-meta.seg_list[-seg_cnt].begin_x
            return x_range
        if x_begin_date != 0:
            assert x_range == 0 and bi_cnt == 0 and seg_cnt == 0, "x_range/x_bi_cnt/x_seg_cnt/x_begin_date can not be set at the same time"
            x_range = 0
            for date_tick in meta.datetick[::-1]:
                if date_tick >= x_begin_date:
                    x_range += 1
                else:
                    break
            return x_range
        return x_range

    def create_figure(self, plot_metas: List[CChanPlotMeta], plot_macd: Dict[KL_TYPE, bool], figure_config):
        """
        创建图形布局，参考PlotDriver.py的实现方式
        """
        # 参考PlotDriver.py的图形高度设置方式
        default_w, default_h = 24, 10
        macd_h_ration = figure_config.get('macd_h', 0.3)
        w = figure_config.get('w', default_w)
        h = figure_config.get('h', default_h)

        # 计算总高度和高度比例，参考PlotDriver.py的实现
        total_h = 0
        row_heights = []
        specs = []
        row_titles = []
        
        self.macd_row_map = {}  # 记录每个级别对应的MACD行
        sub_pic_cnt = 0
        
        for meta, (lv, need_macd) in zip(plot_metas, plot_macd.items()):
            # 主图（K线图等）
            specs.append([{"secondary_y": True}])
            row_heights.append(1)
            row_titles.append(f"{self.chan.code}/{self.format_kl_type(lv)}")
            sub_pic_cnt += 1
            total_h += h
            
            # 如果需要MACD，添加MACD子图
            if need_macd:
                specs.append([{"secondary_y": True}])
                row_heights.append(macd_h_ration)
                row_titles.append(f"MACD - {self.chan.code}/{self.format_kl_type(lv)}")
                self.macd_row_map[lv] = sub_pic_cnt + 1  # 记录MACD子图行号
                sub_pic_cnt += 1
                total_h += h * macd_h_ration
        
        # 创建子图
        figure = make_subplots(
            rows=sub_pic_cnt,
            cols=1,
            specs=specs,
            shared_xaxes=True,
            vertical_spacing=0.05,
            row_titles=row_titles,
            row_heights=row_heights
        )
        
        # 设置图形大小，与PlotDriver.py保持一致
        figure.update_layout(
            title="缠论分析图",
            height=total_h * 50,  # matplotlib中1单位高度约等于100像素
            width=w * 60,  # matplotlib中1单位宽度约等于100像素
            showlegend=True
        )
        
        return figure

    def set_x_tick(self, meta: CChanPlotMeta, row_idx: int, x_limits):
        """
        设置x轴刻度，以日期为标签，-45度展示
        """
        # 确保索引在有效范围内
        begin_idx = max(0, int(x_limits[0]))
        end_idx = min(len(meta.datetick) - 1, int(x_limits[1]))
        
        # 获取指定范围内的日期
        tick_dates = meta.datetick[begin_idx:end_idx+1]
        tick_indices = list(range(begin_idx, end_idx+1))
        
        # 为了防止标签过于密集，只显示部分标签
        if len(tick_indices) > 20:  # 如果标签过多，减少显示数量
            step = len(tick_indices) // 20
            tick_indices = tick_indices[::step]
            tick_dates = tick_dates[::step]
        
        # 更新x轴标签
        self.figure.update_xaxes(
            tickvals=tick_indices,
            ticktext=tick_dates,
            tickangle=-45,
            row=row_idx, col=1
        )

    def DrawElement(self, plot_config: Dict[str, bool], meta: CChanPlotMeta, row_idx: int, plot_para, x_limits):
        if plot_config.get("plot_kline", False):
            self.draw_klu(meta, row_idx, **plot_para.get('kl', {}))
        if plot_config.get("plot_kline_combine", False):
            self.draw_klc(meta, row_idx, **plot_para.get('klc', {}))
        if plot_config.get("plot_bi", False):
            self.draw_bi(meta, row_idx, **plot_para.get('bi', {}))
        if plot_config.get("plot_seg", False):
            self.draw_seg(meta, row_idx, **plot_para.get('seg', {}))
        if plot_config.get("plot_segseg", False):
            self.draw_segseg(meta, row_idx, **plot_para.get('segseg', {}))
        if plot_config.get("plot_eigen", False):
            self.draw_eigen(meta, row_idx, **plot_para.get('eigen', {}))
        if plot_config.get("plot_segeigen", False):
            self.draw_segeigen(meta, row_idx, **plot_para.get('segeigen', {}))
        if plot_config.get("plot_zs", False):
            self.draw_zs(meta, row_idx, **plot_para.get('zs', {}))
        if plot_config.get("plot_segzs", False):
            self.draw_segzs(meta, row_idx, **plot_para.get('segzs', {}))
        if plot_config.get("plot_macd", False):
            # MACD将在单独的子图中绘制，在主图中不绘制
            pass
        if plot_config.get("plot_mean", False):
            self.draw_mean(meta, row_idx, **plot_para.get('mean', {}))
        if plot_config.get("plot_channel", False):
            self.draw_channel(meta, row_idx, **plot_para.get('channel', {}))
        if plot_config.get("plot_boll", False):
            self.draw_boll(meta, row_idx, **plot_para.get('boll', {}))
        if plot_config.get("plot_bsp", False):
            self.draw_bs_point(meta, row_idx, **plot_para.get('bsp', {}))
        if plot_config.get("plot_segbsp", False):
            self.draw_seg_bs_point(meta, row_idx, **plot_para.get('seg_bsp', {}))
        if plot_config.get("plot_demark", False):
            self.draw_demark(meta, row_idx, **plot_para.get('demark', {}))
        if plot_config.get("plot_marker", False):
            # markers需要特殊处理
            pass
        if plot_config.get("plot_rsi", False):
            # RSI绘制需要在secondary_y上
            pass
        if plot_config.get("plot_kdj", False):
            # KDJ绘制需要在secondary_y上
            pass

    def draw_klu(self, meta: CChanPlotMeta, row_idx: int, width=0.4, rugd=True, plot_mode="kl", **kwargs):
        # rugd: red up green down
        up_color = 'red' if rugd else 'green'
        down_color = 'green' if rugd else 'red'
        up_color = kwargs.get('up_color', up_color)
        down_color = kwargs.get('down_color', down_color)

        x_begin = 0  # 简化处理
        x_data = []
        open_data = []
        high_data = []
        low_data = []
        close_data = []
        color_data = []
        width_data = []

        for kl in meta.klu_iter():
            i = kl.idx
            if i < x_begin:
                continue
                
            x_data.append(i)
            open_data.append(kl.open)
            high_data.append(kl.high)
            low_data.append(kl.low)
            close_data.append(kl.close)
            
            # 确定颜色
            color = up_color if kl.close > kl.open else down_color
            color_data.append(color)
            width_data.append(width)

        if plot_mode == "kl":
            # 绘制K线图
            self.figure.add_trace(
                go.Candlestick(
                    x=x_data,
                    open=open_data,
                    high=high_data,
                    low=low_data,
                    close=close_data,
                    name="KLine",
                    increasing_line_color=up_color,
                    decreasing_line_color=down_color,
                    visible=True
                ),
                row=row_idx, col=1
            )
        elif plot_mode in ["close", "high", "low", "open"]:
            y_map = {
                "close": close_data,
                "high": high_data,
                "low": low_data,
                "open": open_data
            }
            y_data = y_map[plot_mode]
            
            self.figure.add_trace(
                go.Scatter(
                    x=x_data,
                    y=y_data,
                    mode='lines',
                    name=f"{plot_mode.capitalize()} Line",
                    line=dict(color=up_color),
                    visible=True
                ),
                row=row_idx, col=1
            )

    def draw_klc(self, meta: CChanPlotMeta, row_idx: int, width=0.4, plot_single_kl=True, **kwargs):
        color_type = {
            FX_TYPE.TOP: 'red',
            FX_TYPE.BOTTOM: 'blue',
            KLINE_DIR.UP: 'green',
            KLINE_DIR.DOWN: 'green'
        }
        
        x_begin = 0  # 简化处理
        
        for klc_meta in meta.klc_list:
            if klc_meta.klu_list[-1].idx + width < x_begin:
                continue
            if klc_meta.end_idx == klc_meta.begin_idx and not plot_single_kl:
                continue
                
            color = color_type.get(klc_meta.type, 'green')
            color = kwargs.get('color', color)
            
            # 绘制K线合并形状
            self.figure.add_shape(
                type="rect",
                x0=klc_meta.begin_idx - width,
                x1=klc_meta.end_idx + width,
                y0=klc_meta.low,
                y1=klc_meta.high,
                line=dict(color=color),
                row=row_idx, col=1
            )

    def draw_bi(self, meta: CChanPlotMeta, row_idx: int, color='black', show_num=False, num_fontsize=15, 
                num_color="red", sub_lv_cnt=None, facecolor='green', alpha=0.1, disp_end=False, 
                end_color='black', end_fontsize=10, **kwargs):
        x_begin = 0  # 简化处理
        
        for bi_idx, bi in enumerate(meta.bi_list):
            if bi.end_x < x_begin:
                continue
                
            # 绘制笔
            self.figure.add_trace(
                go.Scatter(
                    x=[bi.begin_x, bi.end_x],
                    y=[bi.begin_y, bi.end_y],
                    mode='lines',
                    line=dict(color=color, width=2),
                    name=f"Bi {bi.idx}",
                    visible=True
                ),
                row=row_idx, col=1
            )
            
            # 显示数字
            if show_num and bi.begin_x >= x_begin:
                self.figure.add_trace(
                    go.Scatter(
                        x=[(bi.begin_x + bi.end_x) / 2],
                        y=[(bi.begin_y + bi.end_y) / 2],
                        mode='text',
                        text=[str(bi.idx)],
                        textfont=dict(size=num_fontsize, color=num_color),
                        showlegend=False,
                        visible=True
                    ),
                    row=row_idx, col=1
                )

    def draw_seg(self, meta: CChanPlotMeta, row_idx: int, width=5, color="green", sub_lv_cnt=None, 
                 facecolor='green', alpha=0.1, disp_end=False, end_color='green', end_fontsize=13, 
                 plot_trendline=False, trendline_color='red', trendline_width=3, show_num=False, 
                 num_fontsize=25, num_color="blue", **kwargs):
        x_begin = 0  # 简化处理

        for seg_idx, seg_meta in enumerate(meta.seg_list):
            if seg_meta.end_x < x_begin:
                continue
                
            # 根据是否确定选择线型
            line_style = dict(width=width, color=color)
            if not seg_meta.is_sure:
                line_style['dash'] = 'dash'
                
            # 绘制线段
            self.figure.add_trace(
                go.Scatter(
                    x=[seg_meta.begin_x, seg_meta.end_x],
                    y=[seg_meta.begin_y, seg_meta.end_y],
                    mode='lines',
                    line=line_style,
                    name=f"Segment {seg_meta.idx}",
                    visible=True
                ),
                row=row_idx, col=1
            )
            
            # 显示数字
            if show_num and seg_meta.begin_x >= x_begin:
                self.figure.add_trace(
                    go.Scatter(
                        x=[(seg_meta.begin_x + seg_meta.end_x) / 2],
                        y=[(seg_meta.begin_y + seg_meta.end_y) / 2],
                        mode='text',
                        text=[str(seg_meta.idx)],
                        textfont=dict(size=num_fontsize, color=num_color),
                        showlegend=False,
                        visible=True
                    ),
                    row=row_idx, col=1
                )

    def draw_segseg(self, meta: CChanPlotMeta, row_idx: int, width=7, color="brown", disp_end=False, 
                    end_color='brown', end_fontsize=15, show_num=False, num_fontsize=30, num_color="blue", **kwargs):
        x_begin = 0  # 简化处理

        for seg_idx, seg_meta in enumerate(meta.segseg_list):
            if seg_meta.end_x < x_begin:
                continue
                
            # 根据是否确定选择线型
            line_style = dict(width=width, color=color)
            if not seg_meta.is_sure:
                line_style['dash'] = 'dash'
                
            # 绘制高级线段
            self.figure.add_trace(
                go.Scatter(
                    x=[seg_meta.begin_x, seg_meta.end_x],
                    y=[seg_meta.begin_y, seg_meta.end_y],
                    mode='lines',
                    line=line_style,
                    name=f"SegSeg {seg_meta.idx}",
                    visible=True
                ),
                row=row_idx, col=1
            )
            
            # 显示数字
            if show_num and seg_meta.begin_x >= x_begin:
                self.figure.add_trace(
                    go.Scatter(
                        x=[(seg_meta.begin_x + seg_meta.end_x) / 2],
                        y=[(seg_meta.begin_y + seg_meta.end_y) / 2],
                        mode='text',
                        text=[str(seg_meta.idx)],
                        textfont=dict(size=num_fontsize, color=num_color),
                        showlegend=False,
                        visible=True
                    ),
                    row=row_idx, col=1
                )

    def draw_zs(self, meta: CChanPlotMeta, row_idx: int, color='orange', linewidth=2, sub_linewidth=0.5, 
                show_text=False, fontsize=14, text_color='orange', draw_one_bi_zs=False, **kwargs):
        x_begin = 0  # 简化处理
        
        for zs_meta in meta.zs_lst:
            if not draw_one_bi_zs and zs_meta.is_onebi_zs:
                continue
            if zs_meta.begin + zs_meta.w < x_begin:
                continue
                
            line_style = dict(width=linewidth, color=color)
            if not zs_meta.is_sure:
                line_style['dash'] = 'dash'
                
            # 绘制中枢矩形
            self.figure.add_shape(
                type="rect",
                x0=zs_meta.begin,
                x1=zs_meta.begin + zs_meta.w,
                y0=zs_meta.low,
                y1=zs_meta.low + zs_meta.h,
                line=line_style,
                row=row_idx, col=1
            )
            
            # 显示文字
            if show_text:
                self.figure.add_trace(
                    go.Scatter(
                        x=[zs_meta.begin, zs_meta.begin + zs_meta.w],
                        y=[zs_meta.low, zs_meta.low + zs_meta.h],
                        mode='text',
                        text=[f"{zs_meta.low:.2f}", f"{zs_meta.low + zs_meta.h:.2f}"],
                        textfont=dict(size=fontsize, color=text_color),
                        showlegend=False,
                        visible=True
                    ),
                    row=row_idx, col=1
                )

    def draw_segzs(self, meta: CChanPlotMeta, row_idx: int, color='red', linewidth=10, sub_linewidth=4, **kwargs):
        x_begin = 0  # 简化处理
        
        for zs_meta in meta.segzs_lst:
            if zs_meta.begin + zs_meta.w < x_begin:
                continue
                
            line_style = dict(width=linewidth, color=color)
            if not zs_meta.is_sure:
                line_style['dash'] = 'dash'
                
            # 绘制线段中枢矩形
            self.figure.add_shape(
                type="rect",
                x0=zs_meta.begin,
                x1=zs_meta.begin + zs_meta.w,
                y0=zs_meta.low,
                y1=zs_meta.low + zs_meta.h,
                line=line_style,
                row=row_idx, col=1
            )

    def draw_mean(self, meta: CChanPlotMeta, row_idx: int, **kwargs):
        # 绘制均线
        x_data = list(range(meta.klu_len))
        
        if meta.klu_len > 0:
            # 获取第一个K线单元查看trend数据结构
            first_klu = next(meta.klu_iter())
            if hasattr(first_klu, 'trend') and TREND_TYPE.MEAN in first_klu.trend:
                Ts = list(first_klu.trend[TREND_TYPE.MEAN].keys())
                
                for T in Ts:
                    mean_data = []
                    for klu in meta.klu_iter():
                        if TREND_TYPE.MEAN in klu.trend and T in klu.trend[TREND_TYPE.MEAN]:
                            mean_data.append(klu.trend[TREND_TYPE.MEAN][T])
                        else:
                            mean_data.append(None)
                    
                    self.figure.add_trace(
                        go.Scatter(
                            x=x_data,
                            y=mean_data,
                            mode='lines',
                            name=f'{T} Mean Line',
                            visible=True
                        ),
                        row=row_idx, col=1
                    )

    def draw_channel(self, meta: CChanPlotMeta, row_idx: int, T=None, top_color="red", bottom_color="blue", 
                     linewidth=3, linestyle="solid", **kwargs):
        # 绘制通道线
        x_data = list(range(meta.klu_len))
        
        if meta.klu_len > 0:
            first_klu = next(meta.klu_iter())
            if hasattr(first_klu, 'trend'):
                if TREND_TYPE.MAX in first_klu.trend and TREND_TYPE.MIN in first_klu.trend:
                    config_T_lst = sorted(list(first_klu.trend[TREND_TYPE.MAX].keys()))
                    if T is None and config_T_lst:
                        T = config_T_lst[-1]
                    elif T not in config_T_lst:
                        return  # T不在配置中
                        
                    # 获取通道数据
                    top_data = []
                    bottom_data = []
                    for klu in meta.klu_iter():
                        if TREND_TYPE.MAX in klu.trend and T in klu.trend[TREND_TYPE.MAX]:
                            top_data.append(klu.trend[TREND_TYPE.MAX][T])
                        else:
                            top_data.append(None)
                            
                        if TREND_TYPE.MIN in klu.trend and T in klu.trend[TREND_TYPE.MIN]:
                            bottom_data.append(klu.trend[TREND_TYPE.MIN][T])
                        else:
                            bottom_data.append(None)
                    
                    # 绘制上线
                    self.figure.add_trace(
                        go.Scatter(
                            x=x_data,
                            y=top_data,
                            mode='lines',
                            line=dict(color=top_color, width=linewidth),
                            name=f'{T}-TOP-channel',
                            visible=True
                        ),
                        row=row_idx, col=1
                    )
                    
                    # 绘制下线
                    self.figure.add_trace(
                        go.Scatter(
                            x=x_data,
                            y=bottom_data,
                            mode='lines',
                            line=dict(color=bottom_color, width=linewidth),
                            name=f'{T}-BOTTOM-channel',
                            visible=True
                        ),
                        row=row_idx, col=1
                    )

    def draw_boll(self, meta: CChanPlotMeta, row_idx: int, mid_color="black", up_color="blue", down_color="purple", **kwargs):
        # 绘制布林带
        x_data = []
        mid_data = []
        up_data = []
        down_data = []
        
        for idx, klu in enumerate(meta.klu_iter()):
            if hasattr(klu, 'boll'):
                x_data.append(idx)
                mid_data.append(klu.boll.MID)
                up_data.append(klu.boll.UP)
                down_data.append(klu.boll.DOWN)
        
        if x_data:
            # 中线
            self.figure.add_trace(
                go.Scatter(
                    x=x_data,
                    y=mid_data,
                    mode='lines',
                    line=dict(color=mid_color),
                    name='BOLL MID',
                    visible=True
                ),
                row=row_idx, col=1
            )
            
            # 上线
            self.figure.add_trace(
                go.Scatter(
                    x=x_data,
                    y=up_data,
                    mode='lines',
                    line=dict(color=up_color),
                    name='BOLL UP',
                    visible=True
                ),
                row=row_idx, col=1
            )
            
            # 下线
            self.figure.add_trace(
                go.Scatter(
                    x=x_data,
                    y=down_data,
                    mode='lines',
                    line=dict(color=down_color),
                    name='BOLL DOWN',
                    visible=True
                ),
                row=row_idx, col=1
            )

    def draw_bs_point(self, meta: CChanPlotMeta, row_idx: int, buy_color='red', sell_color='green', fontsize=15, 
                      arrow_l=0.15, arrow_h=0.2, arrow_w=1, **kwargs):
        self.bsp_common_draw(meta.bs_point_lst, row_idx, buy_color, sell_color, fontsize, arrow_l, arrow_h, arrow_w)

    def draw_seg_bs_point(self, meta: CChanPlotMeta, row_idx: int, buy_color='red', sell_color='green', fontsize=18, 
                          arrow_l=0.2, arrow_h=0.25, arrow_w=1.2, **kwargs):
        self.bsp_common_draw(meta.seg_bsp_lst, row_idx, buy_color, sell_color, fontsize, arrow_l, arrow_h, arrow_w)

    def bsp_common_draw(self, bsp_list: List[CBS_Point_meta], row_idx: int, buy_color, sell_color, fontsize, 
                        arrow_l, arrow_h, arrow_w):
        x_begin = 0  # 简化处理
        
        # 获取y轴范围来计算箭头长度，参考PlotDriver.py的方式
        y_axis_name_str = f'yaxis{row_idx}' if row_idx > 1 else 'yaxis'
        y_axis = self.figure.layout[y_axis_name_str].range if y_axis_name_str in self.figure.layout else None
        # 在PlotDriver.py中，y_range = self.y_max - self.y_min，这里我们模拟这个计算方式
        # 通过查看当前视图内的K线数据来计算y_range
        y_range = (y_axis[1] - y_axis[0]) if y_axis else 1  # 使用实际的y轴范围
        
        # 根据PlotDriver.py，进一步缩小箭头长度，避免偏离太远
        # PlotDriver.py中arrow_l默认是0.15或0.2，我们保持一致
        arrow_len = arrow_l * y_range
        arrow_head = arrow_len * arrow_h
        
        annotations = []
        
        for bsp in bsp_list:
            if bsp.x < x_begin:
                continue
                
            color = buy_color if bsp.is_buy else sell_color
            
            # 根据PlotDriver.py实现，计算箭头方向
            # 注意：PlotDriver.py中arrow_dir = 1 if bsp.is_buy else -1
            # 买入点(bsp.is_buy=True)箭头向下，卖出点(bsp.is_buy=False)箭头向上
            arrow_dir = 1 if bsp.is_buy else -1
            
            # 箭头起点和终点位置
            text_x = bsp.x
            text_y = bsp.y - arrow_len * arrow_dir
            arrow_x = bsp.x
            arrow_y = bsp.y - arrow_len * arrow_dir
            arrow_dx = 0
            arrow_dy = (arrow_len - arrow_head) * arrow_dir
            
            # 添加箭头注释
            annotations.append(
                dict(
                    x=arrow_x,
                    y=arrow_y + arrow_dy,
                    ax=arrow_x + arrow_dx,
                    ay=arrow_y,
                    xref=f'x{row_idx}',
                    yref=f'y{row_idx}',
                    axref=f'x{row_idx}',
                    ayref=f'y{row_idx}',
                    showarrow=True,
                    arrowhead=2,  # 箭头样式
                    arrowsize=1,
                    arrowwidth=arrow_w,
                    arrowcolor=color
                )
            )
            
            # 添加文本注释
            annotations.append(
                dict(
                    x=text_x,
                    y=text_y,
                    text=bsp.desc(),
                    font=dict(size=fontsize, color=color),
                    xref=f'x{row_idx}',
                    yref=f'y{row_idx}',
                    showarrow=False,
                    bgcolor='rgba(255,255,255,0.8)',  # 半透明白色背景使文字更清晰
                    bordercolor='rgba(0,0,0,0)'
                )
            )
        
        # 批量添加注释
        self.figure.update_layout(annotations=annotations)

    def draw_demark(self, meta: CChanPlotMeta, row_idx: int, setup_color='blue', countdown_color='red', fontsize=12, 
                    min_setup=9, max_countdown_background='yellow', begin_line_color='purple', begin_line_style='dash', **kwargs):
        # 绘制Demark点
        x_begin = 0  # 简化处理
        
        setup_x = []
        setup_y = []
        setup_text = []
        
        countdown_x = []
        countdown_y = []
        countdown_text = []
        
        for klu in meta.klu_iter():
            if klu.idx < x_begin:
                continue
                
            # 处理setup点
            for demark_idx in klu.demark.get_setup():
                if demark_idx['series'].idx < min_setup or not demark_idx['series'].setup_finished:
                    continue
                    
                setup_x.append(klu.idx)
                # 根据方向确定y位置
                y_pos = klu.low if demark_idx['dir'] == BI_DIR.DOWN else klu.high
                setup_y.append(y_pos)
                setup_text.append(str(demark_idx['idx']))
            
            # 处理countdown点
            for demark_idx in klu.demark.get_countdown():
                countdown_x.append(klu.idx)
                # 根据方向确定y位置
                y_pos = klu.low if demark_idx['dir'] == BI_DIR.DOWN else klu.high
                countdown_y.append(y_pos)
                countdown_text.append(str(demark_idx['idx']))
        
        # 绘制setup点
        if setup_x:
            self.figure.add_trace(
                go.Scatter(
                    x=setup_x,
                    y=setup_y,
                    mode='text',
                    text=setup_text,
                    textfont=dict(size=fontsize, color=setup_color),
                    name="Demark Setup",
                    visible=True
                ),
                row=row_idx, col=1
            )
        
        # 绘制countdown点
        if countdown_x:
            self.figure.add_trace(
                go.Scatter(
                    x=countdown_x,
                    y=countdown_y,
                    mode='text',
                    text=countdown_text,
                    textfont=dict(size=fontsize, color=countdown_color),
                    name="Demark Countdown",
                    visible=True
                ),
                row=row_idx, col=1
            )

    def draw_eigen(self, meta: CChanPlotMeta, row_idx: int, color_top="red", color_bottom="blue", aplha=0.5, only_peak=False, **kwargs):
        self.plot_eigen_common(meta.eigenfx_lst, row_idx, color_top, color_bottom, aplha, only_peak)

    def draw_segeigen(self, meta: CChanPlotMeta, row_idx: int, color_top="red", color_bottom="blue", aplha=0.5, only_peak=False, **kwargs):
        self.plot_eigen_common(meta.seg_eigenfx_lst, row_idx, color_top, color_bottom, aplha, only_peak)

    def plot_eigen_common(self, eigenfx_lst, row_idx: int, color_top="red", color_bottom="blue", aplha=0.5, only_peak=False):
        x_begin = 0  # 简化处理
        
        for eigenfx_meta in eigenfx_lst:
            color = color_top if eigenfx_meta.fx == FX_TYPE.TOP else color_bottom
            
            for idx, eigen_meta in enumerate(eigenfx_meta.ele):
                if eigen_meta.begin_x + eigen_meta.w < x_begin:
                    continue
                if only_peak and idx != 1:
                    continue
                    
                # 绘制特征序列矩形
                self.figure.add_shape(
                    type="rect",
                    x0=eigen_meta.begin_x,
                    x1=eigen_meta.begin_x + eigen_meta.w,
                    y0=eigen_meta.begin_y,
                    y1=eigen_meta.begin_y + eigen_meta.h,
                    fillcolor=color,
                    opacity=aplha,
                    line=dict(width=0),
                    row=row_idx, col=1
                )

    def draw_macd(self, meta: CChanPlotMeta, row_idx: int, x_limits, width=0.4, **kwargs):
        macd_lst = [klu.macd for klu in meta.klu_iter()]
        assert macd_lst[0] is not None, "you can't draw macd until you delete macd_metric=False"

        x_begin = x_limits[0]
        x_idx = list(range(len(macd_lst)))[x_begin:]
        dif_line = [macd.DIF for macd in macd_lst[x_begin:]]
        dea_line = [macd.DEA for macd in macd_lst[x_begin:]]
        macd_bar = [macd.macd for macd in macd_lst[x_begin:]]
        
        # 计算MACD值的范围，用于设置y轴范围
        y_min = min([min(dif_line), min(dea_line), min(macd_bar)])
        y_max = max([max(dif_line), max(dea_line), max(macd_bar)])
        y_range = y_max - y_min
        # 添加一些边距
        y_range_pad = kwargs.get('y_range_pad', 0.1)
        y_min -= y_range * y_range_pad
        y_max += y_range * y_range_pad
        
        # 先设置MACD子图的y轴范围，使其与主图分离
        self.figure.update_yaxes(
            range=[y_min, y_max],
            row=row_idx, col=1
        )

        # 绘制DIF线
        self.figure.add_trace(
            go.Scatter(
                x=x_idx,
                y=dif_line,
                mode='lines',
                name='DIF',
                line=dict(color='#FFA500'),
                visible=True
            ),
            row=row_idx, col=1
        )

        # 绘制DEA线
        self.figure.add_trace(
            go.Scatter(
                x=x_idx,
                y=dea_line,
                mode='lines',
                name='DEA',
                line=dict(color='#0000FF'),
                visible=True
            ),
            row=row_idx, col=1
        )

        # 绘制MACD柱状图
        colors = ['red' if macd >= 0 else '#006400' for macd in macd_bar]
        self.figure.add_trace(
            go.Bar(
                x=x_idx,
                y=macd_bar,
                name='MACD',
                marker_color=colors,
                width=width,
                visible=True
            ),
            row=row_idx, col=1
        )

    def show(self, width=None, height=None, config=None):
        """
        显示图形
        """
        if width and height:
            self.figure.update_layout(width=width, height=height)
            
        # 如果没有提供config参数，则使用默认配置
        if config is None:
            config = {
                'displayModeBar': True,  # 显示工具栏
                'displaylogo': False,    # 不显示plotly logo
            }
            
        self.figure.show(config=config)

    def savefig(self, path, width=None, height=None, config=None):
        """
        保存图形到文件
        """
        if width and height:
            self.figure.update_layout(width=width, height=height)
            
        # 如果没有提供config参数，则使用默认配置
        if config is None:
            config = {
                'displayModeBar': True,  # 显示工具栏
                'displaylogo': False,    # 不显示plotly logo
            }
            
        self.figure.write_html(path, config=config)

    def save_html(self, path, width=None, height=None, config=None):
        """
        保存图形到HTML文件（与main.py兼容的接口）
        """
        self.savefig(path, width, height, config)
        
    def save2html(self, path, width=None, height=None, config=None):
        """
        保存图形到HTML文件（与PlotDriver.py接口一致）
        """
        self.savefig(path, width, height, config)
