from Chan import CChan
from ChanConfig import CChanConfig
from Common.CEnum import AUTYPE, DATA_SRC, KL_TYPE
from Plot.AnimatePlotDriver import CAnimateDriver
from Plot.PlotDriver import CPlotDriver
from Plot.PlotlyDriver import CPlotlyDriver
import sys
import os
from datetime import datetime, timedelta

def get_begin_time_by_level(level):
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

def monitor_stock(code_lists, begin_time, end_time, data_src, lv_list):
    if isinstance(code_lists, str):
        code_lists = [code_lists]
    
    # 根据级别列表中的第一个级别确定begin_time
    if len(lv_list) > 0:
        level = lv_list[0]
        begin_time = get_begin_time_by_level(level)
    
    for code in code_lists:
        print(f"{code}")
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

        chan = CChan(
            code=code,
            begin_time=begin_time,
            end_time=end_time,
            data_src=data_src,
            lv_list=lv_list,
            config=config,
            autype=AUTYPE.QFQ,
        )

        plot_para = {
            "seg": {
                "plot_trendline": True,
            },
            "bi": {
                #"show_num": True,
                "disp_end": True,
            },
            "figure": {
                "x_range": chan.get_max_kline_range(),
            },
            "marker": {
                # "markers": {  # text, position, color
                #     '2023/06/01': ('marker here', 'up', 'red'),
                #     '2023/06/08': ('marker here', 'down')
                # },
            }
        }

        if not config.trigger_step:
            kltype = [ str(x) for x in lv_list ]
            output_name = f"{code}-{",".join(kltype)}-{begin_time}-{str(data_src)}"

            # chan 是已经计算完成的 CChan 实例
            
            plotly_driver = CPlotlyDriver(chan, plot_config=plot_config, plot_para=plot_para)
            #plotly_driver.show()
            #plotly_driver.save2img(f'{output_name}.png')  # 保存为图片文件

            output_path = os.path.join(os.getcwd(), "output")
            if not os.path.exists(output_path):
                os.makedirs(output_path)
            plotly_driver.savefig(f'{output_path}/{output_name}.html')  # 保存为HTML文件
            '''
            plot_driver = CPlotDriver(
                chan,
                plot_config=plot_config,
                plot_para=plot_para,
            )
            plot_driver.figure.show()
            
            plot_driver.save2img(f"{output_name}.png")
            #plot_driver.save2html_mpld3(f"{output_name}.html")
            #input("Press Enter to continue...")
            '''
            
        else:
            CAnimateDriver(
                chan,
                plot_config=plot_config,
                plot_para=plot_para,
            )


if __name__ == "__main__":
    #code = "sz.159915"
    #code = "上证指数"
    #codes = ["上证指数", "sz.510050", "510500", "510300", "sz.159915", "sz.588000"]
    codes = ["上证指数", "sz.510050", "510500", "510300"]
    begin_time = "2025-08-01"  # 这个值现在会被动态覆盖
    end_time = None
    data_src = DATA_SRC.QSTOCK
    # 级别从大到小，如果涉及到日级以内，时间不要超过1个月
    lv_list = [ 
        KL_TYPE.K_MON,
        KL_TYPE.K_WEEK,
        KL_TYPE.K_DAY,
        KL_TYPE.K_60M,
        KL_TYPE.K_30M,
        KL_TYPE.K_15M,
        KL_TYPE.K_5M,
    ]
    for lv in lv_list:
        monitor_stock(codes, begin_time, end_time, data_src, [lv])