import pandas as pd
import matplotlib.pyplot as plt
from typing import List, Dict, Any, Optional
from Chan import CChan
from ChanConfig import CChanConfig
from Common.CEnum import AUTYPE, DATA_SRC, KL_TYPE
from Strategy.BaseStrategy import CBaseStrategy
from DataAPI.BaoStockAPI import CBaoStock
from KLine.KLine_Unit import CKLine_Unit


class CAdvancedBacktester:
    """
    高级回测引擎，支持更多功能
    """

    def __init__(self, strategy: CBaseStrategy, result_dir: str = "backtest_results"):
        """
        初始化回测引擎
        :param strategy: 策略实例
        :param result_dir: 结果保存目录
        """
        self.strategy = strategy
        self.initial_capital = 100000  # 初始资金
        self.current_capital = self.initial_capital  # 当前资金
        self.history_capital = [self.initial_capital]  # 资金历史
        self.positions = []  # 持仓记录
        self.equity_curve = []  # 权益曲线
        self.result_dir = result_dir

    def run_backtest(
            self,
            code: str,
            begin_time: str,
            end_time: str,
            data_src: DATA_SRC,
            lv_list: List[KL_TYPE],
            config: CChanConfig,
            autype: AUTYPE = AUTYPE.QFQ,
            capital: float = 100000.0
    ) -> Dict[str, Any]:
        """
        运行回测
        :param code: 股票代码
        :param begin_time: 开始时间
        :param end_time: 结束时间
        :param data_src: 数据源
        :param lv_list: 级别列表
        :param config: 配置
        :param autype: 复权类型
        :param capital: 初始资金
        :return: 回测结果
        """
        self.initial_capital = capital
        self.current_capital = capital
        self.history_capital = [capital]

        print(f"开始回测: {code} 从 {begin_time} 到 {end_time}")

        # 初始化缠论实例，确保开启逐步返回
        # 创建新的配置对象，确保trigger_step为True
        config_backtest = config  # 直接使用传入的配置

        chan = CChan(
            code=code,
            begin_time=begin_time,
            end_time=end_time,
            data_src=data_src,
            lv_list=lv_list,
            config=config_backtest,
            autype=autype,
        )

        # 用于统计
        total_trades = 0
        winning_trades = 0
        total_profit = 0.0
        max_drawdown = 0.0
        peak_capital = capital

        # 逐步加载数据并执行策略
        for chan_snapshot in chan.step_load():
            # 执行策略
            self.strategy.on_bar(chan_snapshot, lv_list[0])
            
            # 更新资金曲线
            current_time = chan_snapshot[lv_list[0]][-1][-1].time
            current_price = chan_snapshot[lv_list[0]][-1][-1].close
            
            # 计算当前权益
            current_equity = self.calculate_equity(current_price)
            self.equity_curve.append((current_time, current_equity))
            self.history_capital.append(current_equity)
            
            # 计算最大回撤
            if current_equity > peak_capital:
                peak_capital = current_equity
            drawdown = (peak_capital - current_equity) / peak_capital * 100 if peak_capital > 0 else 0
            max_drawdown = max(max_drawdown, drawdown)

        # 统计结果
        transactions = self.strategy.get_transactions()
        for transaction in transactions:
            if transaction["type"] == "sell":
                total_trades += 1
                if transaction.get("profit_rate", 0) > 0:
                    winning_trades += 1
                total_profit += transaction.get("profit_rate", 0)

        # 计算统计指标
        win_rate = winning_trades / total_trades if total_trades > 0 else 0
        avg_profit = total_profit / total_trades if total_trades > 0 else 0

        final_capital = self.initial_capital * (1 + total_profit / 100) if total_trades > 0 else self.initial_capital
        roi = (final_capital - self.initial_capital) / self.initial_capital * 100

        # 计算夏普比率 (简化计算，假设无风险利率为0)
        returns = pd.Series(self.history_capital).pct_change().dropna()
        sharpe_ratio = returns.mean() / returns.std() * (252 ** 0.5) if returns.std() > 0 else 0

        result = {
            "initial_capital": self.initial_capital,
            "final_capital": final_capital,
            "roi": roi,
            "total_trades": total_trades,
            "winning_trades": winning_trades,
            "win_rate": win_rate,
            "total_profit": total_profit,
            "avg_profit": avg_profit,
            "max_drawdown": max_drawdown,
            "sharpe_ratio": sharpe_ratio,
            "transactions": transactions,
            "equity_curve": self.equity_curve,
            "code": code,
            "begin_time": begin_time,
            "end_time": end_time
        }

        print(f"回测完成:")
        print(f"  初始资金: {self.initial_capital}")
        print(f"  最终资金: {final_capital:.2f}")
        print(f"  收益率: {roi:.2f}%")
        print(f"  总交易数: {total_trades}")
        print(f"  胜率: {win_rate:.2f}")
        print(f"  平均收益: {avg_profit:.2f}%")
        print(f"  最大回撤: {max_drawdown:.2f}%")
        print(f"  夏普比率: {sharpe_ratio:.2f}")

        return result

    def calculate_equity(self, current_price: float) -> float:
        """
        计算当前权益
        :param current_price: 当前价格
        :return: 当前权益
        """
        equity = self.current_capital
        if self.strategy.get_hold_status():
            # 假设持有1手
            position_value = self.strategy.get_current_position() * current_price
            equity += position_value
        return equity

    def plot_equity_curve(self, result: Dict[str, Any], title: str = "Equity Curve"):
        """
        绘制权益曲线
        :param result: 回测结果
        :param title: 图表标题
        """
        try:
            equity_curve = result.get("equity_curve", [])
            if not equity_curve:
                print("没有权益曲线数据")
                return

            # 修复类型错误：将CTime转换为字符串
            times = [str(item[0]) for item in equity_curve]
            equities = [item[1] for item in equity_curve]

            plt.figure(figsize=(12, 6))
            plt.plot(times, equities)
            plt.title(title)
            plt.xlabel("时间")
            plt.ylabel("权益")
            plt.grid(True)
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.show()
        except ImportError:
            print("matplotlib未安装，无法绘制图表")

    def run_backtest_with_external_data(
            self,
            code: str,
            begin_time: str,
            end_time: str,
            data_src_type: DATA_SRC,
            lv_list: List[KL_TYPE],
            config: CChanConfig,
            autype: AUTYPE = AUTYPE.QFQ,
            capital: float = 100000.0
    ) -> Dict[str, Any]:
        """
        使用外部数据源运行回测
        :param code: 股票代码
        :param begin_time: 开始时间
        :param end_time: 结束时间
        :param data_src_type: 数据源类型
        :param lv_list: 级别列表
        :param config: 配置
        :param autype: 复权类型
        :param capital: 初始资金
        :return: 回测结果
        """
        self.initial_capital = capital
        self.current_capital = capital
        self.history_capital = [capital]

        print(f"开始使用外部数据源回测: {code} 从 {begin_time} 到 {end_time}")

        # 确保开启逐步返回
        # 注意：CChanConfig是不可变的，我们需要创建一个新的实例
        # 通过重新初始化来确保trigger_step为True
        config_dict = {}  # 创建一个空的配置字典
        # 尝试获取原始配置中的关键参数
        try:
            config_dict = {
                "divergence_rate": config.bs_point_conf.b_conf.divergence_rate,
                "min_zs_cnt": config.bs_point_conf.b_conf.min_zs_cnt,
                "max_bs2_rate": config.bs_point_conf.b_conf.max_bs2_rate,
                "bs_type": ",".join([t.value for t in config.bs_point_conf.b_conf.target_types]),
                "bs1_peak": config.bs_point_conf.b_conf.bs1_peak,
                "bsp2_follow_1": config.bs_point_conf.b_conf.bsp2_follow_1,
                "bsp3_follow_1": config.bs_point_conf.b_conf.bsp3_follow_1,
            }
        except Exception:
            pass  # 如果无法获取配置，就使用空字典

        config_dict["trigger_step"] = True
        config_backtest = CChanConfig(config_dict)

        # 初始化缠论实例
        chan = CChan(
            code=code,
            begin_time=begin_time,  # 这些参数实际上不会被使用
            end_time=end_time,
            data_src=data_src_type,
            lv_list=lv_list,
            config=config_backtest,
            autype=autype,
        )

        # 初始化数据源
        CBaoStock.do_init()
        data_src = CBaoStock(
            code=code,
            k_type=lv_list[0],
            begin_date=begin_time,
            end_date=end_time,
            autype=autype
        )

        # 用于统计
        total_trades = 0
        winning_trades = 0
        total_profit = 0.0
        max_drawdown = 0.0
        peak_capital = capital

        # 获取数据并逐步执行策略
        for klu in data_src.get_kl_data():
            chan.trigger_load({lv_list[0]: [klu]})
            self.strategy.on_bar(chan, lv_list[0])
            
            # 更新资金曲线
            current_time = klu.time
            current_price = klu.close
            
            # 计算当前权益
            current_equity = self.calculate_equity(current_price)
            self.equity_curve.append((current_time, current_equity))
            self.history_capital.append(current_equity)
            
            # 计算最大回撤
            if current_equity > peak_capital:
                peak_capital = current_equity
            drawdown = (peak_capital - current_equity) / peak_capital * 100 if peak_capital > 0 else 0
            max_drawdown = max(max_drawdown, drawdown)

        CBaoStock.do_close()

        # 统计结果
        transactions = self.strategy.get_transactions()
        for transaction in transactions:
            if transaction["type"] == "sell":
                total_trades += 1
                if transaction.get("profit_rate", 0) > 0:
                    winning_trades += 1
                total_profit += transaction.get("profit_rate", 0)

        # 计算统计指标
        win_rate = winning_trades / total_trades if total_trades > 0 else 0
        avg_profit = total_profit / total_trades if total_trades > 0 else 0

        final_capital = self.initial_capital * (1 + total_profit / 100) if total_trades > 0 else self.initial_capital
        roi = (final_capital - self.initial_capital) / self.initial_capital * 100

        # 计算夏普比率 (简化计算，假设无风险利率为0)
        returns = pd.Series(self.history_capital).pct_change().dropna()
        sharpe_ratio = returns.mean() / returns.std() * (252 ** 0.5) if returns.std() > 0 else 0

        result = {
            "initial_capital": self.initial_capital,
            "final_capital": final_capital,
            "roi": roi,
            "total_trades": total_trades,
            "winning_trades": winning_trades,
            "win_rate": win_rate,
            "total_profit": total_profit,
            "avg_profit": avg_profit,
            "max_drawdown": max_drawdown,
            "sharpe_ratio": sharpe_ratio,
            "transactions": transactions,
            "equity_curve": self.equity_curve,
            "code": code,
            "begin_time": begin_time,
            "end_time": end_time
        }

        print(f"回测完成:")
        print(f"  初始资金: {self.initial_capital}")
        print(f"  最终资金: {final_capital:.2f}")
        print(f"  收益率: {roi:.2f}%")
        print(f"  总交易数: {total_trades}")
        print(f"  胜率: {win_rate:.2f}")
        print(f"  平均收益: {avg_profit:.2f}%")
        print(f"  最大回撤: {max_drawdown:.2f}%")
        print(f"  夏普比率: {sharpe_ratio:.2f}")

        return result