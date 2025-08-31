import os
import pandas as pd
from typing import Dict, Any, List
from datetime import datetime


class CHTMLVisualization:
    """
    回测结果HTML可视化类
    """

    def __init__(self, output_dir: str = "backtest_results"):
        """
        初始化HTML可视化器
        :param output_dir: 输出目录
        """
        self.output_dir = output_dir
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def generate_summary_html(self, result: Dict[str, Any], strategy_name: str, output_file: str = None) -> str:
        """
        生成回测结果摘要HTML
        :param result: 回测结果
        :param strategy_name: 策略名称
        :param output_file: 输出文件名
        :return: HTML内容
        """
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"{self.output_dir}/backtest_summary_{timestamp}.html"

        html_content = f"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>回测结果 - {strategy_name}</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background-color: white;
            border-radius: 10px;
            box-shadow: 0 0 10px rgba(0,0,0,0.1);
            padding: 20px;
        }}
        h1 {{
            color: #333;
            text-align: center;
            border-bottom: 2px solid #4CAF50;
            padding-bottom: 10px;
        }}
        .summary-card {{
            background-color: #f9f9f9;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 20px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
        }}
        .metrics-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 15px;
            margin-top: 20px;
        }}
        .metric-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border-radius: 8px;
            padding: 15px;
            text-align: center;
        }}
        .metric-value {{
            font-size: 24px;
            font-weight: bold;
            margin: 10px 0;
        }}
        .metric-label {{
            font-size: 14px;
            opacity: 0.9;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }}
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }}
        th {{
            background-color: #4CAF50;
            color: white;
        }}
        tr:hover {{
            background-color: #f5f5f5;
        }}
        .transaction-table {{
            overflow-x: auto;
        }}
        .chart-container {{
            height: 400px;
            margin: 30px 0;
        }}
        .positive {{
            color: #4CAF50;
        }}
        .negative {{
            color: #f44336;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>回测结果报告 - {strategy_name}</h1>
        
        <div class="summary-card">
            <h2>策略摘要</h2>
            <div class="metrics-grid">
                <div class="metric-card">
                    <div class="metric-label">初始资金</div>
                    <div class="metric-value">¥{result['initial_capital']:,.2f}</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">最终资金</div>
                    <div class="metric-value">¥{result['final_capital']:,.2f}</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">收益率</div>
                    <div class="metric-value {'positive' if result['roi'] >= 0 else 'negative'}">{result['roi']:.2f}%</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">夏普比率</div>
                    <div class="metric-value">{'N/A' if pd.isna(result['sharpe_ratio']) else f"{result['sharpe_ratio']:.2f}"}</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">交易次数</div>
                    <div class="metric-value">{result['total_trades']}</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">胜率</div>
                    <div class="metric-value">{result['win_rate']:.2f}</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">最大回撤</div>
                    <div class="metric-value {'negative' if result.get('max_drawdown', 0) > 0 else ''}">{result.get('max_drawdown', 0):.2f}%</div>
                </div>
                <div class="metric-card">
                    <div class="metric-label">平均收益</div>
                    <div class="metric-value {'positive' if result['avg_profit'] >= 0 else 'negative'}">{result['avg_profit']:.2f}%</div>
                </div>
            </div>
        </div>
        
        <div class="summary-card">
            <h2>交易记录</h2>
            <div class="transaction-table">
                <table>
                    <thead>
                        <tr>
                            <th>时间</th>
                            <th>类型</th>
                            <th>价格</th>
                            <th>原因</th>
                            <th>收益率</th>
                        </tr>
                    </thead>
                    <tbody>
"""

        # 添加交易记录
        transactions = result.get("transactions", [])
        for transaction in transactions:
            profit_rate = transaction.get("profit_rate", 0)
            profit_class = "positive" if profit_rate >= 0 else "negative"
            html_content += f"""
                        <tr>
                            <td>{transaction['time']}</td>
                            <td>{transaction['type']}</td>
                            <td>¥{transaction['price']:.2f}</td>
                            <td>{transaction['reason']}</td>
                            <td class="{profit_class}">{
                                f"{profit_rate:.2f}%" if transaction["type"] == "sell" else "-"
                            }</td>
                        </tr>
"""

        html_content += f"""
                    </tbody>
                </table>
            </div>
        </div>
        
        <div class="summary-card">
            <h2>策略说明</h2>
            <p>本报告展示了 {strategy_name} 策略的回测结果。策略基于缠论买卖点进行交易决策，通过历史数据验证策略的有效性。</p>
        </div>
    </div>
</body>
</html>
"""

        # 保存HTML文件
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(html_content)

        print(f"HTML报告已生成: {output_file}")
        return html_content

    def generate_comparison_html(self, results: List[Dict[str, Any]], strategy_names: List[str], output_file: str = None) -> str:
        """
        生成策略对比HTML
        :param results: 回测结果列表
        :param strategy_names: 策略名称列表
        :param output_file: 输出文件名
        :return: HTML内容
        """
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"{self.output_dir}/strategy_comparison_{timestamp}.html"

        html_content = """
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>策略对比报告</title>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background-color: white;
            border-radius: 10px;
            box-shadow: 0 0 10px rgba(0,0,0,0.1);
            padding: 20px;
        }
        h1 {
            color: #333;
            text-align: center;
            border-bottom: 2px solid #4CAF50;
            padding-bottom: 10px;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
        }
        th, td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }
        th {
            background-color: #4CAF50;
            color: white;
        }
        tr:hover {
            background-color: #f5f5f5;
        }
        .positive {
            color: #4CAF50;
        }
        .negative {
            color: #f44336;
        }
        .best {
            background-color: #e8f5e9;
            font-weight: bold;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>策略对比报告</h1>
        <table>
            <thead>
                <tr>
                    <th>策略名称</th>
                    <th>收益率 (%)</th>
                    <th>胜率</th>
                    <th>最大回撤 (%)</th>
                    <th>夏普比率</th>
                    <th>交易次数</th>
                    <th>平均收益 (%)</th>
                </tr>
            </thead>
            <tbody>
"""

        # 找出最佳策略（按收益率）
        best_roi_index = max(range(len(results)), key=lambda i: results[i]['roi']) if results else 0

        for i, (result, name) in enumerate(zip(results, strategy_names)):
            row_class = "best" if i == best_roi_index else ""
            html_content += f"""
                <tr class="{row_class}">
                    <td>{name}</td>
                    <td class="{'positive' if result['roi'] >= 0 else 'negative'}">{result['roi']:.2f}</td>
                    <td>{result['win_rate']:.2f}</td>
                    <td class="{'negative' if result.get('max_drawdown', 0) > 0 else ''}">{result.get('max_drawdown', 0):.2f}</td>
                    <td>{'N/A' if pd.isna(result['sharpe_ratio']) else f"{result['sharpe_ratio']:.2f}"}</td>
                    <td>{result['total_trades']}</td>
                    <td class="{'positive' if result['avg_profit'] >= 0 else 'negative'}">{result['avg_profit']:.2f}</td>
                </tr>
"""

        html_content += """
            </tbody>
        </table>
        
        <div>
            <h2>分析说明</h2>
            <p>本报告对比了不同策略的回测结果。通过收益率、胜率、最大回撤、夏普比率等关键指标，可以评估各策略的表现。</p>
            <p><strong>标记为绿色的行</strong>表示在收益率方面表现最佳的策略。</p>
        </div>
    </div>
</body>
</html>
"""

        # 保存HTML文件
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(html_content)

        print(f"策略对比报告已生成: {output_file}")
        return html_content

    def generate_equity_curve_html(self, result: Dict[str, Any], strategy_name: str, output_file: str = None) -> str:
        """
        生成包含权益曲线的HTML报告
        :param result: 回测结果
        :param strategy_name: 策略名称
        :param output_file: 输出文件名
        :return: HTML内容
        """
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"{self.output_dir}/equity_curve_{timestamp}.html"

        # 准备权益曲线数据
        equity_curve = result.get("equity_curve", [])
        times = [str(item[0]) for item in equity_curve]
        equities = [item[1] for item in equity_curve]

        # 转换为JavaScript数组格式
        times_js = str(times)
        equities_js = str(equities)

        html_content = f"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>权益曲线 - {strategy_name}</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background-color: white;
            border-radius: 10px;
            box-shadow: 0 0 10px rgba(0,0,0,0.1);
            padding: 20px;
        }}
        h1 {{
            color: #333;
            text-align: center;
            border-bottom: 2px solid #4CAF50;
            padding-bottom: 10px;
        }}
        .chart-container {{
            height: 500px;
            margin: 30px 0;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>权益曲线 - {strategy_name}</h1>
        <div class="chart-container">
            <canvas id="equityChart"></canvas>
        </div>
    </div>

    <script>
        const ctx = document.getElementById('equityChart').getContext('2d');
        const equityChart = new Chart(ctx, {{
            type: 'line',
            data: {{
                labels: {times_js},
                datasets: [{{
                    label: '账户权益',
                    data: {equities_js},
                    borderColor: 'rgb(75, 192, 192)',
                    backgroundColor: 'rgba(75, 192, 192, 0.2)',
                    borderWidth: 2,
                    fill: true,
                    tension: 0.1
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                scales: {{
                    x: {{
                        display: true,
                        title: {{
                            display: true,
                            text: '时间'
                        }}
                    }},
                    y: {{
                        display: true,
                        title: {{
                            display: true,
                            text: '账户权益 (¥)'
                        }}
                    }}
                }},
                plugins: {{
                    legend: {{
                        display: true,
                        position: 'top',
                    }},
                    tooltip: {{
                        mode: 'index',
                        intersect: false,
                    }}
                }},
                interaction: {{
                    mode: 'nearest',
                    axis: 'x',
                    intersect: false
                }}
            }}
        }});
    </script>
</body>
</html>
"""

        # 保存HTML文件
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(html_content)

        print(f"权益曲线报告已生成: {output_file}")
        return html_content