import os
import sys
import traceback
from datetime import datetime, timedelta
from typing import List, Dict, Any
from agno.agent import Agent
from agno.team import Team
from agno.models.deepseek import DeepSeek
from agno.tools.reasoning import ReasoningTools

# 添加项目路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from agent_tools.qstocktools import QStockTools
from .mcp_instance import mcp
from mcp_app import initialize_dataservice

from dataservices.cache_manager import init_cache_manager

# 配置Redis和数据库参数
redis_host = "200.200.167.104"
redis_port = 6379
db_type = "sqlite"
db_config = {"db_path": "example_cache.db"}

init_cache_manager(
        redis_host=redis_host,
        redis_port=redis_port,
        db_type=db_type,
        db_config=db_config
    )


initialize_dataservice()

# 初始化MCP工具
qstock_tools = QStockTools(enable_all=True)

class OptionAnalysisTeam:
    """
    AI股票期权智能分析团队
    包含多个专业分析师角色，能够自动分析市场信息并给出期权买卖建议和策略
    """
    
    def __init__(self, model_id: str = "gpt-4o", api_key: str = None):
        """
        初始化分析团队
        """
        # 定义模型
        model = DeepSeek(id="glm-4-flash", api_key="101e256afffac0d69df2f19fb81f422c.foAhPS4GSmmsTfhr", base_url="https://open.bigmodel.cn/api/paas/v4/")
        
        # 创建各个分析师角色
        self.macro_economist = Agent(
            name="宏观经济分析师",
            role="你是专业的宏观经济分析师，擅长分析宏观经济指标对金融市场的影响。",
            model=model,
            tools=[
                ReasoningTools(think=True, analyze=True),
                qstock_tools.get_economic_calendar,
                #qstock_tools.get_macro_data,  # 用于获取CPI、PPI、PMI等数据
            ],
            instructions=[
                "分析宏观经济指标对股市和期权市场的影响",
                "关注GDP、CPI、PPI、PMI、货币供应量等指标",
                "评估货币政策对市场流动性的影响",
                "提供宏观经济对期权定价的影响分析"
            ],
            show_tool_calls=True,
            markdown=True,
            monitoring=True,
            debug_mode=True
        )
        
        self.index_analyst = Agent(
            name="指数分析师",
            role="你是专业的指数分析师，专注于分析主要股指的走势和市场情绪。",
            model=model,
            tools=[
                ReasoningTools(think=True, analyze=True),
                qstock_tools.get_index_realtime_data,
                qstock_tools.get_stock_history_data,
                qstock_tools.get_board_trade_realtime_data,
                qstock_tools.get_turnover_history_data,
                qstock_tools.get_usd_index_data,
                qstock_tools.get_ftse_a50_futures_data,
            ],
            instructions=[
                "分析主要股指（如上证指数、深证成指、创业板指等）的走势",
                "评估市场整体情绪和趋势",
                "分析股指期货对现货市场的影响",
                "提供指数波动对期权策略的影响分析"
            ],
            show_tool_calls=True,
            markdown=True,
            debug_mode=True
        )
        
        self.capital_flow_analyst = Agent(
            name="资金流分析师",
            role="你是专业的资金流分析师，专注于分析市场资金流向和融资融券数据。",
            model=model,
            tools=[
                ReasoningTools(think=True, analyze=True),
                qstock_tools.get_rzrq_industry_rank_mcp,
                qstock_tools.get_rzrq_concept_rank_mcp,
                qstock_tools.get_rzrq_account_data_mcp,
                qstock_tools.get_rzrq_market_summary_mcp,
            ],
            instructions=[
                "分析融资融券数据，评估市场杠杆水平",
                "跟踪北向资金流向，分析外资偏好",
                "分析行业和概念板块的资金流入流出情况",
                "提供资金流向对期权市场的影响分析"
            ],
            show_tool_calls=True,
            markdown=True,
            debug_mode=True
        )
        
        self.futures_analyst = Agent(
            name="期货市场分析师",
            role="你是专业的期货市场分析师，专注于分析期货市场对现货市场的影响。",
            model=model,
            tools=[
                ReasoningTools(think=True, analyze=True),
                qstock_tools.get_futures_list,
                qstock_tools.get_futures_market_codes,
                qstock_tools.get_future_org_list,
                qstock_tools.get_qh_jcgc_data,
                qstock_tools.get_qh_ccjg_data,
                qstock_tools.get_qh_lhb_data,
                qstock_tools.get_qh_jcgc_summary,
                qstock_tools.get_qh_ccjg_multi_market,
            ],
            instructions=[
                "分析主要商品期货和金融期货的走势",
                "评估期货市场对相关现货股票的影响",
                "提供期货市场情绪对期权定价的影响分析",
                "关注黑色系、有色、化工、农产品等期货品种"
            ],
            show_tool_calls=True,
            markdown=True,
            debug_mode=True
        )
        
        self.option_market_analyst = Agent(
            name="期权市场分析师",
            role="你是专业的期权市场分析师，专注于分析期权市场的整体状况。",
            model=model,
            tools=[
                ReasoningTools(think=True, analyze=True),
                qstock_tools.get_option_target_list,
                qstock_tools.get_option_realtime_data,
                qstock_tools.get_option_risk_data,
                qstock_tools.get_option_tboard_data,
                qstock_tools.get_option_value_data,
                qstock_tools.get_option_expire_all_data,
                qstock_tools.get_option_tboard_data,
            ],
            instructions=[
                "分析期权市场的整体交易情况",
                "评估期权市场的活跃度和参与者情绪",
                "提供期权市场隐含波动率分析",
                "分析期权成交量和持仓量分布"
            ],
            show_tool_calls=True,
            markdown=True,
            debug_mode=True
        )
        
        self.option_volatility_analyst = Agent(
            name="期权波动率分析师",
            role="你是专业的期权波动率分析师，专注于分析期权波动率指标。",
            model=model,
            tools=[
                ReasoningTools(think=True, analyze=True),
                qstock_tools.get_50etf_qvix,
                qstock_tools.get_50etf_min_qvix,
                qstock_tools.get_300etf_qvix,
                qstock_tools.get_300etf_min_qvix,
                qstock_tools.get_100etf_qvix,
                qstock_tools.get_100etf_min_qvix,
                qstock_tools.get_500etf_qvix,
                qstock_tools.get_500etf_min_qvix,
                qstock_tools.get_kcb_qvix,
                qstock_tools.get_kcb_min_qvix,
                qstock_tools.get_cyb_qvix,
                qstock_tools.get_cyb_min_qvix,
                qstock_tools.get_1000index_qvix,
                qstock_tools.get_1000index_min_qvix,
                qstock_tools.get_50index_qvix,
                qstock_tools.get_50index_min_qvix,
            ],
            instructions=[
                "分析期权波动率指数(QVIX)走势",
                "评估市场对未来波动率的预期",
                "提供波动率交易策略建议",
                "分析波动率锥和波动率曲面"
            ],
            show_tool_calls=True,
            markdown=True,
            debug_mode=True
        )
        
        self.stock_volatility_theorist = Agent(
            name="股票波动理论分析师",
            role="你是专业的股票波动理论分析师，专注于分析股票价格波动的理论模型。",
            model=model,
            tools=[
                ReasoningTools(think=True, analyze=True),
                qstock_tools.get_stock_history_data,
            ],
            instructions=[
                "运用波动理论分析股票价格行为",
                "结合技术指标和基本面分析",
                "提供股票波动特征分析",
                "评估股票波动对期权定价的影响"
            ],
            show_tool_calls=True,
            markdown=True,
            debug_mode=True
        )
        
        self.technical_analyst = Agent(
            name="技术分析师",
            role="你是专业的技术分析师，擅长使用各种技术指标分析市场走势。",
            model=model,
            tools=[
                ReasoningTools(think=True, analyze=True),
                qstock_tools.get_stock_history_data,
                qstock_tools.get_index_realtime_data,
            ],
            instructions=[
                "使用技术指标分析股票和指数走势",
                "识别关键支撑位和阻力位",
                "提供趋势分析和买卖信号",
                "结合多种技术指标进行综合判断"
            ],
            show_tool_calls=True,
            markdown=True,
            debug_mode=True
        )
        
        self.cl_analyst = Agent(
            name="股票缠论分析师",
            role="你是专业的缠论分析师，擅长使用缠论理论分析市场走势。",
            model=model,
            tools=[
                ReasoningTools(think=True, analyze=True),
                qstock_tools.get_stock_history_data,
                qstock_tools.get_index_realtime_data,
            ],
            instructions=[
"""
你需完全代入 “专业缠论分析师” 角色，具备 10 年以上市场（股票 / 期货 / 外汇等）缠论实战分析经验，精通缠论核心体系，能精准运用理论拆解市场走势并输出可落地的分析结论。具体执行要求如下：
一、理论应用准则：严格以缠论核心框架为分析基础，所有结论需对应缠论明确概念，包括但不限于：
精准识别并标注 K 线图中的 “笔、线段、中枢”，明确中枢级别（如 1 分钟、5 分钟、30 分钟）及延伸 / 扩展状态；
基于 “走势终完美” 原则，判断当前走势（上涨 / 下跌 / 盘整）的完成度，分析是否存在背驰（包括顶背驰、底背驰，需区分 MACD 辅助判断与走势结构背驰的关系）；
结合 “三类买卖点” 规则，定位当前市场可能的交易信号，说明买点 / 卖点对应的走势结构逻辑（如第一买点对应下跌趋势背驰，第三买点对应中枢上移后的回抽确认）。
二、分析输出规范：接收用户提供的 “市场品种 + 时间周期 + K 线走势描述（或关键价格区间）” 后，按以下结构输出：
第一步：走势结构拆解（明确当前走势级别、已形成的笔 / 线段数量、中枢位置及区间）；
第二步：趋势与背驰判断（说明当前趋势方向，是否出现背驰信号，判断依据是什么）；
第三步：买卖点定位（指出当前是否存在缠论买卖点，若存在，说明是哪类买卖点及操作逻辑；若不存在，说明需等待的关键走势信号）；
第四步：后续走势推演（基于 “走势终完美”，给出 2-3 种可能的后续走势演化路径，标注每种路径的关键确认信号）。
三、沟通与补充要求：
若用户提供的走势信息不完整（如未明确时间周期、关键价格节点缺失），需先向用户追问核心信息，再开展分析，避免主观臆断；
分析过程中需规避 “预测涨跌” 的绝对化表述，所有结论需附加 “走势结构确认条件”（如 “若后续跌破 XX 价位，当前中枢将升级为 XX 级别”）；
对用户提出的缠论概念疑问（如 “如何区分笔与线段”“中枢扩展与延伸的区别”），需用 “理论定义 + 实例对应” 的方式解答，确保用户理解理论与走势的结合逻辑。
"""
            ],
            show_tool_calls=True,
            markdown=True,
            debug_mode=True
        )
        
        self.option_strategist = Agent(
            name="期权策略分析师",
            role="你是专业的期权策略分析师，擅长设计和评估各种期权策略。",
            model=model,
            tools=[
                ReasoningTools(think=True, analyze=True),
                qstock_tools.get_option_target_list,
                qstock_tools.get_option_realtime_data,
            ],
            instructions=[
                "根据市场分析结果设计期权策略",
                "评估不同策略的风险收益特征",
                "提供策略执行建议和风险控制措施",
                "结合波动率分析优化策略选择"
            ],
            show_tool_calls=True,
            markdown=True,
            debug_mode=True
        )
        
        self.hft_expert = Agent(
            name="期权高频交易高手",
            role="你是专业的期权高频交易专家，擅长捕捉短期市场机会。",
            model=model,
            tools=[
                ReasoningTools(think=True, analyze=True),
                qstock_tools.get_option_realtime_data,
            ],
            instructions=[
                "分析期权市场的短期价格行为",
                "识别高频交易机会",
                "提供套利策略建议",
                "评估交易执行成本和滑点风险"
            ],
            show_tool_calls=True,
            markdown=True,
            debug_mode=True
        )
        
        # 创建团队
        self.team = Team(
            name="AI股票期权智能分析团队",
            mode="coordinate",
            members=[
                self.macro_economist,
                self.index_analyst,
                self.capital_flow_analyst,
                self.futures_analyst,
                self.option_market_analyst,
                self.option_volatility_analyst,
                self.stock_volatility_theorist,
                self.technical_analyst,
                self.cl_analyst,
                self.option_strategist,
                self.hft_expert
            ],
            model=model,
            instructions=[
                "定期分析市场信息，提供最优收益(风险最小收益最高)的期权买卖标的和策略",
                "综合各专家意见，形成最终的投资建议",
                "重点关注期权市场的风险控制"
            ],
            show_tool_calls=True,
            show_members_responses=True,
            markdown=True,
            debug_mode=True
        )
    
    def run_daily_analysis(self) -> Dict[str, Any]:
        """
        执行每日市场分析
        """
        print("开始执行每日市场分析...")
        
        # 团队协作分析
        analysis_result = self.team.run(
            "请各位专家分析当前市场情况，重点关注以下方面：\n"
            "1. 宏观经济环境对股市和期权市场的影响\n"
            "2. 主要股指的走势和市场情绪\n"
            "3. 资金流向和市场参与者行为\n"
            "4. 期货市场对现货市场的影响\n"
            "5. 期权市场的整体状况和活跃度\n"
            "6. 波动率水平和预期\n"
            "7. 基于技术分析和缠论的市场走势判断\n"
            "8. 综合以上分析，推荐最优收益(风险最小收益最高)的期权买卖标的和策略\n\n"
            "请各位专家分别从自己的专业角度进行分析，最后由团队整合形成最终建议。"
        )
        
        return {
            "timestamp": datetime.now().isoformat(),
            "analysis_result": analysis_result
        }
    
    def run_periodic_analysis(self, interval_minutes: int = 30) -> None:
        """
        定时执行市场分析
        """
        import time
        print(f"开始定时分析，每{interval_minutes}分钟执行一次...")
        
        while True:
            try:
                result = self.run_daily_analysis()
                print(f"分析完成，结果: {result}")
            except Exception as e:
                print(f"分析过程中发生错误: {e}")
            
            # 等待指定时间间隔
            time.sleep(interval_minutes * 60)

# 使用示例
if __name__ == "__main__":
    try:
        # 初始化团队
        team = OptionAnalysisTeam()
        
        # 执行一次分析
        result = team.run_daily_analysis()
        print(result)
    except Exception as e:
        traceback.print_exc()
        print(f"发生错误: {e}")