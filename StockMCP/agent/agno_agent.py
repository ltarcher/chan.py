from agno.agent import Agent
from agno.models.deepseek import DeepSeek
from agno.team import Team
from agno.tools.reasoning import ReasoningTools
from agno.tools.mcp import MCPTools

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from agent_tools.qstocktools import QStockTools


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

def load_system_prompt(file_path):
    """加载系统提示词"""
    with open(file_path, 'r', encoding='utf-8') as file:
        return file.read().strip()

# 初始化MCP工具
qstock_tools = QStockTools(enable_all=True)

mcptools = MCPTools(url="http://127.0.0.1:8000", transport='sse')

tech_agent = Agent(
    name="基础技术分析Agent",
    role=load_system_prompt(os.path.join(os.path.dirname(os.path.abspath(__file__)), "tech_agent.txt")),
    model=DeepSeek(id="glm-4-flash", api_key="101e256afffac0d69df2f19fb81f422c.foAhPS4GSmmsTfhr", base_url="https://open.bigmodel.cn/api/paas/v4/"),
    tools=[
        ReasoningTools(think=True, analyze=True, add_instructions=True),
        qstock_tools.get_china_latest_trade_day,
        qstock_tools.get_stock_history_data,
        qstock_tools.get_index_realtime_data,
    ],
    instructions=[
        "先通过`get_china_latest_trade_day`工具获取最新交易日。",
        "请从用户提问中获取股票名称或股票代码，如`深信服`。",
        "然后通过`get_stock_history_data`工具获取股票或指数的历史数据，请分别获取1分钟、5分钟、15分钟、30分钟、60分钟、日线、周线、月线数据，并返回。",
        "最后通过`get_index_realtime_data`工具获取股票或指数的实时数据。",
        "提示：请使用工具调用的方式调用工具，并使用工具调用的返回值作为工具调用的参数。"
    ],
    add_datetime_to_instructions=True,
    show_tool_calls=True,
    markdown=True,
    monitoring=True,
    debug_mode=True
)

#tech_agent.print_response("请对深信服(股票代码300454)进行全面技术分析", stream=True)


cl_agent = Agent(
    name="缠论技术分析Agent",
    role="你是一位专业的缠论分析师，擅长使用缠论理论分析市场走势。",
    system_message=load_system_prompt(os.path.join(os.path.dirname(os.path.abspath(__file__)), "cl_agent.txt")),
    model=DeepSeek(id="glm-4-flash", api_key="101e256afffac0d69df2f19fb81f422c.foAhPS4GSmmsTfhr", base_url="https://open.bigmodel.cn/api/paas/v4/"),
    tools=[
        ReasoningTools(think=True, analyze=True, add_instructions=True),
        qstock_tools.get_china_latest_trade_day,
        qstock_tools.get_stock_history_data,
        qstock_tools.get_index_realtime_data,
    ],
    instructions=[
        "先通过`get_china_latest_trade_day`工具获取最新交易日。",
        "请从用户提问中获取股票名称或股票代码，如`深信服`。",
        "然后通过`get_stock_history_data`工具获取股票或指数的历史数据，请分别获取1分钟、5分钟、15分钟、30分钟、60分钟、日线、周线、月线数据，并展示获取到的数据。",
        "最后通过`get_index_realtime_data`工具获取股票或指数的实时数据。",
        "提示：请使用工具调用的方式调用工具。"
    ],
    add_datetime_to_instructions=True,
    show_tool_calls=True,
    markdown=True,
    monitoring=True,
    debug_mode=True
)

agent_team = Team(
    mode="coordinate",
    members=[tech_agent, cl_agent],
    model=DeepSeek(id="glm-4-flash", api_key="101e256afffac0d69df2f19fb81f422c.foAhPS4GSmmsTfhr", base_url="https://open.bigmodel.cn/api/paas/v4/"),
    success_criteria="结合基础技术分析和缠论技术分析结果，并给出买入、卖出、观望建议。",
    instructions=[
        "基础技术分析交给基础技术分析agent。", 
        "缠论技术分析交给缠论技术分析agent。",
        "最后结合基础技术分析和缠论技术分析结果，并给出买入、卖出、观望建议。"
    ],
    add_datetime_to_instructions=True,
    show_tool_calls=True,
    show_members_responses=True,
    markdown=True,
    monitoring=True,
    debug_mode=True
)

agent_team.print_response("请对深信服(股票代码：300454)进行全面的技术分析", 
                          stream=True, show_full_reasoning=True, stream_intermediate_steps=True)

