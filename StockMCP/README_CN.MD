# MCP - 金融数据智能分析系统

MCP是一个基于金融数据分析的项目，用于股票、期权、期货等金融产品的分析与决策支持。

## 功能模块

### 核心数据服务
- 股票数据服务
- 期权数据服务
- 期货数据服务
- 融资融券数据服务
- 宏观经济数据服务

### 分析模块
- 技术指标分析
- 宏观经济与行业分析
- 资金流向分析
- 期权波动率分析
- 期货市场分析

### AI智能分析团队 (新增)
基于Agno库构建的AI股票期权智能分析团队，包含以下专业角色：
- 宏观经济分析师
- 指数分析师
- 资金流分析师
- 期货市场分析师
- 期权市场分析师
- 期权波动率分析师
- 股票波动理论分析师
- 股票缠论分析师
- 技术分析师
- 期权策略分析师
- 期权高频交易高手

该团队能够定时自动分析市场信息，并给出最优收益(风险最小收益最高)的期权买卖标的和策略。

## 安装依赖

```bash
pip install -r requirements.txt
conda install -c conda-forge ta
```

## 使用方法

### 启动MCP服务

```bash
python mcp_app.py
```

### 运行AI智能分析团队

```bash
# 一次性分析
python option_analysis_team.py

# 定时分析
python schedule_option_analysis.py
```

## 目录结构

```
.
├── agent/                 # Agno代理实现
├── agent_tools/           # 代理工具
├── dataservices/          # 数据服务模块
├── tests/                 # 测试文件
├── option_analysis_team.py # AI股票期权智能分析团队 (新增)
├── schedule_option_analysis.py # 定时分析脚本 (新增)
└── ...                    # 其他模块文件
```

## 技术栈

- Python 3.x
- Agno (AI Agent框架)
- ta (技术指标库)
- qstocktools (第三方金融数据接口)
- Redis (缓存)
- SQLite (本地数据库)

## 配置说明

系统使用Redis作为缓存，SQLite作为本地数据库存储。可通过命令行参数配置连接信息。

## 贡献

欢迎提交Issue和Pull Request来改进本项目。