#!/usr/bin/env python
# -*- encoding=utf8 -*-

"""
简化版HTTP客户端示例

此示例演示如何通过简单的HTTP请求调用MCP工具。
"""

import requests
import json


def call_mcp_tool(base_url: str, tool_name: str, arguments: dict):
    """
    通过HTTP调用MCP工具
    
    Args:
        base_url: MCP服务器基础URL
        tool_name: 工具名称
        arguments: 工具参数
        
    Returns:
        dict: 工具调用结果
    """
    # 构造请求URL
    url = f"{base_url.rstrip('/')}/mcp/message"
    
    # 构造JSON-RPC请求
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": tool_name,
            "arguments": arguments
        }
    }
    
    # 发送POST请求
    headers = {
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()  # 检查HTTP错误
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"请求失败: {e}")
        return None


def list_mcp_tools(base_url: str):
    """
    获取MCP工具列表
    
    Args:
        base_url: MCP服务器基础URL
        
    Returns:
        dict: 工具列表
    """
    # 构造请求URL
    url = f"{base_url.rstrip('/')}/mcp/message"
    
    # 构造JSON-RPC请求
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/list",
        "params": {}
    }
    
    # 发送POST请求
    headers = {
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()  # 检查HTTP错误
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"请求失败: {e}")
        return None


def main():
    """主函数"""
    # MCP服务器地址
    base_url = "http://127.0.0.1:8000"
    
    print("=== MCP工具调用示例 ===")
    
    # 1. 获取工具列表
    print("\n1. 获取工具列表...")
    tools_response = list_mcp_tools(base_url)
    if tools_response and 'result' in tools_response:
        print("可用工具:")
        for tool in tools_response['result']:
            print(f"  - {tool.get('name', 'Unknown')}: {tool.get('description', 'No description')}")
    else:
        print("获取工具列表失败")
        return
    
    # 2. 调用获取指数实时数据的工具
    print("\n2. 调用工具: get_index_realtime_data...")
    result = call_mcp_tool(
        base_url,
        "get_index_realtime_data",
        {
            "codes": ["000001"],
            "market": "沪深A"
        }
    )
    
    if result:
        print("调用结果:")
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print("工具调用失败")
    
    # 3. 调用获取董事会实时交易数据的工具
    print("\n3. 调用工具: get_board_trade_realtime_data...")
    result = call_mcp_tool(
        base_url,
        "get_board_trade_realtime_data",
        {}
    )
    
    if result:
        print("调用结果:")
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print("工具调用失败")


if __name__ == "__main__":
    main()