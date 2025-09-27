#!/usr/bin/env python
# -*- encoding=utf8 -*-

"""
SSE/Streamable HTTP调用MCP工具示例

此示例演示如何通过SSE或Streamable HTTP协议连接到MCP服务器并调用工具。
"""

import asyncio
import json
import aiohttp
import sys
from typing import Dict, Any, Optional


class MCPClient:
    """MCP客户端，支持SSE和Streamable HTTP协议"""
    
    def __init__(self, base_url: str, transport: str = "sse"):
        """
        初始化MCP客户端
        
        Args:
            base_url: MCP服务器基础URL，例如 "http://127.0.0.1:8000"
            transport: 传输协议，"sse" 或 "streamable-http"
        """
        self.base_url = base_url.rstrip('/')
        self.transport = transport
        self.session_id = None
        self.session = None
        
    async def __aenter__(self):
        """异步上下文管理器入口"""
        self.session = aiohttp.ClientSession()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """异步上下文管理器出口"""
        if self.session:
            await self.session.close()
            
    async def connect(self) -> bool:
        """
        连接到MCP服务器
        
        Returns:
            bool: 连接是否成功
        """
        if self.transport == "sse":
            return await self._connect_sse()
        elif self.transport == "streamable-http":
            return await self._connect_streamable_http()
        else:
            raise ValueError(f"不支持的传输协议: {self.transport}")
            
    async def _connect_sse(self) -> bool:
        """
        通过SSE协议连接到MCP服务器
        
        Returns:
            bool: 连接是否成功
        """
        try:
            # SSE连接到/mcp端点
            url = f"{self.base_url}/mcp"
            print(f"正在连接到SSE端点: {url}")
            
            # 发送初始化请求获取会话信息
            async with self.session.get(url) as response:
                if response.status == 200:
                    # 读取SSE流中的第一条消息
                    async for line in response.content:
                        line = line.decode('utf-8').strip()
                        if line.startswith('data:'):
                            data = line[5:].strip()
                            try:
                                message = json.loads(data)
                                if 'session_id' in message:
                                    self.session_id = message['session_id']
                                    print(f"连接成功，会话ID: {self.session_id}")
                                    return True
                            except json.JSONDecodeError:
                                continue
                else:
                    print(f"连接失败，状态码: {response.status}")
                    return False
        except Exception as e:
            print(f"SSE连接异常: {e}")
            return False
            
        return False
        
    async def _connect_streamable_http(self) -> bool:
        """
        通过Streamable HTTP协议连接到MCP服务器
        
        Returns:
            bool: 连接是否成功
        """
        try:
            # Streamable HTTP连接到/mcp/message端点
            url = f"{self.base_url}/mcp/message"
            print(f"正在连接到Streamable HTTP端点: {url}")
            
            # 发送初始化请求
            init_message = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "capabilities": {}
                }
            }
            
            async with self.session.post(
                url, 
                json=init_message,
                headers={"Content-Type": "application/json"}
            ) as response:
                if response.status == 200:
                    result = await response.json()
                    if 'result' in result and 'session_id' in result['result']:
                        self.session_id = result['result']['session_id']
                        print(f"连接成功，会话ID: {self.session_id}")
                        return True
                    else:
                        print(f"初始化响应不包含会话ID: {result}")
                else:
                    print(f"连接失败，状态码: {response.status}")
                    text = await response.text()
                    print(f"响应内容: {text}")
        except Exception as e:
            print(f"Streamable HTTP连接异常: {e}")
            
        return False
        
    async def list_tools(self) -> Optional[Dict[Any, Any]]:
        """
        获取可用工具列表
        
        Returns:
            Dict: 工具列表或None（如果失败）
        """
        try:
            message = {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/list",
                "params": {}
            }
            
            return await self._send_message(message)
        except Exception as e:
            print(f"获取工具列表失败: {e}")
            return None
            
    async def call_tool(self, name: str, arguments: Dict) -> Optional[Dict[Any, Any]]:
        """
        调用指定的工具
        
        Args:
            name: 工具名称
            arguments: 工具参数
            
        Returns:
            Dict: 工具调用结果或None（如果失败）
        """
        try:
            message = {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {
                    "name": name,
                    "arguments": arguments
                }
            }
            
            return await self._send_message(message)
        except Exception as e:
            print(f"调用工具 {name} 失败: {e}")
            return None
            
    async def _send_message(self, message: Dict) -> Optional[Dict[Any, Any]]:
        """
        发送消息到MCP服务器
        
        Args:
            message: 要发送的JSON-RPC消息
            
        Returns:
            Dict: 服务器响应或None（如果失败）
        """
        if self.transport == "sse":
            return await self._send_message_sse(message)
        elif self.transport == "streamable-http":
            return await self._send_message_streamable_http(message)
        else:
            raise ValueError(f"不支持的传输协议: {self.transport}")
            
    async def _send_message_sse(self, message: Dict) -> Optional[Dict[Any, Any]]:
        """
        通过SSE协议发送消息
        
        Args:
            message: 要发送的JSON-RPC消息
            
        Returns:
            Dict: 服务器响应或None（如果失败）
        """
        try:
            url = f"{self.base_url}/mcp/message"
            async with self.session.post(
                url,
                json=message,
                headers={"Content-Type": "application/json"}
            ) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    print(f"发送消息失败，状态码: {response.status}")
                    return None
        except Exception as e:
            print(f"发送SSE消息异常: {e}")
            return None
            
    async def _send_message_streamable_http(self, message: Dict) -> Optional[Dict[Any, Any]]:
        """
        通过Streamable HTTP协议发送消息
        
        Args:
            message: 要发送的JSON-RPC消息
            
        Returns:
            Dict: 服务器响应或None（如果失败）
        """
        try:
            url = f"{self.base_url}/mcp/message"
            async with self.session.post(
                url,
                json=message,
                headers={"Content-Type": "application/json"}
            ) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    print(f"发送消息失败，状态码: {response.status}")
                    return None
        except Exception as e:
            print(f"发送Streamable HTTP消息异常: {e}")
            return None


async def example_sse_usage():
    """SSE使用示例"""
    print("=== SSE协议使用示例 ===")
    
    async with MCPClient("http://127.0.0.1:8000", "sse") as client:
        # 连接到服务器
        if not await client.connect():
            print("连接失败")
            return
            
        # 获取工具列表
        print("\n1. 获取工具列表...")
        tools = await client.list_tools()
        if tools:
            print("可用工具:")
            for tool in tools.get('result', []):
                print(f"  - {tool.get('name', 'Unknown')}: {tool.get('description', 'No description')}")
        else:
            print("获取工具列表失败")
            
        # 调用获取指数实时数据的工具
        print("\n2. 调用工具: get_index_realtime_data...")
        result = await client.call_tool(
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


async def example_streamable_http_usage():
    """Streamable HTTP使用示例"""
    print("=== Streamable HTTP协议使用示例 ===")
    
    async with MCPClient("http://127.0.0.1:8000", "streamable-http") as client:
        # 连接到服务器
        if not await client.connect():
            print("连接失败")
            return
            
        # 获取工具列表
        print("\n1. 获取工具列表...")
        tools = await client.list_tools()
        if tools:
            print("可用工具:")
            for tool in tools.get('result', []):
                print(f"  - {tool.get('name', 'Unknown')}: {tool.get('description', 'No description')}")
        else:
            print("获取工具列表失败")
            
        # 调用获取指数实时数据的工具
        print("\n2. 调用工具: get_index_realtime_data...")
        result = await client.call_tool(
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


def print_usage():
    """打印使用说明"""
    print("MCP工具调用示例")
    print("用法:")
    print("  python example_sse_client.py sse          # 使用SSE协议")
    print("  python example_sse_client.py streamable   # 使用Streamable HTTP协议")
    print("  python example_sse_client.py              # 显示此帮助信息")


async def main():
    """主函数"""
    if len(sys.argv) < 2:
        print_usage()
        return
        
    protocol = sys.argv[1].lower()
    
    if protocol == "sse":
        await example_sse_usage()
    elif protocol == "streamable":
        await example_streamable_http_usage()
    else:
        print_usage()


if __name__ == "__main__":
    # 运行示例
    asyncio.run(main())