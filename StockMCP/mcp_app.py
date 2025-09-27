#!/usr/bin/env python
# -*- encoding=utf8 -*-

import sys
import json
import os
import logging
import traceback
import argparse
import qstock as qs
import replace_qstock_func
from .mcp_instance import mcp
import mcp_index as index
import mcp_qh as qh
import mcp_option_qvix as qvix
import mcp_macro as macro
import mcp_rzrq as rzrq
import mcp_news as news
import mcp_industry as industry
import mcp_money as money
import mcp_fundamental as fundamental
import mcp_wencai as wencai
import mcp_stock_pool as stock_pool
import mcp_trade as trade
import mcp_option as option

logger = logging.getLogger('mcp-stock')

# Fix UTF-8 encoding for Windows console
if sys.platform == 'win32':
    sys.stderr.reconfigure(encoding='utf-8')
    sys.stdout.reconfigure(encoding='utf-8')

# Add an addition tool

def initialize_dataservice():
    """ 初始化数据服务
    参数：
    - 无
    返回：
    - None
    """
    index.intitialize_data_service()
    rzrq.initialize_data_service()
    qh.initialize_data_service()
    option.initialize_data_service()
    qvix.initialize_data_service()

# Start the server
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start the MCP server")
    parser.add_argument('-t', '--transport', type=str, default='stdio', help='Transport type for the MCP server (default: stdio)')
    parser.add_argument('-p', '--port', type=int, default=8000, help='Port number for the MCP server (default: 8000)')
    parser.add_argument('-H', '--host', type=str, default='127.0.0.1', help='Host to listen on when using sse or http transport (default: 127.0.0.1)')
    parser.add_argument('--redis_host', type=str, default='200.200.167.104', help='Redis host (default: 200.200.167.104)') # Redis host (default: 200.200.167.104)
    parser.add_argument('--redis_port', type=int, default=6379, help='Redis port (default: 6379)')
    parser.add_argument('--db_type', type=str, default='sqlite', help='Database type (default: sqlite)')
    parser.add_argument('--db_config', type=str, default='{"db_path": "example_cache.db"}', help='Database configuration (default: {"db_path": "example_cache.db"})')
    parser.add_argument('--enable_redis', action='store_true', help='Enable Redis cache (default: disabled)')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose logging')
    args = parser.parse_args()

    transport = args.transport
    port = args.port
    host = args.host

    try:
        if args.db_config:
            # 如果db_config参数是一个文件路径，则加载文件
            if os.path.exists(args.db_config):
                with open(args.db_config, 'r') as f:
                    args.db_config = json.load(f)
            else:
                # 如果是字符串，则尝试解析为JSON
                args.db_config = json.loads(args.db_config)
    except Exception as e:
        logger.error("Invalid database configuration: %s", str(e))
        sys.exit(1)

    # 初始化缓存管理器，可以配置Redis和数据库参数
    if args.enable_redis:
        from dataservices.cache_manager import init_cache_manager
        logger.info("Initializing cache manager with Redis %s:%s and database configuration %s...", args.redis_host, args.redis_port, args.db_config)
        if args.verbose:
            logger.info("Database configuration: %s", args.db_config)

        init_cache_manager(
            redis_host=args.redis_host,
            redis_port=args.redis_port,
            db_type=args.db_type,
            db_config=args.db_config
        )
        from dataservices.cache_manager import clear_all_cache
        clear_all_cache()
    else:
        logger.info("Redis cache is disabled. Skipping cache manager initialization.")

    # 初始化所有数据服务
    logger.info("Initializing data service...")
    initialize_dataservice()

    logger.info("Starting MCP server as a %s server...", transport)
    
    # 根据传输方式决定是否需要设置host和port
    if transport in ['sse', 'streamable-http']:
        logger.info(f"Server will listen on {host}:{port}")
        # 在FastMCP实例上设置host和port属性
        mcp.settings.host = host
        mcp.settings.port = port
    
    # 运行MCP服务器，不在run方法中传递host和port参数
    if transport == "sse":
        mcp.run(transport=transport, mount_path="/")
    elif transport == "streamable-http":
        mcp.run(transport=transport, mount_path="/mcp")
    else:
        mcp.run(transport=transport)
    
    logger.info("MCP server started successfully.")
    logger.info("Press Ctrl+C to stop the server.")