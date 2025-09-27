import traceback
import logging
from .mcp_instance import mcp
from dataservices.option_qvix_data import OptionQvixDataService

# 配置日志
logger = logging.getLogger("mcp_option_qvix")

# 创建期权QVIX数据服务实例
data_service = None

def initialize_data_service():
    """ 初始化数据服务 """
    global data_service
    if data_service is None:
        logger.info("Initializing OptionQvixDataService...")
        data_service = OptionQvixDataService()
    return data_service

@mcp.tool()
async def get_50etf_qvix() -> list:
    """ 获取50ETF期权波动率指数QVIX日线数据
    参数：
    - 无
    返回：
    - List[Dict]: 包含50ETF期权波动率指数QVIX日线数据，
    Dict包含字段: ['date', 'open', 'high', 'low', 'close']
    """
    try:
        return data_service.get_50etf_qvix()
    except Exception as e:
        logger.error(f"获取50ETF期权波动率指数QVIX日线数据失败: {str(e)}")
        traceback.print_exc()
        return []

@mcp.tool()
async def get_50etf_min_qvix() -> list:
    """ 获取50ETF期权波动率指数QVIX分钟线数据
    参数：
    - 无
    返回：
    - List[Dict]: 包含50ETF期权波动率指数QVIX分钟线数据，
    Dict包含字段: ['time', 'qvix']
    """
    try:
        return data_service.get_50etf_min_qvix()
    except Exception as e:
        logger.error(f"获取50ETF期权波动率指数QVIX分钟线数据失败: {str(e)}")
        traceback.print_exc()
        return []

@mcp.tool()
async def get_300etf_qvix() -> list:
    """ 获取300ETF期权波动率指数QVIX日线数据
    参数：
    - 无
    返回：
    - List[Dict]: 包含300ETF期权波动率指数QVIX日线数据，
    Dict包含字段: ['date', 'open', 'high', 'low', 'close']
    """
    try:
        return data_service.get_300etf_qvix()
    except Exception as e:
        logger.error(f"获取300ETF期权波动率指数QVIX日线数据失败: {str(e)}")
        traceback.print_exc()
        return []

@mcp.tool()
async def get_300etf_min_qvix() -> list:
    """ 获取300ETF期权波动率指数QVIX分钟线数据
    参数：
    - 无
    返回：
    - List[Dict]: 包含300ETF期权波动率指数QVIX分钟线数据，
    Dict包含字段: ['time', 'qvix']
    """
    try:
        return data_service.get_300etf_min_qvix()
    except Exception as e:
        logger.error(f"获取300ETF期权波动率指数QVIX分钟线数据失败: {str(e)}")
        traceback.print_exc()
        return []

@mcp.tool()
async def get_500etf_qvix() -> list:
    """ 获取500ETF期权波动率指数QVIX日线数据
    参数：
    - 无
    返回：
    - List[Dict]: 包含500ETF期权波动率指数QVIX日线数据，
    Dict包含字段: ['date', 'open', 'high', 'low', 'close']
    """
    try:
        return data_service.get_500etf_qvix()
    except Exception as e:
        logger.error(f"获取500ETF期权波动率指数QVIX日线数据失败: {str(e)}")
        traceback.print_exc()
        return []

@mcp.tool()
async def get_500etf_min_qvix() -> list:
    """ 获取500ETF期权波动率指数QVIX分钟线数据
    参数：
    - 无
    返回：
    - List[Dict]: 包含500ETF期权波动率指数QVIX分钟线数据，
    Dict包含字段: ['time', 'qvix']
    """
    try:
        return data_service.get_500etf_min_qvix()
    except Exception as e:
        logger.error(f"获取500ETF期权波动率指数QVIX分钟线数据失败: {str(e)}")
        traceback.print_exc()
        return []

@mcp.tool()
async def get_cyb_qvix() -> list:
    """ 获取创业板期权波动率指数QVIX日线数据
    参数：
    - 无
    返回：
    - List[Dict]: 包含创业板期权波动率指数QVIX日线数据，
    Dict包含字段: ['date', 'open', 'high', 'low', 'close']
    """
    try:
        return data_service.get_cyb_qvix()
    except Exception as e:
        logger.error(f"获取创业板期权波动率指数QVIX日线数据失败: {str(e)}")
        traceback.print_exc()
        return []

@mcp.tool()
async def get_cyb_min_qvix() -> list:
    """ 获取创业板期权波动率指数QVIX分钟线数据
    参数：
    - 无
    返回：
    - List[Dict]: 包含创业板期权波动率指数QVIX分钟线数据，
    Dict包含字段: ['time', 'qvix']
    """
    try:
        return data_service.get_cyb_min_qvix()
    except Exception as e:
        logger.error(f"获取创业板期权波动率指数QVIX分钟线数据失败: {str(e)}")
        traceback.print_exc()
        return []

@mcp.tool()
async def get_kcb_qvix() -> list:
    """ 获取科创板期权波动率指数QVIX日线数据
    参数：
    - 无
    返回：
    - List[Dict]: 包含科创板期权波动率指数QVIX日线数据，
    Dict包含字段: ['date', 'open', 'high', 'low', 'close']
    """
    try:
        return data_service.get_kcb_qvix()
    except Exception as e:
        logger.error(f"获取科创板期权波动率指数QVIX日线数据失败: {str(e)}")
        traceback.print_exc()
        return []

@mcp.tool()
async def get_kcb_min_qvix() -> list:
    """ 获取科创板期权波动率指数QVIX分钟线数据
    参数：
    - 无
    返回：
    - List[Dict]: 包含科创板期权波动率指数QVIX分钟线数据，
    Dict包含字段: ['time', 'qvix']
    """
    try:
        return data_service.get_kcb_min_qvix()
    except Exception as e:
        logger.error(f"获取科创板期权波动率指数QVIX分钟线数据失败: {str(e)}")
        traceback.print_exc()
        return []

@mcp.tool()
async def get_100etf_qvix() -> list:
    """ 获取深证100ETF期权波动率指数QVIX日线数据
    参数：
    - 无
    返回：
    - List[Dict]: 包含深证100ETF期权波动率指数QVIX日线数据，
    Dict包含字段: ['date', 'open', 'high', 'low', 'close']
    """
    try:
        return data_service.get_100etf_qvix()
    except Exception as e:
        logger.error(f"获取深证100ETF期权波动率指数QVIX日线数据失败: {str(e)}")
        traceback.print_exc()
        return []

@mcp.tool()
async def get_100etf_min_qvix() -> list:
    """ 获取深证100ETF期权波动率指数QVIX分钟线数据
    参数：
    - 无
    返回：
    - List[Dict]: 包含深证100ETF期权波动率指数QVIX分钟线数据，
    Dict包含字段: ['time', 'qvix']
    """
    try:
        return data_service.get_100etf_min_qvix()
    except Exception as e:
        logger.error(f"获取深证100ETF期权波动率指数QVIX分钟线数据失败: {str(e)}")
        traceback.print_exc()
        return []

@mcp.tool()
async def get_300index_qvix() -> list:
    """ 获取中证300股指期权波动率指数QVIX日线数据
    参数：
    - 无
    返回：
    - List[Dict]: 包含中证300股指期权波动率指数QVIX日线数据，
    Dict包含字段: ['date', 'open', 'high', 'low', 'close']
    """
    try:
        return data_service.get_300index_qvix()
    except Exception as e:
        logger.error(f"获取中证300股指期权波动率指数QVIX日线数据失败: {str(e)}")
        traceback.print_exc()
        return []

@mcp.tool()
async def get_300index_min_qvix() -> list:
    """ 获取中证300股指期权波动率指数QVIX分钟线数据
    参数：
    - 无
    返回：
    - List[Dict]: 包含中证300股指期权波动率指数QVIX分钟线数据，
    Dict包含字段: ['time', 'qvix']
    """
    try:
        return data_service.get_300index_min_qvix()
    except Exception as e:
        logger.error(f"获取中证300股指期权波动率指数QVIX分钟线数据失败: {str(e)}")
        traceback.print_exc()
        return []

@mcp.tool()
async def get_1000index_qvix() -> list:
    """ 获取中证1000股指期权波动率指数QVIX日线数据
    参数：
    - 无
    返回：
    - List[Dict]: 包含中证1000股指期权波动率指数QVIX日线数据，
    Dict包含字段: ['date', 'open', 'high', 'low', 'close']
    """
    try:
        return data_service.get_1000index_qvix()
    except Exception as e:
        logger.error(f"获取中证1000股指期权波动率指数QVIX日线数据失败: {str(e)}")
        traceback.print_exc()
        return []

@mcp.tool()
async def get_1000index_min_qvix() -> list:
    """ 获取中证1000股指期权波动率指数QVIX分钟线数据
    参数：
    - 无
    返回：
    - List[Dict]: 包含中证1000股指期权波动率指数QVIX分钟线数据，
    Dict包含字段: ['time', 'qvix']
    """
    try:
        return data_service.get_1000index_min_qvix()
    except Exception as e:
        logger.error(f"获取中证1000股指期权波动率指数QVIX分钟线数据失败: {str(e)}")
        traceback.print_exc()
        return []

@mcp.tool()
async def get_50index_qvix() -> list:
    """ 获取上证50股指期权波动率指数QVIX日线数据
    参数：
    - 无
    返回：
    - List[Dict]: 包含上证50股指期权波动率指数QVIX日线数据，
    Dict包含字段: ['date', 'open', 'high', 'low', 'close']
    """
    try:
        return data_service.get_50index_qvix()
    except Exception as e:
        logger.error(f"获取上证50股指期权波动率指数QVIX日线数据失败: {str(e)}")
        traceback.print_exc()
        return []

@mcp.tool()
async def get_50index_min_qvix() -> list:
    """ 获取上证50股指期权波动率指数QVIX分钟线数据
    参数：
    - 无
    返回：
    - List[Dict]: 包含上证50股指期权波动率指数QVIX分钟线数据，
    Dict包含字段: ['time', 'qvix']
    """
    try:
        return data_service.get_50index_min_qvix()
    except Exception as e:
        logger.error(f"获取上证50股指期权波动率指数QVIX分钟线数据失败: {str(e)}")
        traceback.print_exc()
        return []
