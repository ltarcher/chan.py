import asyncio
from dataservices.index_data import IndexDataService
from dataservices.cache_manager import init_cache_manager, clear_all_cache
import qstock as qs

# 初始化缓存管理器，可以配置Redis和数据库参数
init_cache_manager(
    redis_host="200.200.167.104",
    redis_port=6379,
    db_type="sqlite",
    db_config={"db_path": "example_cache.db"}
)

async def main():
    
    data_service = IndexDataService()

    # 清除所有缓存
    print("=== 清除所有缓存 ===")
    clear_all_cache()

    # 获取5日内竞价总成交数据
    print("=== 获取5日内竞价总成交数据 ===")
    df = data_service.get_turnover_history_data(
        start_date='2025-06-22',
        end_date='2025-08-01',
        freq=1,
        fqt=1,
        use_chinese_fields=True
    )
    print(df)
    data = [d for d in df if '09:30:00' in d['日期']]
    print(data)

    print("=== 获取指定指数代码的历史数据 ===")
    df = data_service.get_stock_history_data(
        code=["000001", "000002"],
        start_date="2025-06-01",
        end_date="2025-07-31",
        freq=1,
        fqt=1,
        indicator=True
    )
    print(df)

    # 获取指数实时数据
    print("=== 获取指数实时数据 ===")
    df = data_service.get_index_realtime_data(codes=["上证指数", "深证成指", "创业板指"])
    print(df)

    # 获取历史成交数据
    print("\n=== 获取历史成交数据 ===")
    df = data_service.get_turnover_history_data(start_date='2025-06-22', end_date='2025-08-01', freq='d', fqt=1, use_chinese_fields=True)
    print(df)

    # 获取融资融券数据
    print("\n=== 获取融资融券数据 ===")
    df = data_service.get_rzrq_turnover_ratio(start_date='2024-01-02',  page=1, page_size=800, use_chinese_fields=True)
    print(df)
    
    # 获取美元指数数据
    print("\n=== 获取美元指数数据 ===")
    df = data_service.get_usd_index_data()
    print(df)
    
    # 获取富时A50期货数据
    print("\n=== 获取富时A50期货数据 ===")
    df = data_service.get_ftse_a50_futures_data()
    print(df)
    
    # 获取美元兑离岸人民币数据
    print("\n=== 获取美元兑离岸人民币数据 ===")
    df = data_service.get_usd_cnh_futures_data()
    print(df)
    
    # 获取三十年国债数据
    print("\n=== 获取三十年国债数据 ===")
    df = data_service.get_thirty_year_bond_futures_data()
    print(df)
    
    # 获取市场总成交数据
    print("\n=== 获取市场总成交数据 ===")
    df = data_service.get_board_trade_realtime_data()
    print(df)
    
    # 获取经济日历数据
    print("\n=== 获取经济日历数据 ===")
    df = data_service.get_economic_calendar()
    print(df)

if __name__ == "__main__":
    asyncio.run(main())