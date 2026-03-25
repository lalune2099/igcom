# coding=utf-8

from trading_ig import IGService
from trading_ig.rest import ApiExceededException
import logging
from datetime import datetime, timedelta
import pandas as pd
from pandas import json_normalize
import os
import warnings
from tenacity import Retrying, wait_exponential, retry_if_exception_type

# 过滤FutureWarning（消除库过时参数警告）
warnings.filterwarnings('ignore', category=FutureWarning)

# 设置日志（输出时间+模块+级别+信息）
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# IG账户配置（重命名避免冲突）
class IGConfig(object):
    username = 'Lalune20999'
    password = 'Yj123456@'
    api_key = 'd23ccdabe844c198fa46accfe03820af315dab52'
    acc_type = "LIVE"  # LIVE=实盘 / DEMO=模拟盘
    # acc_number = "ABC123"  # 可选，账户号码（v3会话需要时取消注释）

# 核心：Epic与英文名称的映射字典（严格对应你的注释）
EPIC_TO_NAME = {
    'IX.D.SPTRD.IFMM.IP': 'US 500 Cash ($1)',
    'IX.D.HANGSENG.IFU.IP': 'Hong Kong HS50 Cash ($1)',
    'IX.D.NIKKEI.IFM.IP': 'Japan 225 Cash ($1)',
    'CS.D.USDJPY.CFD.IP': 'USD/JPY',
    'CS.D.USDSGD.CFD.IP': 'USD/SGD',
    'IX.D.FTSE.IFMM.IP': 'UK 100 Cash ($1)',
    'CS.D.GBPUSD.CFD.IP': 'GBP/USD',
    'IX.D.CAC.IFMM.IP': 'France 40 Cash ($1)',
    'CS.D.EURUSD.CFD.IP': 'EUR/USD',
    'CS.D.USDINR.MINI.IP': 'EMFX USD/INR ($1 Mini Contract)',
    'IX.D.DAX.IFMS.IP': 'Germany 40 Cash ($1)',
    'CS.D.USDCNH.CFD.IP': 'USD/CNH',
    'CS.D.USDTWD.MINI.IP': 'EMFX USD/TWD ($1 Mini Contract)',
    'IX.D.ASX.IFMM.IP': 'Australia 200 Cash ($1)',
    'CS.D.AUDUSD.CFD.IP': 'AUD/USD',
    'CS.D.USDKRW.MINI.IP': 'EMFX USD/KRW ($1 Mini Contract)',
    'CS.D.USDMXN.CFD.IP': 'USD/MXN'
}

# 新增：Excel工作表名安全清理函数
def safe_sheet_name(name):
    """清理Excel工作表名（移除不支持的特殊字符，限制31字符）"""
    invalid_chars = r'\/:*?"<>|'  # Excel禁止的8个特殊字符
    for char in invalid_chars:
        name = name.replace(char, '_')
    return name[:31]  # 限制最大长度31字符

# 支持的时间间隔（IG API兼容列表）
SUPPORTED_RESOLUTIONS = ['1Min', '5Min', '15Min', '30Min', '1h', '2h', '4h', 'D', 'W', 'ME']

def safe_mid_prices(prices, version):
    """
    安全的mid_prices格式化函数，处理日期格式问题，计算中间价
    """
    if len(prices) == 0:
        raise Exception("Historical price data not found")

    df = json_normalize(prices)

    if version == "3":
        df = df.set_index("snapshotTimeUTC")
        df = df.drop(columns=["snapshotTime"], errors='ignore')
        df.index = pd.to_datetime(df.index, format='ISO8601')  # 自动推断ISO8601日期格式
    else:
        df = df.set_index("snapshotTime")
        from trading_ig.utils import DATE_FORMATS
        date_format = DATE_FORMATS[int(version)]
        df.index = pd.to_datetime(df.index, format=date_format)

    df.index.name = "DateTime"

    # 计算开盘/最高/最低/收盘中间价（bid+ask平均值）
    df["Open"] = df[["openPrice.bid", "openPrice.ask"]].mean(axis=1)
    df["High"] = df[["highPrice.bid", "highPrice.ask"]].mean(axis=1)
    df["Low"] = df[["lowPrice.bid", "lowPrice.ask"]].mean(axis=1)
    df["Close"] = df[["closePrice.bid", "closePrice.ask"]].mean(axis=1)

    # 删除冗余列（保留lastTradedVolume成交量）
    drop_cols = [
        "openPrice.lastTraded", "closePrice.lastTraded",
        "highPrice.lastTraded", "lowPrice.lastTraded",
        "openPrice.bid", "openPrice.ask", "closePrice.bid", "closePrice.ask",
        "highPrice.bid", "highPrice.ask", "lowPrice.bid", "lowPrice.ask"
    ]
    df = df.drop(columns=[col for col in drop_cols if col in df.columns])

    return df

def get_multiple_historical_prices(
    epic_list, 
    resolution='30Min', 
    start_date=None,
    end_date=None,
    days=1, 
    save_individual=True, 
    save_combined=True
):
    """
    批量获取多个金融产品的历史价格数据（Epic自动替换为英文名称）

    :param epic_list: Epic列表
    :param resolution: 时间间隔（默认30Min）
    :param start_date: 开始时间（datetime对象，可选）
    :param end_date: 结束时间（datetime对象，可选）
    :param days: 默认获取天数（start_date为空时生效）
    :param save_individual: 是否单独保存每个产品数据（默认True）
    :param save_combined: 是否保存合并数据（默认True）
    :return: 字典，key=Epic，value=数据DataFrame
    """
    # 校验时间间隔合法性
    if resolution not in SUPPORTED_RESOLUTIONS:
        raise ValueError(f"Unsupported resolution! Supported options: {SUPPORTED_RESOLUTIONS}")
    
    # 初始化重试机制（应对API限流）
    retryer = Retrying(
        wait=wait_exponential(),  # 指数退避等待（避免频繁请求）
        retry=retry_if_exception_type(ApiExceededException)  # 仅对限流异常重试
    )

    # 初始化IG服务
    ig_service = IGService(
        IGConfig.username,
        IGConfig.password,
        IGConfig.api_key,
        IGConfig.acc_type,
        retryer=retryer,
        use_rate_limiter=True  # 启用速率限制
    )

    all_data = {}  # 存储所有产品数据（key=Epic）
    combined_data = []  # 存储合并数据

    # 处理时间范围
    if end_date is None:
        end_date = datetime.now()
    if start_date is None:
        start_date = end_date - timedelta(days=days)
    start_date_str = start_date.strftime('%Y-%m-%dT%H:%M:%S')
    end_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%S')

    print(f"📅 Time Range: {start_date_str} to {end_date_str}")
    print(f"⏰ Resolution: {resolution}")
    print(f"📊 Total Products to Fetch: {len(epic_list)}")

    try:
        # 创建IG会话
        ig_service.create_session()
        print("✅ IG Session Created Successfully")

        # 创建输出目录（按时间戳命名，避免重复）
        output_dir = f"historical_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        if save_individual or save_combined:
            os.makedirs(output_dir, exist_ok=True)
            print(f"📁 Output Directory Created: {output_dir}")

        # 循环获取每个产品数据
        for i, epic in enumerate(epic_list, 1):
            # 获取产品英文名称（未匹配到则显示原Epic）
            product_name = EPIC_TO_NAME.get(epic, epic)
            print(f"\n--- Fetching {i}/{len(epic_list)} Product: {product_name} (Epic: {epic}) ---")

            try:
                # 调用IG API获取历史数据
                response = ig_service.fetch_historical_prices_by_epic(
                    epic=epic,
                    resolution=resolution,
                    start_date=start_date_str,
                    end_date=end_date_str,
                    format=safe_mid_prices
                )

                prices_df = response['prices']
                print(f"✅ Successfully Fetched {len(prices_df)} Records")

                # 添加产品英文名称和Epic列（便于识别和溯源）
                prices_df['Product Name'] = product_name
                prices_df['Epic'] = epic  # 保留原Epic，便于核对

                # 存储数据到字典
                all_data[epic] = prices_df

                # 添加到合并数据列表
                if save_combined:
                    combined_data.append(prices_df)

                # 单独保存每个产品的CSV文件（用英文名称命名，处理特殊字符）
                if save_individual:
                    # 清理文件名（兼容Excel和系统路径）
                    safe_filename = safe_sheet_name(product_name).replace(' ', '_').replace('$', 'USD')
                    csv_filename = os.path.join(output_dir,
                                              f"{safe_filename}_{resolution}_mid_prices_{datetime.now().strftime('%Y%m%d')}.csv")
                    prices_df.to_csv(csv_filename, encoding='utf-8')
                    print(f"💾 Individual Data Saved to: {csv_filename}")

                # 显示数据预览（包含英文名称）
                print("\n📈 Data Preview:")
                print(prices_df[['Product Name', 'Open', 'High', 'Low', 'Close', 'lastTradedVolume']].head())

                # 显示数据统计信息
                print("\n📊 Data Statistics:")
                print(prices_df[['Open', 'High', 'Low', 'Close', 'lastTradedVolume']].describe())

            except Exception as e:
                logger.error(f"Failed to Fetch Data for {product_name} ({epic}): {str(e)}")
                print(f"❌ Error Fetching Data for {product_name}: {str(e)}")
                continue

        # 保存合并数据（CSV+Excel）
        if save_combined and combined_data:
            # 合并数据并去重、排序
            combined_df = pd.concat(combined_data, ignore_index=False)
            combined_df = combined_df.sort_index().drop_duplicates()  # 按时间排序+去重

            # 保存合并CSV
            combined_csv_filename = os.path.join(output_dir,
                                               f"Combined_Data_{resolution}_mid_prices_{datetime.now().strftime('%Y%m%d')}.csv")
            combined_df.to_csv(combined_csv_filename, encoding='utf-8')
            print(f"\n💾 Combined Data Saved to: {combined_csv_filename}")
            print(f"📊 Total Combined Records: {len(combined_df)}")

            # 保存Excel文件（多工作表：合并数据+单个产品数据）
            excel_filename = os.path.join(output_dir,
                                        f"All_Products_{resolution}_mid_prices_{datetime.now().strftime('%Y%m%d')}.xlsx")
            with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
                # 合并数据工作表
                combined_df.to_excel(writer, sheet_name='Combined_Data', index=True)
                # 单个产品数据工作表（使用清理后的安全名称）
                for epic, df in all_data.items():
                    sheet_name = safe_sheet_name(EPIC_TO_NAME.get(epic, epic))
                    df.to_excel(writer, sheet_name=sheet_name, index=True)
            print(f"💾 Excel File Saved to: {excel_filename}")

        return all_data

    except Exception as e:
        logger.error(f"Overall Execution Failed: {str(e)}")
        print(f"❌ Overall Error: {str(e)}")
        return None

    finally:
        # 确保会话关闭
        ig_service.logout()
        print("🔚 Session Closed")

def get_hangseng_historical_prices():
    """
    单独获取恒生指数期货历史价格数据（保持原有功能）
    """
    epics = ['IX.D.HANGSENG.IFU.IP']
    return get_multiple_historical_prices(epics)

if __name__ == "__main__":
    # 定义要获取的Epic列表（与EPIC_TO_NAME对应）
    epic_list = [
        'IX.D.SPTRD.IFMM.IP',    # US 500 Cash ($1)
        'IX.D.HANGSENG.IFU.IP',  # Hong Kong HS50 Cash ($1)
        'IX.D.NIKKEI.IFM.IP',    # Japan 225 Cash ($1)
        'CS.D.USDJPY.CFD.IP',    # USD/JPY
        'CS.D.USDSGD.CFD.IP',    # USD/SGD
        'IX.D.FTSE.IFMM.IP',     # UK 100 Cash ($1)
        'CS.D.GBPUSD.CFD.IP',    # GBP/USD
        'IX.D.CAC.IFMM.IP',      # France 40 Cash ($1)
        'CS.D.EURUSD.CFD.IP',    # EUR/USD
        'CS.D.USDINR.MINI.IP',   # EMFX USD/INR ($1 Mini Contract)
        'IX.D.DAX.IFMS.IP',      # Germany 40 Cash ($1)
        'CS.D.USDCNH.CFD.IP',    # USD/CNH
        'CS.D.USDTWD.MINI.IP',   # EMFX USD/TWD ($1 Mini Contract)
        'IX.D.ASX.IFMM.IP',      # Australia 200 Cash ($1)
        'CS.D.AUDUSD.CFD.IP',    # AUD/USD
        'CS.D.USDKRW.MINI.IP',   # EMFX USD/KRW ($1 Mini Contract)
        'CS.D.USDMXN.CFD.IP'     # USD/MXN
    ]

    print("🚀 Starting Batch Historical Data Fetch...")

    # 批量获取数据（可调整参数：时间间隔、天数等）
    all_historical_data = get_multiple_historical_prices(
        epic_list=epic_list,
        resolution='30Min',  # 时间间隔：30分钟（支持其他选项见SUPPORTED_RESOLUTIONS）
        days=1,              # 获取1天数据（也可手动传入start_date和end_date）
        save_individual=True,
        save_combined=True
    )

    # 输出执行结果汇总
    if all_historical_data:
        print(f"\n🎯 All Data Fetch Completed!")
        print(f"📊 Successfully Fetched Data for {len(all_historical_data)} Products")
        print("\n📋 Product Data Summary:")
        for epic, df in all_historical_data.items():
            product_name = EPIC_TO_NAME.get(epic, epic)
            print(f"  📈 {product_name}: {len(df)} Records, Time Range: {df.index.min()} to {df.index.max()}")
    else:
        print("❌ Data Fetch Failed")