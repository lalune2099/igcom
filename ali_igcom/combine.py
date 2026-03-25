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
# 邮件相关依赖
from smtplib import SMTP
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

# ===================== 全局配置（绝对路径+核心参数）=====================
# 服务器文件绝对路径（基于你的目录 /root/igcom/）
BASE_DIR = '/igcom/'  # 基础目录
OUTPUT_DIR_PREFIX = 'historical_data_'  # 输出目录前缀
EXCEL_ATTACHMENT_PREFIX = 'All_Products_'  # Excel文件前缀（与数据生成逻辑一致）

# IG账户配置
class IGConfig(object):
    username = 'Lalune20999'
    password = 'Yj123456@'
    api_key = 'd23ccdabe844c198fa46accfe03820af315dab52'
    acc_type = "LIVE"  # LIVE=实盘 / DEMO=模拟盘
    # acc_number = "ABC123"  # 可选，账户号码（v3会话需要时取消注释）

# Gmail发送配置
GMAIL_CONFIG = {
    'send_usr': 'xieminggen@gmail.com',
    'send_pwd': 'hqsivzuksymeyuga',  # 授权码
    # 'receive_usr': '18826728999@139.com',  # 收件人邮箱
    'receive_usr': 'shawncarpediem1121@gmail.com',  # 收件人邮箱
    'email_title': f'Excel数据附件 - {datetime.now().strftime("%Y%m%d")}',  # 动态标题（含日期）
    'email_content': '这是igcom金融数据Excel附件，请查收！',
    'smtp_server': 'smtp.gmail.com',
    'smtp_port': 587  # TLS端口
}

# Epic与英文名称映射
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

# 支持的时间间隔（IG API兼容）
SUPPORTED_RESOLUTIONS = ['1Min', '5Min', '15Min', '30Min', '1h', '2h', '4h', 'D', 'W', 'ME']

# ===================== 工具函数 =====================
def safe_sheet_name(name):
    """清理Excel工作表名（移除非法字符，限制31字符）"""
    invalid_chars = r'\/:*?"<>|'
    for char in invalid_chars:
        name = name.replace(char, '_')
    return name[:31]

def safe_mid_prices(prices, version):
    """格式化价格数据，计算中间价"""
    if len(prices) == 0:
        raise Exception("Historical price data not found")

    df = json_normalize(prices)
    if version == "3":
        df = df.set_index("snapshotTimeUTC")
        df = df.drop(columns=["snapshotTime"], errors='ignore')
        df.index = pd.to_datetime(df.index, format='ISO8601')
    else:
        df = df.set_index("snapshotTime")
        from trading_ig.utils import DATE_FORMATS
        date_format = DATE_FORMATS[int(version)]
        df.index = pd.to_datetime(df.index, format=date_format)

    df.index.name = "DateTime"
    # 计算中间价
    df["Open"] = df[["openPrice.bid", "openPrice.ask"]].mean(axis=1)
    df["High"] = df[["highPrice.bid", "highPrice.ask"]].mean(axis=1)
    df["Low"] = df[["lowPrice.bid", "lowPrice.ask"]].mean(axis=1)
    df["Close"] = df[["closePrice.bid", "closePrice.ask"]].mean(axis=1)
    # 删除冗余列
    drop_cols = [
        "openPrice.lastTraded", "closePrice.lastTraded", "highPrice.lastTraded", "lowPrice.lastTraded",
        "openPrice.bid", "openPrice.ask", "closePrice.bid", "closePrice.ask",
        "highPrice.bid", "highPrice.ask", "lowPrice.bid", "lowPrice.ask"
    ]
    df = df.drop(columns=[col for col in drop_cols if col in df.columns])
    return df

# ===================== 数据获取函数 =====================
def get_multiple_historical_prices(
    epic_list, 
    resolution='30Min', 
    start_date=None,
    end_date=None,
    days=1, 
    save_individual=True, 
    save_combined=True
):
    """批量获取金融产品历史数据，返回输出目录路径"""
    if resolution not in SUPPORTED_RESOLUTIONS:
        raise ValueError(f"Unsupported resolution! Supported options: {SUPPORTED_RESOLUTIONS}")

    # 初始化IG服务
    retryer = Retrying(
        wait=wait_exponential(),
        retry=retry_if_exception_type(ApiExceededException)
    )
    ig_service = IGService(
        IGConfig.username,
        IGConfig.password,
        IGConfig.api_key,
        IGConfig.acc_type,
        retryer=retryer,
        use_rate_limiter=True
    )

    all_data = {}
    combined_data = []
    # 时间范围处理
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
        ig_service.create_session()
        print("✅ IG Session Created Successfully")

        # 创建输出目录（绝对路径）
        output_dir = os.path.join(BASE_DIR, f"{OUTPUT_DIR_PREFIX}{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        os.makedirs(output_dir, exist_ok=True)
        print(f"📁 Output Directory Created: {output_dir}")

        # 循环获取数据
        for i, epic in enumerate(epic_list, 1):
            product_name = EPIC_TO_NAME.get(epic, epic)
            print(f"\n--- Fetching {i}/{len(epic_list)} Product: {product_name} (Epic: {epic}) ---")
            try:
                response = ig_service.fetch_historical_prices_by_epic(
                    epic=epic, resolution=resolution, start_date=start_date_str,
                    end_date=end_date_str, format=safe_mid_prices
                )
                prices_df = response['prices']
                print(f"✅ Successfully Fetched {len(prices_df)} Records")

                prices_df['Product Name'] = product_name
                prices_df['Epic'] = epic
                all_data[epic] = prices_df
                if save_combined:
                    combined_data.append(prices_df)

                # 保存单个CSV（绝对路径）
                if save_individual:
                    safe_filename = safe_sheet_name(product_name).replace(' ', '_').replace('$', 'USD')
                    csv_filename = os.path.join(output_dir,
                                              f"{safe_filename}_{resolution}_mid_prices_{datetime.now().strftime('%Y%m%d')}.csv")
                    prices_df.to_csv(csv_filename, encoding='utf-8')
                    print(f"💾 Individual Data Saved to: {csv_filename}")

                # 数据预览
                print("\n📈 Data Preview:")
                print(prices_df[['Product Name', 'Open', 'High', 'Low', 'Close', 'lastTradedVolume']].head())
                print("\n📊 Data Statistics:")
                print(prices_df[['Open', 'High', 'Low', 'Close', 'lastTradedVolume']].describe())

            except Exception as e:
                logging.error(f"Failed to Fetch Data for {product_name} ({epic}): {str(e)}")
                print(f"❌ Error Fetching Data for {product_name}: {str(e)}")
                continue

        # 保存合并数据（CSV+Excel，绝对路径）
        if save_combined and combined_data:
            combined_df = pd.concat(combined_data, ignore_index=False).sort_index().drop_duplicates()
            # 合并CSV
            combined_csv = os.path.join(output_dir,
                                       f"Combined_Data_{resolution}_mid_prices_{datetime.now().strftime('%Y%m%d')}.csv")
            combined_df.to_csv(combined_csv, encoding='utf-8')
            print(f"\n💾 Combined Data Saved to: {combined_csv}")

            # 合并Excel（邮件附件核心文件）
            excel_filename = os.path.join(output_dir,
                                        f"{EXCEL_ATTACHMENT_PREFIX}{resolution}_mid_prices_{datetime.now().strftime('%Y%m%d')}.xlsx")
            with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
                combined_df.to_excel(writer, sheet_name='Combined_Data', index=True)
                for epic, df in all_data.items():
                    sheet_name = safe_sheet_name(EPIC_TO_NAME.get(epic, epic))
                    df.to_excel(writer, sheet_name=sheet_name, index=True)
            print(f"💾 Excel File Saved to: {excel_filename}")

        return output_dir  # 返回输出目录路径，用于邮件附件查找

    except Exception as e:
        logging.error(f"Overall Execution Failed: {str(e)}")
        print(f"❌ Overall Error: {str(e)}")
        return None

    finally:
        ig_service.logout()
        print("🔚 Session Closed")

# ===================== Gmail发送函数 =====================
def send_gmail_with_attachment(attachment_dir):
    """发送带Excel附件的Gmail，attachment_dir为数据输出目录"""
    # 构建邮件
    msg = MIMEMultipart()
    msg['Subject'] = GMAIL_CONFIG['email_title']
    msg['From'] = GMAIL_CONFIG['send_usr']
    msg['To'] = GMAIL_CONFIG['receive_usr']

    # 添加正文
    msg.attach(MIMEText(GMAIL_CONFIG['email_content'], 'plain', 'utf-8'))

    # 查找Excel附件（自动匹配输出目录中的Excel文件）
    excel_files = [f for f in os.listdir(attachment_dir) if f.endswith('.xlsx') and EXCEL_ATTACHMENT_PREFIX in f]
    if not excel_files:
        print(f"❌ No Excel file found in {attachment_dir}")
        return
    attachment_path = os.path.join(attachment_dir, excel_files[0])  # 取第一个匹配的Excel文件

    # 添加附件
    if os.path.exists(attachment_path):
        with open(attachment_path, 'rb') as f:
            attachment = MIMEApplication(f.read(), _subtype='xlsx')
            attachment.add_header(
                'Content-Disposition', 'attachment',
                filename=os.path.basename(attachment_path)
            )
            msg.attach(attachment)
        print(f'📎 Added Attachment: {os.path.basename(attachment_path)}')
    else:
        print(f'❌ Attachment not found: {attachment_path}')
        return

    # 发送邮件
    try:
        smtp = SMTP(GMAIL_CONFIG['smtp_server'], GMAIL_CONFIG['smtp_port'], timeout=30)
        smtp.starttls()
        smtp.login(GMAIL_CONFIG['send_usr'], GMAIL_CONFIG['send_pwd'])
        smtp.sendmail(GMAIL_CONFIG['send_usr'], [GMAIL_CONFIG['receive_usr']], msg.as_string())
        smtp.quit()
        print('✅ Gmail Sent Successfully!')
    except Exception as e:
        print(f'❌ Failed to Send Gmail: {str(e)}')

# ===================== 主函数（整合数据获取+邮件发送）=====================
def main():
    # 定义要获取的Epic列表
    epic_list = [
        'IX.D.SPTRD.IFMM.IP', 'IX.D.HANGSENG.IFU.IP', 'IX.D.NIKKEI.IFM.IP',
        'CS.D.USDJPY.CFD.IP', 'CS.D.USDSGD.CFD.IP', 'IX.D.FTSE.IFMM.IP',
        'CS.D.GBPUSD.CFD.IP', 'IX.D.CAC.IFMM.IP', 'CS.D.EURUSD.CFD.IP',
        'CS.D.USDINR.MINI.IP', 'IX.D.DAX.IFMS.IP', 'CS.D.USDCNH.CFD.IP',
        'CS.D.USDTWD.MINI.IP', 'IX.D.ASX.IFMM.IP', 'CS.D.AUDUSD.CFD.IP',
        'CS.D.USDKRW.MINI.IP', 'CS.D.USDMXN.CFD.IP'
    ]

    print("🚀 Starting Batch Data Fetch + Gmail Send Process...")
    # 1. 获取数据，返回输出目录路径
    output_dir = get_multiple_historical_prices(
        epic_list=epic_list,
        resolution='30Min',
        days=1,
        save_individual=True,
        save_combined=True
    )

    # 2. 若数据获取成功，发送邮件
    if output_dir and os.path.exists(output_dir):
        print(f"\n📤 Starting Gmail Send with Attachment from: {output_dir}")
        send_gmail_with_attachment(attachment_dir=output_dir)
    else:
        print("\n❌ Data Fetch Failed, Cannot Send Gmail")

if __name__ == "__main__":
    # 初始化日志
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    warnings.filterwarnings('ignore', category=FutureWarning)
    main()