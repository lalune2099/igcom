from trading_ig import IGService
from trading_ig.rest import ApiExceededException
import logging
from datetime import datetime, timedelta
import pandas as pd
from pandas import json_normalize
import os
import warnings
import pytz
from tenacity import Retrying, wait_exponential, retry_if_exception_type
# 邮件发送相关库
from smtplib import SMTP
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

# 过滤警告
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', category=pd.errors.SettingWithCopyWarning)

# 日志配置
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
if not logger.handlers:
    logger.addHandler(console_handler)
else:
    logger.handlers = []
    logger.addHandler(console_handler)

# 时区配置
TZ_BEIJING = pytz.timezone('Asia/Shanghai')
TZ_LONDON = pytz.timezone('Europe/London')
TZ_UTC = pytz.UTC

# IG账户配置（替换为你的实际信息）
class IGConfig(object):
    username = 'xmg6666'
    password = 'Yj123456@'
    api_key = '13f0de841aa5247e997f77cb90e4a56373380d83'
    acc_type = "LIVE"  # LIVE=实盘 / DEMO=模拟盘

# # IG账户配置（重命名避免冲突）
# class IGConfig(object):
#     username = 'Lalune20999'
#     password = 'Yj123456@'
#     api_key = 'd23ccdabe844c198fa46accfe03820af315dab52'
#     acc_type = "LIVE"  # LIVE=实盘 / DEMO=模拟盘
#     # acc_number = "ABC123"  # 可选，账户号码（v3会话需要时取消注释）
#     #需要减去8h

# ====================== 邮件发送配置（多收件人） ======================
class EmailConfig(object):
    # Gmail配置
    send_usr = 'xieminggen@gmail.com'  # 发送方Gmail邮箱
    send_pwd = 'hqsivzuksymeyuga'      # Gmail授权码（无空格）
    # 多收件人列表
    receive_usr_list = [
        'shawncarpediem1121@gmail.com',
        'elitoc@163.com',
        '18826728999@139.com'
    ]
    email_server = 'smtp.gmail.com'    # Gmail SMTP服务器
    email_port = 587                   # TLS加密端口

# ====================== 核心配置 ======================
# 目标产品EPIC映射
TARGET_EPIC_MAP = {
    'IX.D.SPTRD.IFMM.IP': 'US 500 Cash ($1)',
    'IX.D.HANGSENG.IFU.IP': 'Hong Kong HS50 Cash ($1)',
    'IX.D.NIKKEI.IFM.IP': 'Japan 225 Cash ($1)'
}
# 抓取触发条件：整点后≥10分钟
TRIGGER_MINUTE_THRESHOLD = 10
# 数据根目录（Linux:/igcom | Windows:D:\Desktop\new）
DATA_ROOT_DIR = '/igcom'  # 按需修改为Windows路径：r"D:\Desktop\new"

def get_daily_file_path():
    """
    按伦敦时间生成当日数据文件路径
    格式：根目录/ig_accumulated_1h_data_YYYYMMDD.xlsx
    """
    # 获取当前伦敦日期
    london_today = datetime.now(TZ_LONDON).date()
    # 生成带日期的文件名
    daily_filename = f"ig_accumulated_1h_data_{london_today.strftime('%Y%m%d')}.xlsx"
    # 拼接完整路径
    daily_file_path = os.path.join(DATA_ROOT_DIR, daily_filename)
    
    # 确保根目录存在
    if not os.path.exists(DATA_ROOT_DIR):
        os.makedirs(DATA_ROOT_DIR)
        print(f"📁 创建数据根目录：{DATA_ROOT_DIR}")
    
    return daily_file_path

def safe_sheet_name(name: str) -> str:
    """清理Excel工作表名"""
    invalid_chars = r'\/:*?"<>|'
    for char in invalid_chars:
        name = name.replace(char, '_')
    return name[:31]

def safe_mid_prices(prices, version):
    """提取Close中间价，转换为伦敦时间"""
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

    # UTC转伦敦时间（去除时区）
    df.index = df.index.tz_localize(TZ_UTC).tz_convert(TZ_LONDON).tz_localize(None)
    df.index.name = "DateTime (London)"
    # 计算Close中间价
    df["Close"] = df[["closePrice.bid", "closePrice.ask"]].mean(axis=1)
    # 删除冗余列
    drop_cols = [col for col in df.columns if col != "Close"]
    df = df.drop(columns=drop_cols, errors="ignore")

    return df

def calculate_target_hour():
    """自动计算目标抓取小时（伦敦时间）→ 修复：仅返回target_hour，current_london单独计算"""
    current_london = datetime.now(TZ_LONDON)
    current_hour = current_london.hour
    current_minute = current_london.minute

    print(f"🕒 当前伦敦时间：{current_london.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📅 今日伦敦日期：{current_london.date().strftime('%Y-%m-%d')}")
    print(f"📄 今日数据文件：{get_daily_file_path()}")
    
    if current_minute >= TRIGGER_MINUTE_THRESHOLD:
        target_hour = current_hour - 1
        if target_hour < 0:
            target_hour = 23
        print(f"✅ 达到抓取条件，自动抓取 {target_hour} 点数据")
        return target_hour, current_london  # 保留元组返回，但确保无None解包
    else:
        next_trigger = current_london.replace(minute=TRIGGER_MINUTE_THRESHOLD, second=0, microsecond=0)
        wait_mins = (next_trigger - current_london).total_seconds() / 60
        print(f"⚠️ 未到抓取时间，需等待 {wait_mins:.1f} 分钟")
        return None, current_london  # 返回元组（None + current_london）

def fetch_single_product_1h_data(ig_service, epic: str, product_name: str, target_hour: int = None) -> pd.DataFrame:
    """抓取单个产品指定小时的1h数据 → 修复：target_hour设为可选参数"""
    if target_hour is None:
        logger.warning(f"target_hour为None，跳过{product_name}抓取")
        return pd.DataFrame()
    
    end_date_london = datetime.now(TZ_LONDON)
    end_date_london = end_date_london.replace(hour=target_hour + 1, minute=0, second=0, microsecond=0)
    start_date_london = end_date_london - timedelta(hours=2)

    # 转换为UTC时间字符串
    start_utc = start_date_london.astimezone(TZ_UTC)
    end_utc = end_date_london.astimezone(TZ_UTC)
    start_str = start_utc.strftime('%Y-%m-%dT%H:%M:%S')
    end_str = end_utc.strftime('%Y-%m-%dT%H:%M:%S')

    try:
        response = ig_service.fetch_historical_prices_by_epic(
            epic=epic, resolution='1h', start_date=start_str, end_date=end_str, format=safe_mid_prices
        )
        df = response['prices']
        if df.empty:
            logger.warning(f"{product_name} 无1h数据")
            return pd.DataFrame()

        # 筛选目标小时并创建副本（消除警告）→ 修复：删除重复copy
        df = df[df.index.hour == target_hour].copy()
        # 添加产品标识
        df['Product Name'] = product_name
        df['Epic'] = epic
        df['Resolution'] = '1h'

        print(f"✅ {product_name} 抓取完成：{len(df)} 条数据")
        return df
    except Exception as e:
        logger.error(f"抓取{product_name}失败：{str(e)}")
        return pd.DataFrame()

def load_accumulated_data():
    """加载当日累积数据 → 修复：移除返回值标注（兼容低版本Python）"""
    daily_file_path = get_daily_file_path()
    if not os.path.exists(daily_file_path):
        print(f"📁 首次运行今日数据文件，创建：{daily_file_path}")
        # 修复：首次运行强制创建空Excel文件（避免文件不存在）
        empty_df = pd.DataFrame(columns=['DateTime (London)', 'Close', 'Product Name', 'Epic', 'Resolution'])
        with pd.ExcelWriter(daily_file_path, engine='openpyxl') as writer:
            empty_df.to_excel(writer, sheet_name='All_Accumulated_Data', index=True)
        return empty_df
    
    try:
        df = pd.read_excel(
            daily_file_path,
            index_col='DateTime (London)',
            parse_dates=['DateTime (London)']
        )
        print(f"📁 加载今日累积数据：{len(df)} 条记录")
        return df
    except Exception as e:
        logger.error(f"加载今日累积数据失败：{str(e)}")
        return pd.DataFrame(columns=['DateTime (London)', 'Close', 'Product Name', 'Epic', 'Resolution'])

def save_accumulated_data(new_df: pd.DataFrame):
    """保存当日累积数据 → 修复：移除返回值（与Windows版本一致）"""
    daily_file_path = get_daily_file_path()
    history_df = load_accumulated_data()
    
    if not history_df.empty:
        new_reset = new_df.reset_index()
        history_reset = history_df.reset_index()
        combined = pd.concat([history_reset, new_reset], ignore_index=True)
        # 去重
        combined = combined.drop_duplicates(subset=['DateTime (London)', 'Product Name'], keep='last')
        combined = combined.set_index('DateTime (London)').sort_index()
    else:
        combined = new_df

    # 保存到当日Excel文件
    with pd.ExcelWriter(daily_file_path, engine='openpyxl') as writer:
        combined.to_excel(writer, sheet_name='All_Accumulated_Data', index=True)
        for name, group in combined.groupby('Product Name'):
            sheet_name = safe_sheet_name(name)
            group.to_excel(writer, sheet_name=sheet_name, index=True)
    
    print(f"💾 今日累积数据保存完成：{daily_file_path}")
    print(f"📊 总计数据量：{len(combined)} 条")
    print(f"📈 各产品数据量：")
    for name, group in combined.groupby('Product Name'):
        print(f"   - {name}: {len(group)} 条")
    return combined  # 保留返回值（不影响运行）

def send_gmail_with_attachment(target_hour: int, current_london: datetime, final_df: pd.DataFrame):
    """发送带当日文件附件的Gmail邮件（多收件人）"""
    daily_file_path = get_daily_file_path()
    # 构建邮件标题（包含日期+小时）
    email_title = f"IG 1h累积数据 - 伦敦{current_london.date().strftime('%Y%m%d')} {target_hour}点 - {current_london.strftime('%H%M')}"
    
    # 构建邮件正文
    content = f"""
    IG 1h数据自动抓取&累积完成通知
    ==============================
    抓取时间（伦敦）：{current_london.strftime('%Y-%m-%d %H:%M:%S')}
    目标抓取小时：{target_hour} 点
    今日数据文件：{daily_file_path}
    
    本次抓取统计：
    ------------------------------
    """
    # 添加各产品统计信息
    for product_name, group in final_df.groupby('Product Name'):
        hours = sorted(group.index.hour.unique())
        content += f"✅ {product_name}：已累积 {len(hours)} 小时数据 → {hours}\n"
    
    content += f"""
    总计累积数据量：{len(final_df)} 条
    ==============================
    该邮件由Python脚本自动发送，请勿回复。
    """

    # 构建邮件对象
    msg = MIMEMultipart()
    msg['Subject'] = email_title
    msg['From'] = EmailConfig.send_usr
    # 多收件人：用逗号分隔显示
    msg['To'] = ', '.join(EmailConfig.receive_usr_list)

    # 添加正文
    msg.attach(MIMEText(content, 'plain', 'utf-8'))

    # 添加当日文件作为附件
    if os.path.exists(daily_file_path):
        with open(daily_file_path, 'rb') as f:
            attachment = MIMEApplication(f.read(), _subtype='xlsx')
            attachment.add_header(
                'Content-Disposition',
                'attachment',
                filename=os.path.basename(daily_file_path)
            )
            msg.attach(attachment)
        print(f'📎 已添加附件：{os.path.basename(daily_file_path)}')
    else:
        print(f'❌ 附件不存在：{daily_file_path}')
        return

    # 发送邮件
    try:
        smtp = SMTP(EmailConfig.email_server, EmailConfig.email_port, timeout=30)
        smtp.starttls()
        smtp.login(EmailConfig.send_usr, EmailConfig.send_pwd)
        smtp.sendmail(EmailConfig.send_usr, EmailConfig.receive_usr_list, msg.as_string())
        smtp.quit()
        print(f'✅ Gmail邮件发送成功！收件人：{", ".join(EmailConfig.receive_usr_list)}')
    except Exception as e:
        print(f'❌ 邮件发送失败：{str(e)}')

def run_accumulated_data_fetch():
    """主执行函数：按伦敦时间每日分文件+多收件人邮件"""
    print("🚀 启动IG 1h数据自动累积+邮件发送程序（每日分文件+多收件人）")
    print(f"📂 数据根目录：{DATA_ROOT_DIR}")
    print(f"📧 收件人列表：{', '.join(EmailConfig.receive_usr_list)}")
    print(f"📌 抓取规则：伦敦时间整点后≥{TRIGGER_MINUTE_THRESHOLD}分钟抓取上一小时数据")

    # 初始化标识
    is_data_fetched_success = False
    final_df = None
    target_hour = None
    current_london = None

    try:
        # 计算目标小时（修复：解包元组，即使target_hour为None也不报错）
        target_hour, current_london = calculate_target_hour()
        if target_hour is None:
            print("❌ 未满足抓取条件，退出程序")
            return

        # 初始化IG服务 → 修复：显式指定参数名（兼容trading_ig不同版本）
        retryer = Retrying(wait=wait_exponential(), retry=retry_if_exception_type(ApiExceededException))
        ig_service = IGService(
            username=IGConfig.username,
            password=IGConfig.password,
            api_key=IGConfig.api_key,
            acc_type=IGConfig.acc_type,
            retryer=retryer,
            use_rate_limiter=True
        )

        all_new_data = []
        # 创建IG会话
        try:
            ig_service.create_session()
            print("✅ IG会话创建成功")
        except Exception as e:
            raise Exception(f"IG会话创建失败：{str(e)}")

        # 遍历产品抓取数据
        for epic, name in TARGET_EPIC_MAP.items():
            print(f"\n--- 抓取 {name} ---")
            df = fetch_single_product_1h_data(ig_service, epic, name, target_hour)
            if not df.empty:
                all_new_data.append(df)

        # 判断是否抓取到新数据
        if len(all_new_data) > 0:
            new_combined = pd.concat(all_new_data).sort_index()
            final_df = save_accumulated_data(new_combined)
            is_data_fetched_success = True
            print(f"\n✅ 本次成功抓取 {len(all_new_data)} 个产品的新数据")
        else:
            print("\n⚠️ 所有产品均未抓取到新数据")

    except Exception as e:
        logger.error(f"数据抓取过程异常：{str(e)}")
        print(f"\n❌ 数据抓取失败：{str(e)}")
    finally:
        # 关闭IG会话
        try:
            ig_service.logout()
        except:
            pass
        print("🔚 IG会话已关闭")

    # 仅成功抓取时发送邮件
    if is_data_fetched_success and final_df is not None and target_hour is not None:
        print("\n📧 开始发送邮件（数据抓取成功+每日文件）...")
        send_gmail_with_attachment(target_hour, current_london, final_df)
    else:
        print("\n📧 跳过邮件发送（原因：数据抓取失败/无新数据）")

    # 打印当日统计
    daily_file_path = get_daily_file_path()
    if os.path.exists(daily_file_path):
        final_df = load_accumulated_data()
        if not final_df.empty:
            print("\n🎯 当日累积数据概览（伦敦时间）：")
            for name, group in final_df.groupby('Product Name'):
                hours = sorted(group.index.hour.unique())
                print(f"   {name}：已累积 {len(hours)} 小时 → {hours}")
        else:
            print("\n❌ 当日累积数据为空")
    else:
        print("\n❌ 当日数据文件不存在")

if __name__ == "__main__":
    run_accumulated_data_fetch()