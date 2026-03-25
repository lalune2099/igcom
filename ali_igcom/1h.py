# -*- coding: utf-8 -*-
"""
IG 每日 1h 数据抓取脚本（过去24小时，伦敦时间）
- 每天执行一次
- 只抓过去24小时
- 有数据才保存 & 发一次邮件
"""

from trading_ig import IGService
from trading_ig.rest import ApiExceededException
from tenacity import Retrying, wait_exponential, retry_if_exception_type

import pandas as pd
from pandas import json_normalize
from datetime import datetime, timedelta
import pytz
import os
import logging
import warnings

from smtplib import SMTP
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

# =========================
# 基础设置
# =========================
warnings.filterwarnings("ignore")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# 时区
TZ_LONDON = pytz.timezone("Europe/London")
TZ_UTC = pytz.UTC

# 数据目录（云服务器）
DATA_ROOT_DIR = "/igcom"

# =========================
# IG 账户配置
# =========================
class IGConfig:
    username = "xmg6666"
    password = "Yj123456@"
    api_key = "13f0de841aa5247e997f77cb90e4a56373380d83"
    acc_type = "LIVE"

# =========================
# 邮件配置
# =========================
class EmailConfig:
    send_usr = "xieminggen@gmail.com"
    send_pwd = "hqsivzuksymeyuga"   # Gmail 授权码
    receive_usr_list = [
        # "shawncarpediem1121@gmail.com",
        # "elitoc@163.com",
        "18826728999@139.com"
    ]
    email_server = "smtp.gmail.com"
    email_port = 587

# =========================
# 目标产品
# =========================
TARGET_EPIC_MAP = {
    "IX.D.SPTRD.IFMM.IP": "US 500 Cash ($1)",
    "IX.D.HANGSENG.IFU.IP": "Hong Kong HS50 Cash ($1)",
    "IX.D.NIKKEI.IFM.IP": "Japan 225 Cash ($1)",
}

# =========================
# 工具函数
# =========================
def get_daily_file_path():
    """按伦敦日期生成当天文件名"""
    london_today = datetime.now(TZ_LONDON).date()
    filename = f"ig_accumulated_1h_data_{london_today.strftime('%Y%m%d')}.xlsx"
    os.makedirs(DATA_ROOT_DIR, exist_ok=True)
    return os.path.join(DATA_ROOT_DIR, filename)

def safe_sheet_name(name: str) -> str:
    return name.replace("/", "_").replace("$", "USD")[:31]

def safe_mid_prices(prices, version):
    if len(prices) == 0:
        raise Exception("No price data")

    df = json_normalize(prices)

    if version == "3":
        df = df.set_index("snapshotTimeUTC")
        df.index = pd.to_datetime(df.index)
    else:
        df = df.set_index("snapshotTime")
        from trading_ig.utils import DATE_FORMATS
        df.index = pd.to_datetime(df.index, format=DATE_FORMATS[int(version)])

    df.index = df.index.tz_localize(TZ_UTC).tz_convert(TZ_LONDON).tz_localize(None)
    df.index.name = "DateTime (London)"

    df["Close"] = df[["closePrice.bid", "closePrice.ask"]].mean(axis=1)
    return df[["Close"]]

# =========================
# 核心逻辑
# =========================
def fetch_last_24h_1h_data(ig_service, epic, product_name):
    """抓取过去24小时（伦敦时间）的1h数据"""

    end_london = datetime.now(TZ_LONDON)
    start_london = end_london - timedelta(days=1)

    start_utc = start_london.astimezone(TZ_UTC)
    end_utc = end_london.astimezone(TZ_UTC)

    logger.info(
        f"{product_name} 抓取区间（London）: "
        f"{start_london} → {end_london}"
    )

    try:
        resp = ig_service.fetch_historical_prices_by_epic(
            epic=epic,
            resolution="1h",
            start_date=start_utc.strftime("%Y-%m-%dT%H:%M:%S"),
            end_date=end_utc.strftime("%Y-%m-%dT%H:%M:%S"),
            format=safe_mid_prices
        )

        df = resp["prices"]
        if df.empty:
            logger.warning(f"{product_name} 无数据")
            return pd.DataFrame()

        df["Product Name"] = product_name
        df["Epic"] = epic
        df["Resolution"] = "1h"

        logger.info(f"{product_name} 获取 {len(df)} 条")
        return df

    except Exception as e:
        logger.error(f"{product_name} 抓取失败: {e}")
        return pd.DataFrame()

# =========================
# 邮件发送
# =========================
def send_email_with_attachment(file_path, df):
    msg = MIMEMultipart()
    today = datetime.now(TZ_LONDON).strftime("%Y%m%d")
    msg["Subject"] = f"IG 1h 数据（日更）- {today}"
    msg["From"] = EmailConfig.send_usr
    msg["To"] = ", ".join(EmailConfig.receive_usr_list)

    body = f"""
IG 1h 数据每日抓取完成（伦敦时间）

文件名：
{file_path}

数据统计：
"""
    for name, g in df.groupby("Product Name"):
        hours = sorted(g.index.hour.unique())
        body += f"- {name}: {len(hours)} 小时 → {hours}\n"

    msg.attach(MIMEText(body, "plain", "utf-8"))

    with open(file_path, "rb") as f:
        part = MIMEApplication(f.read(), _subtype="xlsx")
        part.add_header("Content-Disposition", "attachment", filename=os.path.basename(file_path))
        msg.attach(part)

    smtp = SMTP(EmailConfig.email_server, EmailConfig.email_port)
    smtp.starttls()
    smtp.login(EmailConfig.send_usr, EmailConfig.send_pwd)
    smtp.sendmail(
        EmailConfig.send_usr,
        EmailConfig.receive_usr_list,
        msg.as_string()
    )
    smtp.quit()

    logger.info("📧 邮件发送成功")

# =========================
# 主入口
# =========================
def main():
    logger.info("🚀 IG 每日 1h 数据抓取启动")

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

    all_data = []

    try:
        ig_service.create_session()
        logger.info("✅ IG 会话已创建")

        for epic, name in TARGET_EPIC_MAP.items():
            df = fetch_last_24h_1h_data(ig_service, epic, name)
            if not df.empty:
                all_data.append(df)

    finally:
        try:
            ig_service.logout()
        except:
            pass
        logger.info("🔚 IG 会话关闭")

    if not all_data:
        logger.warning("❌ 过去24小时无任何数据，不保存、不发邮件")
        return

    final_df = pd.concat(all_data).sort_index()

    # 保存
    daily_file = get_daily_file_path()
    with pd.ExcelWriter(daily_file, engine="openpyxl") as writer:
        final_df.to_excel(writer, sheet_name="All_Data")
        for name, g in final_df.groupby("Product Name"):
            g.to_excel(writer, sheet_name=safe_sheet_name(name))

    logger.info(f"💾 数据已保存：{daily_file}")

    # 发邮件
    send_email_with_attachment(daily_file, final_df)

    logger.info("🎉 每日流程完成")

if __name__ == "__main__":
    main()
