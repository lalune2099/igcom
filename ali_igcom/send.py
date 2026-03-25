# coding=utf-8

from smtplib import SMTP
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import os

# ---------------------- Gmail 核心配置 ----------------------
send_usr = 'xieminggen@gmail.com'  # 如 xxx@gmail.com
send_pwd = 'hqsivzuksymeyuga'  # 生成的授权码（无空格）
reverse = '18826728999@139.com'  # 可填任意邮箱（Gmail/QQ/139等）
# 附件路径（按你的文件路径直接填写，Windows系统无需修改斜杠）
attachment_path = '/igcom/historical_data_20251117_180148/all_epics_30Min_mid_prices_20251117.xlsx'
# ------------------------------------------------------------------------

content = '这是用Python脚本发送的Gmail邮件，附带Excel数据附件，请查收！'
email_server = 'smtp.gmail.com'
email_port = 587  # TLS端口（兼容稳定）
email_title = 'Excel数据附件 - 20251108'

def send_gmail_with_attachment():
    # 构建邮件主体（支持正文+附件）
    msg = MIMEMultipart()
    msg['Subject'] = email_title
    msg['From'] = send_usr
    msg['To'] = reverse

    # 添加邮件正文（纯文本格式）
    msg.attach(MIMEText(content, 'plain', 'utf-8'))

    # 处理并添加附件
    if os.path.exists(attachment_path):
        # 读取附件文件（二进制模式）
        with open(attachment_path, 'rb') as f:
            # 构建附件对象，指定文件类型为Excel
            attachment = MIMEApplication(f.read(), _subtype='xlsx')
            # 设置附件头，指定文件名（收件人看到的文件名）
            attachment.add_header(
                'Content-Disposition',
                'attachment',
                filename=os.path.basename(attachment_path)  # 自动获取原文件名
            )
            msg.attach(attachment)
        print(f'已添加附件：{os.path.basename(attachment_path)}')
    else:
        print(f'警告：附件路径不存在！{attachment_path}')
        return  # 附件不存在时终止发送

    # 连接服务器并发送
    try:
        smtp = SMTP(email_server, email_port, timeout=30)
        smtp.starttls()  # 启用TLS加密
        smtp.login(send_usr, send_pwd)
        smtp.sendmail(send_usr, [reverse], msg.as_string())
        smtp.quit()
        print('✅ Gmail邮件（含附件）发送成功！')
    except Exception as e:
        print(f'❌ 发送失败：{str(e)}')

if __name__ == '__main__':
    send_gmail_with_attachment()