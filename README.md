# IGCom 数据抓取与变化率表格自动化

这个仓库用于在服务器定时抓取 IG 金融市场数据，更新变化率 Excel 模板，并按需通过 Gmail 发送结果附件。

代码主体沿用原脚本的流程：

1. 从 IG API 抓取全量历史数据，包括 `1h` 和 `30Min` 两种粒度。
2. 数据索引统一转换为伦敦时间，并标注 `Resolution`。
3. 更新模板中的日期 sheet，包括 `05/07/15/20时` 和 `05/07/15/20变化率`。
4. 从全量数据中筛选模板需要的时间点：
   - `1h`：`05`、`07`、`15`、`19`、`20`
   - `30Min`：`18:00`、`18:30`
5. 将筛选后的 `Close` 写入模板。
6. 如果 `SEND_EMAIL = True`，通过 Gmail 发送最终 Excel 附件。

## 主要运行脚本

服务器定时任务主要运行这四个脚本：

| 脚本 | 默认账号配置 | 用途 |
| --- | --- | --- |
| `all.py` | `ACCOUNT1` | 主流程脚本，使用第 1 个 IG 账号 |
| `all2.py` | `ACCOUNT2` | 主流程脚本，使用第 2 个 IG 账号 |
| `all3.py` | `ACCOUNT3` | 主流程脚本，使用第 3 个 IG 账号 |
| `allMon.py` | `ACCOUNT4` | 主流程脚本，使用第 4 个 IG 账号 |

四个脚本逻辑基本一致，只是默认使用的 IG 账号不同。这样可以继续保持原来“四个脚本分别定时运行、分别对应四个账号”的方式。

## 目录与文件

| 文件 | 说明 |
| --- | --- |
| `config.py` | 统一读取 IG 账号、API key、Gmail 授权码和收件人 |
| `.env.example` | 环境变量模板，不包含真实密钥 |
| `.gitignore` | 忽略 `.env`、`apikey.txt`、缓存和运行输出 |
| `IG变化率表格(英区).xlsx` | 原始 Excel 模板 |
| `all.py` / `all2.py` / `all3.py` / `allMon.py` | 主要定时运行脚本 |
| `combine.py`、`for1day.py`、`live.py`、`onlyfor3.py`、`send.py`、`test.py` | 备用或历史辅助脚本，已改为从 `config.py` 读取配置 |
| `tests/test_config.py` | 配置读取测试 |

运行时会生成这些文件或目录，不需要提交到 GitHub：

| 运行输出 | 说明 |
| --- | --- |
| `outputs/historical_data_*/` | 每次抓取数据的输出目录 |
| `outputs/IG变化率_模版更新_YYYYMMDD.xlsx` | 日期更新后的模板 |
| `outputs/IG变化率_YYYYMMDD.xlsx` | 最终填好数据的结果表 |
| `__pycache__/` | Python 缓存 |

## 配置方式

真实账号、API key、Gmail 授权码不要写进代码，也不要上传 GitHub。服务器上需要在 `/igcom/.env` 放置配置文件。

可以先复制模板：

```bash
cd /igcom
cp .env.example .env
```

然后编辑 `.env`：

```bash
nano /igcom/.env
```

`.env` 需要填写这些字段：

```env
IG_ACCOUNT1_USERNAME=
IG_ACCOUNT1_PASSWORD=
IG_ACCOUNT1_API_KEY=
IG_ACCOUNT1_ACC_TYPE=LIVE

IG_ACCOUNT2_USERNAME=
IG_ACCOUNT2_PASSWORD=
IG_ACCOUNT2_API_KEY=
IG_ACCOUNT2_ACC_TYPE=LIVE

IG_ACCOUNT3_USERNAME=
IG_ACCOUNT3_PASSWORD=
IG_ACCOUNT3_API_KEY=
IG_ACCOUNT3_ACC_TYPE=LIVE

IG_ACCOUNT4_USERNAME=
IG_ACCOUNT4_PASSWORD=
IG_ACCOUNT4_API_KEY=
IG_ACCOUNT4_ACC_TYPE=LIVE

GMAIL_USER=
GMAIL_APP_PASSWORD=
GMAIL_RECIPIENTS=
GMAIL_SERVER=smtp.gmail.com
GMAIL_PORT=587
```

`GMAIL_RECIPIENTS` 支持多个收件人，用英文逗号或分号分隔：

```env
GMAIL_RECIPIENTS=user1@example.com,user2@example.com,user3@example.com
```

通常不要设置 `IG_PROFILE`，这样四个脚本会自动使用各自默认账号。只有在临时调试时，才建议设置它强制所有脚本使用同一个账号：

```env
IG_PROFILE=ACCOUNT1
```

## 服务器更新代码

如果服务器 `/igcom` 已经是这个 Git 仓库：

```bash
cd /igcom
git pull origin main
```

如果服务器 `/igcom` 不是 Git 仓库，可以先克隆到临时目录，再同步到 `/igcom`，注意保留服务器上的 `.env`：

```bash
cd /tmp
rm -rf igcom_new
git clone <你的 GitHub 仓库地址> igcom_new
rsync -av --exclude='.git' --exclude='.env' --exclude='apikey.txt' /tmp/igcom_new/ /igcom/
```

第一次部署时，把本地已填好的 `.env` 上传到服务器：

```bash
scp D:\Desktop\igg\igcom\.env root@服务器IP:/igcom/.env
```

如果已经在服务器手工创建过 `/igcom/.env`，后续更新代码时不要覆盖它。

## 定时任务命令

现有定时任务命令可以保持不变：

```bash
python3 /igcom/allMon.py
python3 /igcom/all.py
python3 /igcom/all2.py
python3 /igcom/all3.py
```

建议定时任务继续使用 `Asia/Shanghai` 时区。四个脚本会按各自默认账号读取 `/igcom/.env`。

## 手动运行与检查

服务器上可以先做语法检查：

```bash
cd /igcom
python3 -m py_compile config.py all.py all2.py all3.py allMon.py
```

确认 `.env` 配置可以正常读取：

```bash
cd /igcom
python3 -c "import config; print(config.get_ig_account('ACCOUNT1').acc_type); print(len(config.get_gmail_config().receive_usr_list))"
```

手动运行某个脚本：

```bash
cd /igcom
python3 all.py
```

## 依赖

脚本主要依赖：

- `trading_ig`
- `pandas`
- `openpyxl`
- `pytz`
- `tenacity`

如果服务器缺少依赖，可以安装：

```bash
pip3 install trading-ig pandas openpyxl pytz tenacity
```

## 安全注意事项

- `.env` 不要提交到 GitHub。
- `apikey.txt` 不要提交到 GitHub。
- Gmail 需要使用应用专用授权码，不要使用邮箱登录密码。
- 如果账号、API key 或 Gmail 授权码曾经上传到公开仓库，建议重新生成或作废旧密钥。
- 代码现在通过 `config.py` 读取配置，缺少字段时会直接报出缺失的环境变量名，方便定位问题。
