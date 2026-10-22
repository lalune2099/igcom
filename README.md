# IGCom 数据抓取与变化率表格自动化

这个仓库用于在服务器定时抓取 IG 金融市场数据，更新变化率 Excel 模板，并按需通过 Gmail 发送结果附件。

## 本次更新（2026-10-22）

- 修复请求时区：根据登录会话的账号时区转换请求范围，返回数据统一使用伦敦时间。
- 修复增量覆盖：按“时间＋EPIC＋Resolution”去重，有效新数据优先覆盖旧数据，无效新值不覆盖有效旧值。
- 明确变化率表取数：05、07、15、19、20使用1h；18:00与18:30使用30Min并交换源时间。
- 修复月度详细版混用周期的问题：只读取30Min记录，保留原始时间，不交换18:00与18:30。
- 增加合并前原始价格、请求状态及新旧价格对照，缺少指定源数据时留空并警告。
- 验证通过：12项离线测试；使用已保存的API原始数据完成96次解析检查、384个日表单元格检查和12个详细表记录检查。上述离线验证不调用API或发送邮件。
- 本次不处理SPREADBET与CFD账号的报价单位差异、KO产品扩展、冬令时定时安排，也不自动修正历史归档表格。

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
| `allMon.py` | `ACCOUNT3` | 周二主流程脚本，与 `all3.py` 共用第 3 个 IG 账号 |

四个脚本沿用各自入口和默认账号；设置 `IG_PROFILE` 时会统一覆盖默认账号选择。

## 数据周期与增量更新

所有报表时间均为伦敦时间，Close 为买卖价平均值，买价或卖价缺失时不计算有效 Close。

| 报表 | 使用的记录 |
| --- | --- |
| 每日变化率表、月度公式版 | 05、07、15、19、20 使用 `1h`；表格18:00读取原始 `30Min` 的18:30，表格18:30读取原始 `30Min` 的18:00 |
| 月度详细版 | 所有时间只使用 `30Min`，按原始伦敦时间填写，不交换18:00和18:30 |

四个主脚本在登录后读取会话的 `timezoneOffset`，将北京时间的抓取范围转换成账号对应的 API 请求时间。返回记录优先按 `snapshotTimeUTC` 转为伦敦时间；没有 UTC 字段时才使用账号时区解释 `snapshotTime`。如果登录结果没有有效的时区偏移，会停止并报错，避免猜测时间。

当天首次运行执行原有全量抓取；后续运行复用当天最近一份结果，重新抓取最近3小时。合并先按“伦敦时间＋EPIC＋Resolution”去重，保留有效新值，再按时间排序。无效新价格不会覆盖有效旧值。

小时线在收盘前可能继续变化，后续运行会用本次有效记录覆盖之前的值。代码不会因为写入过一次就锁定价格。

每次运行仍保存在 `outputs/historical_data_YYYYMMDD_HHMMSS/`，原有 CSV、Excel 文件名和邮件附件保持原样，额外保存：

| 新增文件 | 用途 |
| --- | --- |
| `api_raw/request_metadata.json` | 账号时区偏移、请求边界和对应 UTC 时间，不包含登录凭据 |
| `api_raw/<EPIC>_1h.json`、`api_raw/<EPIC>_30Min.json` | 当次 API 返回的原始价格列表，合并前保留；请求失败、未返回价格时可能没有该文件 |
| `api_raw/fetch_status.json` | 每个品种、周期的成功、空数据、部分无效或失败状态 |
| `Fetched_Products_1h_30min.csv` | 仅本次抓取到的有效数据，不含上次运行的数据 |
| `Merge_price_audit.csv` | 增量运行的新旧价格对照，标记新增、刷新、历史沿用和请求范围内未刷新 |

日志会打印合并前伦敦20:00的 Close，便于核对。部分请求失败时会警告并记录，历史数据可能继续沿用；全部增量请求没有有效数据时会停止，不生成或发送只有旧数据的报表。填表缺少所需周期或交换后的源时间时，对应单元格留空并警告，不改用其他周期或相邻时间。

这些修复不自动修正已经归档的旧价格。月度详细版可由已有原始表重新生成，但旧原始表中未成功刷新的价格仍需要独立核查。定时任务的执行时间不由这些修复调整。

本轮修复不转换报价单位：`ACC_TYPE=LIVE` 仅指定正式环境，不代表登录后的交易账户一定是 CFD。实测英国 `SPREADBET` 与台湾 `CFD` 账户对相同外汇 EPIC 返回的价格可能相差100倍或10000倍，当前代码仍保留 API 返回的报价尺度。混用这两类账户生成或累积外汇报表前，需要另外核实并适配账户类型和报价单位；时区转换不解决该差异。

## 目录与文件

| 文件 | 说明 |
| --- | --- |
| `config.py` | 统一读取 IG 账号、API key、Gmail 授权码和收件人 |
| `.env.example` | 环境变量模板，不包含真实密钥 |
| `.gitignore` | 忽略 `.env`、`apikey.txt`、缓存和运行输出 |
| `IG变化率表格(英区).xlsx` | 原始 Excel 模板 |
| `all.py` / `all2.py` / `all3.py` / `allMon.py` | 主要定时运行脚本 |
| `combine.py`、`for1day.py`、`live.py`、`onlyfor3.py`、`send.py`、`test.py` | 备用或历史辅助脚本，已改为从 `config.py` 读取配置 |
| `cumulative_runs.py` | 共用的请求时区转换、原始价格留存及增量合并 |
| `monthly_report.py` | 累积每日变化率表，生成月度公式版 |
| `detailed_monthly_report.py` | 从原始数据的30Min记录生成月度详细版 |
| `combined_monthly_reports.py` | 生成两份月报并在同一邮件中发送 |

运行时会生成这些文件或目录，不需要提交到 GitHub：

| 运行输出 | 说明 |
| --- | --- |
| `outputs/historical_data_*/` | 每次运行的归档目录，保存抓取数据和结果表 |
| `outputs/historical_data_*/IG变化率_模版更新_YYYYMMDD.xlsx` | 日期更新后的模板 |
| `outputs/historical_data_*/IG变化率_YYYYMMDD.xlsx` | 最终填好数据的结果表 |
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
python3 -m py_compile config.py cumulative_runs.py all.py all2.py all3.py allMon.py detailed_monthly_report.py
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
