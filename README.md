# 市场数据自动抓取系统

自动从 Yahoo Finance 和 FRED 抓取市场数据，保存到 CSV 文件并推送到 Google Sheets。

## 功能特性

- 📊 抓取主要股票指数（标普500、道琼斯、纳指、欧洲指数等）
- 💱 抓取主要货币对汇率
- 📈 抓取美国国债收益率（10Y、2Y）并计算利差
- 🛢️ 抓取大宗商品价格（原油、黄金、铜等）
- 💾 自动保存到 CSV 文件（`data/latest.csv` 和 `data/history.csv`）
- 📋 自动推送到 Google Sheets（可选）
- ⏰ GitHub Actions 定时自动运行

## 快速开始

### 1. 本地运行

```bash
# 安装依赖
pip install -r requirements.txt

# 运行脚本
python fetch_markets.py

# 如果 DXY 符号不可用，使用备用符号
python fetch_markets.py --alt-dxy
```

### 2. 设置 Google Sheets（可选）

#### 步骤 1：创建 Google Cloud 项目和服务账号

1. 访问 [Google Cloud Console](https://console.cloud.google.com/)
2. 创建新项目或选择现有项目
3. 启用 Google Sheets API：
   - 导航到「API 和服务」>「库」
   - 搜索「Google Sheets API」并启用
4. 创建服务账号：
   - 导航到「API 和服务」>「凭据」
   - 点击「创建凭据」>「服务账号」
   - 填写名称（如 `market-data-bot`）并创建
   - 点击创建的服务账号，进入「密钥」标签页
   - 点击「添加密钥」>「创建新密钥」> 选择「JSON」格式
   - 下载 JSON 文件（**妥善保管，不要提交到 Git**）

#### 步骤 2：创建 Google Sheets 并共享

1. 创建新的 Google Sheets 文档
2. 从 URL 中获取 Spreadsheet ID：
   - URL 格式：`https://docs.google.com/spreadsheets/d/SPREADSHEET_ID_HERE/edit`
   - 例如：如果 URL 是 `https://docs.google.com/spreadsheets/d/1Oo_QohM60v7CxEO8KqLGweb8S6SYSse0xsFbrA-KimA/edit`
   - 那么 Spreadsheet ID 就是：`1Oo_QohM60v7CxEO8KqLGweb8S6SYSse0xsFbrA-KimA`
3. 将服务账号的邮箱地址（在 JSON 文件中，格式类似 `xxx@xxx.iam.gserviceaccount.com`）添加到 Google Sheets 的「共享」列表，并授予「编辑者」权限

#### 步骤 3：配置 GitHub Secrets（用于 GitHub Actions）

**使用 Repository secrets（推荐）**

对于这个项目，使用 **Repository secrets** 更简单直接，因为所有工作流都需要相同的凭证。

1. 在 GitHub 仓库中，进入「Settings」>「Secrets and variables」>「Actions」
2. 点击「New repository secret」添加以下两个 Secret：
   - **Name**: `GOOGLE_SHEETS_CREDENTIALS_JSON`
     **Value**: 将下载的 JSON 文件内容**完整复制**粘贴到这里（注意：需要将整个 JSON 作为一行粘贴，或保持换行格式都可以）
   - **Name**: `GOOGLE_SHEETS_SPREADSHEET_ID`
     **Value**: 粘贴你的 Spreadsheet ID（例如：`1Oo_QohM60v7CxEO8KqLGweb8S6SYSse0xsFbrA-KimA`）

**Repository secrets vs Environment secrets：**
- **Repository secrets**（推荐）：适用于整个仓库的所有工作流，设置简单，适合本项目
- **Environment secrets**：需要先创建环境，然后配置 secrets，适合需要多环境隔离的复杂场景

#### 步骤 4：本地测试（可选）

如果你想在本地测试 Google Sheets 功能：

```bash
# 方法 1：使用环境变量
export GOOGLE_SHEETS_CREDENTIALS_JSON='{"type":"service_account",...}'  # 完整 JSON 内容
export GOOGLE_SHEETS_SPREADSHEET_ID='your-spreadsheet-id'
python fetch_markets.py

# 方法 2：使用 .env 文件（需要安装 python-dotenv）
# 将 JSON 内容保存为一行（转义引号），或使用 base64 编码
```

### 3. 设置 GitHub Actions 自动运行

1. 将代码推送到 GitHub 仓库
2. 在 GitHub 仓库中，进入「Actions」标签页
3. 首次运行需要启用工作流（点击「I understand my workflows, go ahead and enable them」）
4. 可以手动触发：点击「Fetch Daily Markets」>「Run workflow」

#### 调整运行时间

编辑 `.github/workflows/markets.yml` 中的 `cron` 表达式：

```yaml
schedule:
  - cron: "5 23 * * *"   # UTC 23:05，每天
```

Cron 格式：`分 时 日 月 星期`

示例：
- `"5 23 * * *"` - 每天 UTC 23:05
- `"0 0 * * *"` - 每天 UTC 00:00（午夜）
- `"0 12 * * 1-5"` - 每周一到周五 UTC 12:00

时区换算：
- 洛杉矶（PST/PDT，UTC-8/-7）：UTC 23:05 = 洛杉矶 15:05/16:05
- 北京（CST，UTC+8）：UTC 23:05 = 北京次日 07:05

## 数据结构

### CSV 文件格式

两个 CSV 文件具有相同的结构：

| 列名 | 说明 |
|------|------|
| `timestamp_utc` | 数据抓取时间（ISO 8601 格式，UTC） |
| `category` | 分类（INDEX/FX/COMMOD、BOND_YIELD） |
| `name` | 资产名称 |
| `symbol` | 交易代码 |
| `value` | 当前值 |
| `prev_close` | 前一交易日收盘价 |
| `change_pct` | 涨跌幅（百分比） |
| `unit` | 单位（%、bp 等） |
| `source` | 数据源 |

### Google Sheets 结构

脚本会自动创建/更新两个工作表：
- **History**: 累计历史数据（追加模式）
- **Latest**: 最新快照（覆盖模式）

## 自定义抓取标的

编辑 `fetch_markets.py` 中的配置：

```python
# 添加更多指数
YF_TICKERS = {
    "Nikkei 225": "^N225",
    "Hang Seng": "^HSI",
    "CSI 300": "^CSI300",
    # ... 更多标的
}

# 添加更多 FRED 数据
FRED_SERIES = {
    "US 30Y Yield": "DGS30",
    # ... 更多系列
}
```

## 故障排除

### 问题：DXY 数据获取失败

```bash
# 使用备用符号
python fetch_markets.py --alt-dxy
```

或修改代码中的 `YF_TICKERS`，将 `"^DXY"` 改为 `"DX-Y.NYB"`。

### 问题：Google Sheets 推送失败

1. 检查服务账号 JSON 是否正确配置
2. 确认服务账号邮箱已添加到 Google Sheets 共享列表
3. 确认 Google Sheets API 已启用
4. 查看 GitHub Actions 日志中的错误信息

### 问题：某些数据获取失败

- yfinance 偶尔会有短时波动，脚本已做了错误兜底
- 失败的数据会在 `source` 字段中记录错误信息
- 可以稍后重试或检查网络连接

## 许可证

MIT License

