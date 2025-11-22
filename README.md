# 币安空投监控系统

一个自动监控和提醒币安交易所空投信息的Python工具。

## 功能特性

- **自动监控**: 定时获取币安官方空投数据
- **价值评估**: 自动计算空投代币的预估价值
- **分级提醒**: 根据空投价值进行高/中/普通等级别提醒
- **即将开始提醒**: 在空投开始前10分钟内发送3次连续提醒
- **状态追踪**: 监控空投信息变化（时间、数量、分数等）
- **历史记录**: 使用SQLite数据库存储空投历史
- **实时通知**: 通过Server酱发送微信/邮件提醒

## 项目结构

```
├── bianace_airdrop.py     # 主要监控程序
├── config.py              # 配置文件
├── requirements.txt       # Python依赖库
├── cron.sh                # 定时任务脚本（Linux）
├── uv_install.sh          # uv安装脚本
└── README.md             # 项目说明文档
```

## 环境要求

- Python 3.8+
- SQLite
- Linux/macOS（推荐）或 Windows

## 安装步骤

### 1. 克隆项目

```bash
git clone https://github.com/pkerXY/bianace_airdrop.git
cd bianace_airdrop
```

### 2. 安装uv
```bash
chmod +x uv_install.sh
./uv_install.sh
```

安装依赖
```bash
pip install -r requirements.txt
```

### 3. 配置通知服务

编辑 `config.py` 文件，设置你的 Server酱 API Key：

```python
# Server酱配置
SERVERCHAN_KEY = "你的Server酱Key"
```

获取 Server酱 Key：[https://sct.ftqq.com/](https://sct.ftqq.com/)

### 4. 更新Cookie（如果需要，可以不写）

在 `config.py` 中定期更新 Cookie：

```python
COOKIE = '你的Cookie'
```

## 使用方法

### 手动运行

```bash
uv python bianace_airdrop.py
```

### 设置定时任务（Linux/macOS）

使用 cron 定时执行监控脚本：

1. 给脚本添加执行权限：

```bash
chmod +x cron.sh
```

2. 编辑 crontab：

```bash
crontab -e
```

3. 添加定时任务：

```bash
./cron.sh
```

4. 查看 crontab：

```bash
crontab -e
```

## 配置说明

### 价值分级阈值

在 `config.py` 中可调整价值阈值：

```python
HIGH_VALUE_THRESHOLD = 50    # 高价值空投阈值（美元）
MEDIUM_VALUE_THRESHOLD = 10  # 中价值空投阈值（美元）
```

### 提醒配置

```python
REMINDER_3MIN = 3      # 开始前3分钟开始提醒
REMINDER_COUNT = 3     # 连续提醒3次
REMINDER_INTERVAL = 1  # 提醒间隔（秒）
```

### 日志配置

```python
LOG_FILE = "bianace.log"    # 日志文件路径
LOG_RETENTION_DAYS = 7              # 日志保留天数
```

## 通知示例

### 新空投发现

**标题:** 新空投发现：TOKEN (项目名)

**内容:**
- 日期: 2025-01-22
- 时间: 14:30
- 数量: 1000
- 代币价格: $0.50
- 预估价值: $500.00
- 类型: Binance
- 阶段: Phase 1

### 紧急提醒

当空投即将在10分钟内开始时，系统会发送3次连续提醒：

**标题:** 🚨 空投提醒 (1/3): TOKEN (项目名)

**内容:**
⏰ 空投将在 5.5 分钟后开始！
这是第 1 次提醒（共 3 次）
...

## 数据存储

- **数据库文件**: `binance_airdrop.db`
- **表结构**:
  - `airdrops`: 空投主数据表
  - `status_changes`: 状态变化记录表

## 日志文件

日志记录在 `binance_airdrop.log`，包含：
- 空投监控记录
- 通知发送记录
- 错误和警告信息

## 常见问题

### Q: Cookie过期怎么办？

A: 需要定期更新 `config.py` 中的 COOKIE 值。建议每1-2周更新一次。

### Q: 收不到通知？

A:
1. 检查 `SERVERCHAN_KEY` 是否正确
2. 检查网络连接
3. 查看日志文件确认是否有错误

### Q: 如何停止定时任务？

A: 执行 `crontab -e` 并删除对应的定时任务行。

## 注意事项

1. **Cookie有效期**: 需要定期手动更新Cookie
2. **API限制**: 避免高频请求，建议每3-5分钟检查一次
3. **通知限制**: Server酱免费版有每日调用次数限制
4. **风险提醒**: 空投信息仅供参考，不构成投资建议

## 更新日志

### v1.0.0 (2025-01-22)
- 初始版本
- 支持空投自动监控
- 支持价值计算和分级提醒
- 支持即将到来的空投提醒

## 待办列表

- [ ] 支持多个通知渠道（Server酱、Telegram等）
- [ ] Web管理界面
- [ ] 历史空投数据分析
- [ ] Cookie自动更新
- [ ] 空投完成结果追踪

## 许可证

MIT License

## 贡献

欢迎提交 Issue 和 Pull Request！

## 免责声明

本项目仅供学习交流使用，使用者应自行承担使用风险。空投市场波动剧烈，请谨慎对待任何空投投资。

## 联系方式

- GitHub Issues: [https://github.com/pkerXY/bianace_airdrop/issues](https://github.com/pkerXY/bianace_airdrop/issues)
