# CloudFront Abuse Detection System

AWS Organizations 多账号 CloudFront 流量异常检测系统，基于 Lambda 无服务器架构，通过 Telegram 实时告警。

## 功能特性

- **多账号监控**: 自动扫描 AWS Organizations 中所有活跃账号的 CloudFront 分发
- **分层告警机制**: 
  - Critical (3x): 流量超过历史平均 3 倍，立即告警
  - Warning (2x): 流量超过历史平均 2 倍，持续 30 分钟后告警
- **双阈值验证**: 相对倍数 + 绝对显著性，避免误报
- **并行处理**: Scheduler + Worker 架构，支持 270+ 账号并行处理
- **Telegram 告警**: 中英双语告警模板，支持去重

## 架构

```
EventBridge (每15分钟)
    │
    ▼
Scheduler Lambda ──► 获取账号列表 ──► 分组调用 Worker
    │
    ▼
Worker Lambda (并行) ──► 处理账号 ──► 检测滥用 ──► Telegram 告警
```

## 快速部署

### 1. 打包

```bash
bash scripts/package.sh
```

### 2. 上传到 S3

```bash
aws s3 cp deployment.zip s3://your-bucket/lambda/deployment.zip
```

### 3. 部署 CloudFormation

```bash
aws cloudformation deploy \
  --template-file cloudformation/template.yaml \
  --stack-name cloudfront-abuse-detection \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    TelegramBotToken=YOUR_BOT_TOKEN \
    TelegramChatId=YOUR_CHAT_ID \
    S3Bucket=your-bucket \
    S3Key=lambda/deployment.zip
```

## 配置参数

| 参数 | 默认值 | 说明 |
|-----|-------|------|
| AbuseMultiplier | 3 | Critical 告警倍数阈值 |
| WarningMultiplier | 2 | Warning 告警倍数阈值 |
| DurationThreshold | 1 | Critical 连续检测次数 |
| WarningDurationThreshold | 2 | Warning 连续检测次数 (30分钟) |
| WorkerConcurrency | 10 | Worker 并发数 |
| AccountsPerWorker | 20 | 每个 Worker 处理的账号数 |
| ScheduleExpression | rate(15 minutes) | 执行频率 |
| AbsoluteRequestThreshold | 250000 | 最小请求数阈值 (15分钟) |
| AbsoluteBytesThreshold | 2684354560 | 最小字节数阈值 (2.5GB/15分钟) |

## 告警示例

```
⚠️ Payer 282225226836 ⚠️
🔴 Critical Alert 🔴
⚠️ 以下Amazon CloudFront分配疑似被盗刷（流量异常） ⚠️

#CDN盗刷 #流量异常 #紧急告警 #AWS #CloudFront

帐号ID | Account ID : 269222222866
帐号名称 | Account Name : ANS01
帐号电邮 | Account Email : example@gmail.com
分配 | Distribution : E23JQVRSK3EZF7

当前15分钟 | Current 15 min : 409.56 GB
过去24小时平均 | Past 24h average : 110.23 GB
滥用阈值 | Abuse Threshold : 330.68 GB (3.0x)
连续超标 | Consecutive Count : 2 次

Tue, 03 Feb 2026 14:54:48 +0800
```

## 项目结构

```
├── cloudformation/template.yaml   # CloudFormation 模板
├── scripts/
│   ├── package.sh                 # 打包脚本
│   └── deploy.sh                  # 部署脚本
├── src/                           # 核心模块
├── tests/                         # 测试文件
├── scheduler_handler.py           # Scheduler Lambda 入口
├── worker_handler.py              # Worker Lambda 入口
└── requirements-prod.txt          # 生产依赖
```

## 成本估算

基于默认配置（每 15 分钟运行，10 个 Worker）：约 $10-20/月

## License

MIT
