# Server AnyTLS Agent

一个基于 AnyTLS 协议的 Agent 节点实现，专为 v2Board 面板设计。本项目是 v2Board 的 AnyTLS 节点代理，提供了完整的 TLS 加密支持和用户管理功能。


## 主要特性

- 🚀 高性能 AnyTLS 协议支持
- 🔒 完整的 TLS 加密实现
- 👥 自动用户管理与同步
- 📊 实时流量统计与上报
- 🔄 自动心跳检测
- 📝 可配置的多级日志系统
- 🔐 灵活的证书管理
- ⚡ 支持 gRPC 通信

## 系统要求

- Go 1.16 或更高版本
- 支持的操作系统：Linux, macOS, Windows
- 至少 512MB RAM
- 稳定的网络连接

## 快速开始

### 安装

```bash
# 使用 go install 安装
go install github.com/xflash-panda/server-anytls/cmd/server-agent@latest

# 或从源码编译
git clone https://github.com/xflash-panda/server-anytls.git
cd server-anytls
go build -o anytls-agent cmd/server-agent/main.go
```

### 运行

#### 使用环境变量

```bash
# 设置必要的环境变量
export X_PANDA_ANYTLS_TOKEN="your-token"  # v2Board 面板分配的节点 token
export X_PANDA_ANYTLS_NODE="your-node-id"  # v2Board 面板分配的节点 ID
export X_PANDA_ANYTLS_SERVER_AGENT_HOST="127.0.0.1"
export X_PANDA_ANYTLS_SERVER_AGENT_PORT="8082"
export X_PANDA_ANYTLS_LOG_LEVEL="info"

# 运行服务
anytls-agent
```

#### 使用命令行参数

```bash
anytls-agent --token="your-token" --node="your-node-id" --server_host="127.0.0.1" --port=8082
```

## 配置说明

### 环境变量

| 环境变量 | 说明 | 默认值 | 是否必需 |
|---------|------|--------|----------|
| X_PANDA_ANYTLS_TOKEN | v2Board 面板分配的节点 token | - | 是 |
| X_PANDA_ANYTLS_NODE | v2Board 面板分配的节点 ID | - | 是 |
| X_PANDA_ANYTLS_SERVER_AGENT_HOST | 服务器主机地址 | 127.0.0.1 | 否 |
| X_PANDA_ANYTLS_SERVER_AGENT_PORT | 服务器端口 | 8082 | 否 |
| X_PANDA_ANYTLS_CERT_FILE | 证书文件路径 | /root/.cert/server.crt | 否 |
| X_PANDA_ANYTLS_KEY_FILE | 密钥文件路径 | /root/.cert/server.key | 否 |
| X_PANDA_ANYTLS_LOG_LEVEL | 日志级别 | error | 否 |
| X_PANDA_ANYTLS_FETCH_USER_INTERVAL | 用户同步间隔(秒) | 60 | 否 |
| X_PANDA_ANYTLS_REPORT_TRAFFICS_INTERVAL | 流量上报间隔(秒) | 80 | 否 |
| X_PANDA_ANYTLS_HEARTBEAT_INTERVAL | 心跳检测间隔(秒) | 60 | 否 |

### 命令行参数

所有环境变量都可以通过命令行参数设置，使用 `--help` 查看完整参数列表：

```bash
anytls-agent --help
```

## 日志级别

- `debug`: 调试模式，显示所有日志信息
- `info`: 信息模式，显示重要操作日志
- `error`: 错误模式，仅显示错误日志

## 开发指南

### 项目结构

```
server-anytls/
├── cmd/
│   └── server-agent/    # Agent 主程序入口
├── internal/
│   ├── app/            # 应用层代码
│   └── pkg/            # 内部包
├── pkg/                # 公共包
└── README.md
```

### 本地开发

```bash
# 克隆仓库
git clone https://github.com/xflash-panda/server-anytls.git

# 安装依赖
cd server-anytls
go mod download

# 运行测试
go test ./...

# 构建
go build -o anytls-agent cmd/server-agent/main.go
```

## 故障排除

1. 如果遇到连接问题，请检查：
   - 网络连接是否正常
   - 服务器地址和端口是否正确
   - 防火墙设置是否允许相应端口
   - v2Board 面板是否正常运行

2. 如果遇到证书问题，请确保：
   - 证书文件路径正确
   - 证书文件权限正确
   - 证书未过期

3. 如果遇到节点认证问题，请确保：
   - Token 正确
   - 节点 ID 正确
   - 节点在 v2Board 面板中状态正常

## 贡献指南

欢迎提交 Issue 和 Pull Request！在提交 PR 前，请确保：

1. 代码经过测试
2. 遵循现有的代码风格
3. 更新相关文档
4. 添加必要的测试用例

## 许可证

本项目采用 MIT 许可证 - 详见 [LICENSE](LICENSE) 文件

## 联系方式

如有问题或建议，请通过以下方式联系我们：

- 提交 Issue
- 发送邮件至项目维护者