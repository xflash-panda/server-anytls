# Server AnyTLS

一个基于 AnyTLS 协议的服务器实现，支持 v2Board 面板。

## 功能特性

- 支持 AnyTLS 协议
- 与 v2Board 面板集成
- 自动用户管理
- 流量统计
- 支持 TLS 加密
- 支持多种日志级别
- 支持自动证书管理

## 快速开始

```bash
# 安装
go install github.com/xflash-panda/server-anytls/cmd/server@latest

# 运行（使用环境变量）
export X_PANDA_ANYTLS_API="https://your-api-server"
export X_PANDA_ANYTLS_TOKEN="your-token"
export X_PANDA_ANYTLS_NODE="your-node-id"
anytls-node

# 或者使用命令行参数
anytls-node --api="https://your-api-server" --token="your-token" --node="your-node-id"
```

## 配置

### 环境变量

| 环境变量 | 说明 | 默认值 |
|---------|------|--------|
| X_PANDA_ANYTLS_API | 服务器地址 | - |
| X_PANDA_ANYTLS_TOKEN | API Token | - |
| X_PANDA_ANYTLS_NODE | 节点 ID | - |
| X_PANDA_ANYTLS_CERT_FILE | 证书文件路径 | /root/.cert/server.crt |
| X_PANDA_ANYTLS_KEY_FILE | 密钥文件路径 | /root/.cert/server.key |
| X_PANDA_ANYTLS_LOG_LEVEL | 日志级别 | error |

### 命令行参数

```bash
anytls-node --help
```

主要参数：
- `--api`: 服务器地址
- `--token`: API Token
- `--node`: 节点 ID
- `--cert_file`: 证书文件路径
- `--key_file`: 密钥文件路径
- `--log_mode`: 日志级别 (debug/info/error)
- `--fetch_users_interval`: 获取用户列表间隔（秒）
- `--report_traffics_interval`: 上报流量间隔（秒）

## 开发

```bash
# 克隆仓库
git clone https://github.com/xflash-panda/server-anytls.git

# 进入目录
cd server-anytls

# 安装依赖
go mod download

# 编译
go build -o anytls-node cmd/server/main.go
```

## 贡献

欢迎提交 Issue 和 Pull Request！

## 许可证

本项目采用 MIT 许可证 - 详见 [LICENSE](LICENSE) 文件