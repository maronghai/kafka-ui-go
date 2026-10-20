# kafka-ui-go

单二进制的 Kafka Web UI：服务端渲染 + `embed` 静态资源，零 Node 依赖，基于 [segmentio/kafka-go](https://github.com/segmentio/kafka-go)。界面为中文。

## 特性

- **集群概览**：cluster id、controller、broker 列表、topic / partition / consumer group 数量
- **Topic 列表**：分区数、副本因子、under-replicated 标记、内部 topic 过滤
- **Topic 详情**：逐分区 leader / replicas / ISR / offline 拓扑、log start / end offset、topic 配置（敏感值脱敏）
- **消息浏览**：任选分区，从最早 / 最新 / 指定 offset 起读取（单次上限 1000 条）；key / value / headers 全量展示；payload 自动 JSON 缩进，非 UTF-8 退化为 hex；超大内容截断到 128KB
- **发送消息**：指定 key / value / header，分区可自动（key 哈希或轮询）或手动指定，发送后跳转回消息页并高亮落点
- **Consumer Group**：列表按总 lag 降序；详情含成员与分区分配、逐分区 committed / end offset / lag
- **工程性**：`/healthz` 探活、SIGINT/SIGTERM 优雅关闭、纯 Go 交叉编译、32 位平台（windows/386）兼容

## 快速开始

要求 Go 1.24+，无需其他运行时依赖。

```bash
go run .                                  # 默认监听 :8080，连 localhost:9092
go build -o kafka-ui-go .                 # 构建单二进制
./kafka-ui-go -brokers kafka1:9092,kafka2:9092 -addr :8080
```

启动时会先 ping 集群：连接失败只打警告，不阻止启动（页面上会显示错误提示）。

## 配置

命令行参数优先，环境变量作为默认值：

| 参数 | 环境变量 | 默认值 | 说明 |
|---|---|---|---|
| `-brokers` | `KAFKA_BROKERS` | `localhost:9092` | 逗号分隔的 bootstrap broker 地址 |
| `-addr` | `KAFKA_UI_ADDR` | `:8080` | HTTP 监听地址 |
| `-timeout` | `KAFKA_UI_TIMEOUT` | `10s` | 单次 Kafka 请求超时 |
| `-h` | — | — | 查看帮助 |

## 路由

| 路径 | 说明 |
|---|---|
| `/` | 集群概览 |
| `/healthz` | 探活（文本 `ok`） |
| `/topics` | Topic 列表，`?q=` 名称过滤 |
| `/topics/{topic}` | Topic 详情（分区拓扑 + 配置） |
| `/topics/{topic}/messages` | 消息浏览，`?partition=&offset=&count=` |
| `/topics/{topic}/produce` | 发送消息（GET 表单 / POST 提交） |
| `/groups` | Consumer group 列表 |
| `/groups/{group}` | Group 详情（成员 + 逐分区 lag） |

消息浏览的 `offset` 取值：`earliest`、`latest`、或绝对 offset 数字；读到 log end 即返回（不做尾部跟随阻塞），页面给出「继续向后读取」链接。

## 架构

```
main.go                 入口：flag 解析、HTTP server、优雅关闭
internal/config         配置（flag + 环境变量）
internal/kafka          Kafka 访问层（只读查询 + 单条发送），面向页面的数据结构
internal/web            路由 / handler / 模板 / 样式，模板与 CSS 经 embed 打入二进制
```

消息读取按已知的 log end offset 设界，读满边界立即返回；reader 的 fetch `MaxWait` 保持很小（25ms），避免 `Close()` 等待后台长轮询拖慢每次请求。点击浏览页的典型耗时在本机集群上为几十毫秒级。

## 测试

```bash
go test ./... -count=1
```

- 单元测试不依赖 Kafka，全部可离线运行。
- 集成测试（如 `TestBrowseIntegrationStopsAtLogEnd`）在 `localhost:9092`（或 `KAFKA_BROKERS` 首地址）可达时启用，否则自动 Skip。
- 可用 `KAFKA_UI_TEST_TOPIC` 指定集成测试读取的 topic，默认取第一个非内部 topic。

## 说明

- 本工具面向开发 / 排查场景，**不带鉴权**；暴露到非受信网络前请自行加反向代理或访问控制。
- 除发送单条消息外均为只读操作，不会修改 topic 配置或 group 状态。
