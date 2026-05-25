# cmdgo — Command Code Go Tier 反代设计

> 一个把 Command Code Go 套餐（$1/mo, $10 credits, 开源模型 only）通过 OpenAI/Anthropic 兼容 API 暴露给任意客户端的反代。Go 单二进制 + 嵌入式 web dashboard。

---

## 1. 项目目标

- **场景**：Command Code 的 Go 套餐只在 CLI（cmd）/Studio 里用，没有原生 API；但通过 OAuth 流程能拿到 `user_...` apikey，调用 `/alpha/generate` 即可。
- **范围**：仅做 Go 套餐反代。Pro/Max/Ultra 用户应该用 CC 官方 API，不在此项目范围。
- **目标客户端**：Claude Code、Cursor、Cline、OpenWebUI、LibreChat 等任何支持 OpenAI 或 Anthropic 协议的工具。

## 2. 决策总览

| 维度 | 选择 |
|---|---|
| 后端语言 | Go 1.25+（推荐 Go 1.26） |
| HTTP 框架 | 标准库 `net/http`（1.22+ ServeMux 路由够用） |
| 存储 | 单 JSON 文件 `~/.cmdgo/state.json`（原子写） |
| 前端 | HTMX 2.0.10 + Alpine.js 3.15.12 + Tailwind v4 CDN（embed.FS 打包） |
| 鉴权 | 单 `pcc_xxx` token |
| 协议 | OpenAI Chat Completions + Anthropic Messages（都做） |
| 部署 | 本地 listen 127.0.0.1（默认）+ VPS 0.0.0.0 + paste-key 兜底 |
| 仓库 | 新建 `cmdgo` 独立 repo |

外部依赖：`github.com/tidwall/gjson`、`github.com/tidwall/sjson`。其余全标准库。预计二进制 ~12MB。

## 3. 目录结构

```
cmdgo/
├── go.mod
├── main.go
├── internal/
│   ├── config/        # CLI flags, env, defaults
│   ├── store/         # JSON file 读写（互斥锁，tmp+rename 原子写）
│   ├── cc/            # Command Code upstream client
│   │   ├── client.go      # /alpha/generate + SSE
│   │   ├── account.go     # /alpha/whoami /usage/summary /billing/credits
│   │   ├── auth.go        # OAuth state + callback
│   │   ├── models.go      # Go-tier 12 模型 whitelist
│   │   └── types.go
│   ├── pool/          # 多账号池 + affinity 路由 + 健康判定
│   ├── proxy/         # 反代核心 + 协议适配
│   │   ├── proxy.go       # canonical runner + cache 注入 + 重试
│   │   ├── openai.go      # /v1/chat/completions 进出
│   │   ├── anthropic.go   # /v1/messages 进出
│   │   ├── sse.go         # 行解析 + flush 转发
│   │   └── retry.go
│   ├── server/        # routes, middleware, handlers
│   └── web/           # embed.FS templates + static
│       ├── templates/
│       │   ├── layout.html
│       │   ├── dashboard.html
│       │   └── partials/{account_card,traffic_row,endpoints}.html
│       └── static/styles.css
├── docs/
│   ├── cmdgo-plan.md
│   └── cc-api-endpoints.md
└── README.md
```

## 4. 数据模型（`~/.cmdgo/state.json`）

```json
{
  "version": 1,
  "proxyToken": "pcc_aB3xR9...",
  "settings": {
    "routing": "affinity",
    "minCreditsUsd": 0.5,
    "maxErrorRate5min": 0.20,
    "creditPollSec": 60,
    "trafficLogMax": 500,
    "mergeReasoningIntoContent": false
  },
  "accounts": [
    {
      "id": "6d37ae30-27ec-...",
      "name": "Hanwen Yu",
      "email": "...",
      "userName": "waylon256yhw",
      "apiKey": "user_...",
      "addedAt": "...",
      "lastUsedAt": "...",
      "paused": false,
      "lastKnownCredits": 9.9367,
      "lastKnownCreditsAt": "..."
    }
  ],
  "trafficLog": [
    {
      "ts": "...",
      "accountId": "...",
      "protocol": "openai|anthropic",
      "model": "deepseek/deepseek-v4-pro",
      "status": 200,
      "inputTokens": 7424,
      "cacheReadTokens": 7424,
      "cacheWriteTokens": 0,
      "outputTokens": 128,
      "costUsd": 0.0001,
      "durationMs": 1300,
      "retried": false,
      "errorCode": ""
    }
  ]
}
```

- `trafficLog` 滑窗保留最近 500 条
- 写入用 `tmp file + rename` 保证原子性
- 内存维护「最近 5 分钟错误率」「每账号 QPS」用于路由判定

## 5. HTTP API

### 反代 API（`Authorization: Bearer pcc_xxx`）

| Method | Path | 说明 |
|---|---|---|
| POST | `/v1/chat/completions` | OpenAI 兼容 |
| POST | `/v1/messages` | Anthropic 兼容 |
| GET | `/v1/models` | Go-tier 12 模型清单（OpenAI 风格 list） |

### Dashboard / 控制面 API

| Method | Path | 说明 |
|---|---|---|
| GET | `/` | dashboard.html |
| GET | `/api/accounts` | HTMX partial（账号卡片） |
| GET | `/api/traffic` | HTMX partial（流量日志） |
| POST | `/api/accounts/{id}/pause` | 暂停账号 |
| POST | `/api/accounts/{id}/resume` | 恢复账号 |
| DELETE | `/api/accounts/{id}` | 移除账号 |
| POST | `/api/oauth/start` | 生成 state + auth URL |
| POST | `/api/oauth/paste-key` | VPS 模式手动粘贴 |
| POST | `/api/proxy-token/rotate` | 轮换 pcc_xxx |
| GET | `/api/events` | SSE 推送（账号增删、credit 变化、流量更新） |

### OAuth 公开端点

| Method | Path | 说明 |
|---|---|---|
| POST | `/callback` | CC Studio POST 目标 |
| OPTIONS | `/callback` | CORS preflight + Private Network Access |

## 6. OAuth 流程

### 本地模式（默认）

反代 listen `127.0.0.1:8080` → callback URL 用 `http://localhost:8080/callback`。

```
Browser                       Proxy                          CC Studio
   │ click [+ Add Account]      │                              │
   ├───────────────────────────►│                              │
   │                            │ gen state + auth URL         │
   │ ◄──────────────────────────┤                              │
   │ open new tab               │                              │
   ├────────────────────────────┼────────────────────────────►│
   │ login + grant              │                              │
   │                            │ POST /callback (apikey,state)│
   │                            │ ◄────────────────────────────┤
   │                            │ verify state, save key       │
   │                            │ SSE push: "account ✓"        │
   │ ◄──────────────────────────┤                              │
   │ dashboard 自动刷新卡片                                    │
```

### VPS 模式（公网部署）

反代 listen `0.0.0.0:8080`，OAuth 走 paste-key 兜底：

1. dashboard 弹 modal："Open `commandcode.ai/studio/auth/cli?...` in browser, copy your apikey, paste below"
2. textarea + Submit → 反代调 `/alpha/whoami` 验证 + `/alpha/billing/credits` 读初始 credits → 加入 accounts
3. 不依赖 CC Studio POST，跨网络/防火墙都能用

CORS / PNA：参考 `pi-commandcode-provider/src/auth-server.ts` 的实现，CORS 允许 `https://commandcode.ai`，PNA 设 `Access-Control-Allow-Private-Network: true`。

## 7. 反代核心数据流

```
client req
  │
  ├─► (1) middleware: auth pcc_xxx, rate limit, slog
  │
  ├─► (2) protocol adapter (openai.go | anthropic.go)
  │       ├─ decode JSON
  │       ├─ extract { model, messages, tools, system, max_tokens, ... }
  │       ├─ validate model ∈ Go-tier whitelist (否则 400)
  │       └─ produce canonical request
  │
  ├─► (3) pool.Pick(clientToken, canonical) → *Account
  │       hash(clientToken + model + prefix(messages[0..-2])) % healthy[]
  │       skip paused / unhealthy / credits<threshold
  │
  ├─► (4) proxy.Run(account, canonical)
  │       ├─ build CC body
  │       │     params.cache_control = {type:"ephemeral"}  ← 一行
  │       │     x-session-id = hash(clientToken + model + prefix)
  │       ├─ POST /alpha/generate streaming
  │       ├─ on retryable err BEFORE first flush:
  │       │     retry up to 2x, new SID, maybe alt account
  │       └─ on err AFTER first flush:
  │             propagate to client, no retry
  │
  ├─► (5) protocol adapter: encode CC SSE → out SSE
  │       openai:    chat.completion.chunk
  │       anthropic: message_start / content_block_* / message_delta / message_stop
  │
  └─► (6) defer: traffic log + lastUsedAt + 异步 broadcast SSE 给 dashboard
```

## 8. Cache 策略（最简）

每个 CC 请求 body 加一行：

```go
ccBody := map[string]any{
    "config": cfg,
    "memory": "", "taste": "", "skills": nil, "permissionMode": "standard",
    "params": map[string]any{
        "model":         model,
        "messages":      messages,
        "tools":         tools,
        "system":        system,
        "max_tokens":    maxTokens,
        "stream":        true,
        "cache_control": map[string]string{"type": "ephemeral"}, // ← 全部 caching
    },
}
```

session_id 稳定算法：

```go
sid := sha256(clientToken + "|" + model + "|" + jsonPrefix(messages[:len-1]))[:32]
```

**已验证**：
- `cache_control` 字段被 CC 透传到上游
- 配合 cache_control 后，Kimi/GLM/Qwen 等"非自动 cache"模型也能命中
- DeepSeek 系列自动 cache（无需 hint）
- MiniMax 系列无论如何都不 cache
- **Cache 是 `(model, content_hash)` 维度，跨 session_id 仍可命中** —— 意味着多账号轮换不影响 cache

无需在 messages 内部做 cache_control 注入，无需 LRU，无需把 string content 转 array。

## 9. 重试策略

| 错误形态 | 重试 | 备注 |
|---|---|---|
| 网络 throw / context deadline | ✅ | 切换 account 重试 |
| HTTP 5xx | ✅ | 同上 |
| stream `error` event `isRetryable=true` | ✅ | 同上 |
| HTTP 429 | ✅ 按 Retry-After | 限速优先级 |
| HTTP 4xx (除 429) / MODEL_NOT_IN_PLAN | ❌ | 透传 |
| `isRetryable=false` | ❌ | 透传 |

- 上限 2 次，退避 250ms → 750ms
- 每次重试换新 `x-session-id`（让 CC 路由到不同 upstream）
- **仅在反代→客户端尚未 flush 任何 text/tool_call 内容时允许重试**

## 10. Pool 路由

```go
type Account struct {
    ID                 string
    APIKey             string
    Paused             bool
    LastKnownCredits   float64
    Stats5min          *RollingStats
}

func (p *Pool) Pick(clientToken, model, msgPrefix string) (*Account, error) {
    healthy := p.healthy()  // credits>0.5 && err5min<0.20 && !paused
    if len(healthy) == 0 { return nil, ErrAllUnhealthy }
    h := xxhash.Sum64String(clientToken + "|" + model + "|" + msgPrefix)
    return healthy[h % uint64(len(healthy))], nil
}
```

后台 goroutine：每 60s 串行调用每个账号的 `/alpha/billing/credits` 更新 `LastKnownCredits`，触发 dashboard SSE 事件。

## 11. 协议适配要点

### OpenAI 入参 → canonical

- `messages[*].content`：string 直传；parts[] 提取 text 拼接（短期忽略 image/audio）
- `tools[].function` → CC tools schema
- `temperature/top_p/tool_choice/response_format`：忽略，响应 header 加 `x-cmdgo-ignored: temperature,top_p`
- `stream`: 永远改 true（CC 只支持 stream）

### canonical → CC body

见 §8

### CC SSE → OpenAI SSE

| CC event | OpenAI chunk |
|---|---|
| `start` | 首个 chunk 含 `role:assistant` |
| `text-delta` | `choices[0].delta.content` |
| `reasoning-delta` | `choices[0].delta.reasoning_content`（DeepSeek/o1 风格，可关） |
| `tool-call` | `choices[0].delta.tool_calls[]` |
| `finish` | 末尾 chunk 带 `finish_reason` + `usage`（含 `prompt_tokens_details.cached_tokens`） |
| 流结束 | `data: [DONE]` |

### Anthropic 入参 → canonical

- messages content blocks 几乎直通
- `system` 顶级字段直通
- `tools` 直通（input_schema 同构）
- `metadata.user_id` 优先作为 client session 因子

### CC SSE → Anthropic SSE

| CC event | Anthropic event |
|---|---|
| `start` | `message_start` |
| `text-delta` | `content_block_start(text)` + `content_block_delta` |
| `reasoning-delta` | `content_block_start(thinking)` + `content_block_delta` |
| `reasoning-end` | `content_block_stop` |
| `tool-call` | `content_block_start(tool_use)` + delta + stop |
| `finish` | `message_delta(stop_reason, usage)` + `message_stop` |

usage 字段映射：`cacheReadTokens → cache_read_input_tokens`，`cacheWriteTokens → cache_creation_input_tokens`。

## 12. 前端（dashboard 模板片段）

```html
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>cmdgo</title>

  <!-- Tailwind v4 browser build -->
  <script src="https://cdn.jsdelivr.net/npm/@tailwindcss/browser@4"></script>
  <style type="text/tailwindcss">
    @theme {
      --color-cmd-bg: #0d0f12;
      --color-cmd-accent: #6ee7b7;
    }
  </style>

  <!-- HTMX 2.0 core + SSE extension（2.x 拆为独立扩展） -->
  <script src="https://cdn.jsdelivr.net/npm/htmx.org@2.0.10/dist/htmx.min.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/htmx-ext-sse@2.2.4"></script>

  <!-- Alpine.js（必须 defer） -->
  <script defer src="https://cdn.jsdelivr.net/npm/alpinejs@3.15.12/dist/cdn.min.js"></script>
</head>
<body hx-ext="sse" class="bg-cmd-bg text-white">
  <div sse-connect="/api/events">
    <div id="accounts"
         hx-get="/api/accounts"
         hx-trigger="load, sse:account-update"></div>
    <div id="traffic"
         hx-get="/api/traffic"
         hx-trigger="load, sse:traffic-row, every 30s"></div>
  </div>
</body>
</html>
```

服务端 SSE 推送（go template 渲染好 HTML 片段，HTMX 直接 swap 进 DOM）：

```
event: account-update
data: <div class="account-card">...</div>

event: traffic-row
data: <tr>...</tr>
```

## 13. Go 标准库现代用法

### ServeMux（Go 1.22+，1.26 完全成熟）

```go
mux := http.NewServeMux()
mux.HandleFunc("POST /v1/chat/completions",      openai.Handle)
mux.HandleFunc("POST /v1/messages",              anthropic.Handle)
mux.HandleFunc("GET  /v1/models",                openai.HandleModels)
mux.HandleFunc("POST /callback",                 oauth.HandleCallback)
mux.HandleFunc("OPTIONS /callback",              oauth.HandlePreflight)
mux.HandleFunc("DELETE /api/accounts/{id}",      accounts.HandleDelete)
mux.HandleFunc("POST /api/accounts/{id}/pause",  accounts.HandlePause)
// 取参数： id := req.PathValue("id")
```

不需要第三方路由库。

### log/slog（标准库 1.21+，1.26 新增 MultiHandler）

```go
logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
    Level:     slog.LevelInfo,
    AddSource: true,
}))
slog.SetDefault(logger)

slog.Info("upstream request",
    "account_id", acc.ID,
    "model", model,
    "session_id", sid,
)
```

### Go 1.26 可选利好

- `errors.AsType[T]` 比 `errors.As` 类型安全
- `runtime/pprof goroutineleak`（实验，`GOEXPERIMENT=goroutineleakprofile`）—— stream + retry 容易漏 goroutine，开发期值得开
- `go fix` modernizers —— 稳定后跑一次自动现代化

## 14. 部署

```bash
# 本地，默认
cmdgo
# → listen 127.0.0.1:8080, data ~/.cmdgo/state.json
# → 首次启动生成 pcc_xxx 并打印到 stderr

# 自定义端口/数据目录
cmdgo --listen 127.0.0.1:9000 --data ./mystate.json

# VPS 公网（套 TLS 反代）
cmdgo --listen 0.0.0.0:8080 --public-url https://cmdgo.example.com
```

Dockerfile：

```dockerfile
FROM scratch
COPY cmdgo /cmdgo
COPY --from=alpine:latest /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
EXPOSE 8080
ENTRYPOINT ["/cmdgo"]
```

CI: GitHub Actions cross-compile linux/darwin/windows × amd64/arm64，release 时打包附件。

## 15. 里程碑

| M | 内容 | 估时 |
|---|---|----:|
| **M0** | repo init, go.mod, main.go, listen + hello | 0.5h |
| **M1** | store.go（JSON 原子读写）+ pcc_xxx 生成与 middleware | 0.5h |
| **M2** | OAuth: `/callback`, `/api/oauth/{start,paste-key}`, 调 `/alpha/whoami` 验证 | 1h |
| **M3** | cc client: `/alpha/generate` SSE + `whoami/usage/credits` | 1h |
| **M4** | `/v1/chat/completions` OpenAI 适配（含 SSE 重写） | 2h |
| **M5** | `/v1/messages` Anthropic 适配 | 1.5h |
| **M6** | pool + affinity 路由 + 重试（pre-flush only）+ 健康判定 | 1.5h |
| **M7** | dashboard HTMX（layout + account_card + traffic）+ `/api/events` SSE | 2.5h |
| **M8** | 流量日志记录 + 60s `/billing/credits` 后台同步 | 1h |
| **M9** | README + Dockerfile + GitHub Actions release | 1h |

**MVP（M0~M6）**：8h，CLI 客户端能通 OpenAI/Anthropic 协议跑通。
**Full（M0~M9）**：12h，含 Dashboard、用量同步、Docker。

## 16. 验收清单

- [ ] `curl http://localhost:8080/v1/chat/completions -H "Authorization: Bearer pcc_..." -d '{model:"deepseek/deepseek-v4-pro",messages:[...]}'` 流式返回 "pong"
- [ ] Anthropic SDK 调 `/v1/messages` 拿到 thinking + text + usage（含 cache）
- [ ] dashboard 显示账号卡片，credits 实时更新
- [ ] 添加第二账号后两次连续请求自动落到不同 SID buckets
- [ ] 强杀 CC 上游（断网模拟）→ 自动重试到 alt 账号 → 客户端无感
- [ ] 流式响应中 ctrl-C 客户端 → 反代正确关闭 upstream connection

## 17. 已确认的设计前提

1. ✅ Go 套餐 apikey 通过 `/studio/auth/cli` OAuth 可获取
2. ✅ Premium 模型（Anthropic/OpenAI/Gemini）服务端 plan-locked，反代不暴露
3. ✅ 12 个开源模型全部可用，定价已掌握（见 `cc-api-endpoints.md`）
4. ✅ Cache 策略：`params.cache_control: {type:"ephemeral"}` 一行解决，跨 SID 也命中
5. ✅ 账户 API：`/alpha/whoami` `/alpha/usage/summary` `/alpha/billing/credits` 都接受 apikey
