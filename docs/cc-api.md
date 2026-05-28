# Command Code API 端点参考

> 通过 OAuth 流程拿到 `user_...` apikey 后，可直接调用以下端点。所有端点均接受 `Authorization: Bearer user_...`。本文档记录实测结果，不依赖官方文档（CC 没有公开 API 文档）。

## 0. 通用约定

### Base URL

```
https://api.commandcode.ai
```

### 鉴权

```
Authorization: Bearer user_...
```

无效 / 缺失 → `HTTP 401`：

```json
{
  "success": false,
  "error": {
    "code": "UNAUTHORIZED",
    "status": 401,
    "message": "Invalid 'Authorization' header or token.",
    "docs": "https://commandcode.ai/docs/reference/errors/unauthorized"
  }
}
```

### 必需 Headers（出于伪装 CC CLI 的需要）

```
Authorization: Bearer user_...
x-command-code-version: 0.24.1
x-cli-environment: production
```

仅查询账户信息（`/whoami`, `/usage/summary`, `/billing/credits`）时，只要前两个 header 就够。`/alpha/generate` 还要更多（见 §4）。

### 通用错误响应（4xx）

JSON 结构统一：

```json
{
  "success": false,
  "error": {
    "code": "FORBIDDEN | UNAUTHORIZED | ...",
    "status": 403,
    "message": "MODEL_NOT_IN_PLAN: Claude Haiku 4.5 available in Pro and above plans...",
    "docs": "https://commandcode.ai/docs/reference/errors/forbidden"
  }
}
```

### 响应中常见 Headers

```
content-type: application/json | text/event-stream
server: cloudflare
x-powered-by: Hono
x-request-id: req_xxx                              ← 排查用
x-system-prompt-breakdown: {"systemPrompt":9247,"memory":0,"taste":0}   ← /alpha/generate only
cf-placement: local-SIN | remote-LAX
cf-ray: ...
```

---

## 1. `GET /alpha/whoami`

返回当前 apikey 对应的用户信息。

### Request

```bash
curl -H "Authorization: Bearer user_..." \
     -H "x-command-code-version: 0.24.1" \
     https://api.commandcode.ai/alpha/whoami
```

### Response 200

```json
{
  "success": true,
  "user": {
    "id": "6d37ae30-27ec-40ac-b409-9adb585bb733",
    "name": "Hanwen Yu",
    "email": "waylon256yhw@gmail.com",
    "userName": "waylon256yhw"
  },
  "org": null
}
```

### 用途

- OAuth 拿到 apikey 后验证有效
- 反代 dashboard 上显示账号身份

---

## 2. `GET /alpha/usage/summary`

返回当前计费周期的用量汇总，含 by-model breakdown。

### Request

```bash
curl -H "Authorization: Bearer user_..." \
     -H "x-command-code-version: 0.24.1" \
     https://api.commandcode.ai/alpha/usage/summary
```

`?period=month` / `?period=day` / `?days=30` 等 query 都被忽略（实测响应大小一致，内容相同），目前只返回当前 cycle 全量。

### Response 200

```json
{
  "totalCount": 13,
  "totalCost": 0.0633,
  "averageCost": 0.004869230769230769,
  "successRate": 100,
  "completedCount": 13,
  "failedCount": 0,
  "totalTokensIn": "81970",
  "totalTokensOut": "249",
  "totalTokens": "82219",
  "totalCredits": 0.0633,
  "totalFreeCredits": 0,
  "totalMonthlyCredits": 0.0633,
  "totalPurchasedCredits": 0,
  "models": [
    { "model": "Qwen/Qwen3.6-Max-Preview",    "totalCost": 0.0126, "count": 1 },
    { "model": "Qwen/Qwen3.7-Max",            "totalCost": 0.0121, "count": 1 },
    { "model": "zai-org/GLM-5.1",             "totalCost": 0.0103, "count": 1 },
    { "model": "moonshotai/Kimi-K2.6",        "totalCost": 0.0071, "count": 1 },
    { "model": "MiniMaxAI/MiniMax-M2.5",      "totalCost": 0.0022, "count": 1 },
    { "model": "stepfun/Step-3.5-Flash",      "totalCost": 0.0007, "count": 1 },
    { "model": "deepseek/deepseek-v4-flash",  "totalCost": 0.0002, "count": 2 },
    { "model": "deepseek/deepseek-v4-pro",    "totalCost": 0.0001, "count": 1 }
  ]
}
```

注意：
- `totalTokensIn/Out/Total` 是字符串（不是数字）
- `models` 按 `totalCost` 降序
- `*Credits` 是 USD

### 用途

- 反代 dashboard 显示账号当前 cycle 用量
- 计算单次请求成本 / 累计成本
- 知道哪些 model 用得最多

---

## 3. `GET /alpha/billing/credits`

返回当前 cycle 剩余 credits + 阈值。

### Request

```bash
curl -H "Authorization: Bearer user_..." \
     -H "x-command-code-version: 0.24.1" \
     https://api.commandcode.ai/alpha/billing/credits
```

### Response 200

```json
{
  "credits": {
    "belowThreshold": false,
    "creditThreshold": 0,
    "monthlyCredits": 9.9367,
    "purchasedCredits": 0,
    "freeCredits": 0
  }
}
```

字段含义：

- `monthlyCredits`：本月套餐 credits 剩余（USD）
- `purchasedCredits`：top-up credits 剩余（不会 expire）
- `freeCredits`：免费 credits
- `creditThreshold`：用户配置的低额警戒阈值
- `belowThreshold`：当前是否低于阈值

### 用途

- 反代 pool 健康判定（`monthlyCredits + purchasedCredits + freeCredits > $0.5`）
- 后台 goroutine 每 60s 同步一次，更新内存 health 状态
- dashboard 进度条显示

---

## 4. `POST /alpha/generate`

主入口。流式生成。

### Request Headers

```
Authorization: Bearer user_...
Content-Type: application/json
x-command-code-version: 0.24.1
x-cli-environment: production
x-project-slug: <你的标识>            # 任意字符串，CC 后台分类用，建议如 cmdgo-prod
x-taste-learning: false               # 不要 taste 学习
x-co-flag: false
x-session-id: <UUID v4>               # 影响上游路由，建议同 client 同对话稳定
```

### Request Body 结构

```json
{
  "config": {
    "workingDir": "/tmp",
    "date": "2026-05-25",
    "environment": "linux-x64, Node.js v22",
    "structure": [],
    "isGitRepo": false,
    "currentBranch": "",
    "mainBranch": "",
    "gitStatus": "",
    "recentCommits": []
  },
  "memory": "",
  "taste": "",
  "skills": null,
  "permissionMode": "standard",
  "params": {
    "model": "deepseek/deepseek-v4-pro",
    "messages": [...],
    "tools": [],
    "system": "",
    "max_tokens": 64,
    "stream": true,
    "cache_control": { "type": "ephemeral" }
  }
}
```

`config` 模拟 cmd CLI 在用户工作目录跑的上下文。各字段可以全填空 / 默认值，反代不需要 reflect 真实工作目录。

`params` 是真正的生成参数，结构类似 Vercel AI SDK 风格（CC 内部用 ai-sdk 串上游）。

### `params.messages` 内部 schema（基于 Vercel AI SDK 风格）

```json
[
  // user message：content 可以是 string 或 content blocks
  { "role": "user", "content": "Reply with exactly one word: pong" },
  // 或
  { "role": "user", "content": [
    { "type": "text", "text": "..." },
    { "type": "text", "text": "...", "cache_control": { "type": "ephemeral" } }
  ]},

  // assistant message：content 是 blocks 数组
  { "role": "assistant", "content": [
    { "type": "text", "text": "..." },
    { "type": "reasoning", "text": "..." },
    { "type": "tool-call", "toolCallId": "...", "toolName": "...", "input": {...} }
  ]},

  // tool result message
  { "role": "tool", "content": [
    {
      "type": "tool-result",
      "toolCallId": "...",
      "toolName": "...",
      "output": { "type": "text", "value": "..." }
      // 或 { "type": "error-text", "value": "..." }
    }
  ]}
]
```

### `params.tools` 内部 schema

```json
[
  {
    "type": "function",
    "name": "get_weather",
    "description": "Get current weather",
    "input_schema": {
      "type": "object",
      "properties": { "city": { "type": "string" } },
      "required": ["city"]
    }
  }
]
```

`input_schema` 是 JSON Schema，与 Anthropic Messages API 同构。

### Cache Control 字段位置

**Automatic mode**（推荐，最简）：在请求顶层放一个 `cache_control`，CC 自动决定 breakpoint 位置。两个位置都接受：

```json
// 方式 A: 在 params 内顶层
{
  "params": {
    ...,
    "cache_control": { "type": "ephemeral" }
  }
}

// 方式 B: 在 body 顶层（与 config 平级）
{
  "config": {...},
  "cache_control": { "type": "ephemeral" },
  "params": {...}
}
```

实测两种位置效果完全相同。

**Explicit mode**：在 messages content blocks 上手动打：

```json
{
  "role": "user",
  "content": [
    { "type": "text", "text": "...", "cache_control": { "type": "ephemeral" } }
  ]
}
```

最多 4 个 breakpoint，需要把 string content 转为 array。

### Response

`content-type: text/event-stream` —— **但实际 framing 是 newline-delimited JSON，不是 SSE**。每行就是一个完整的 `{"type":"..."}`，**没有 `data:` 前缀、没有空行分隔**。严格按 SSE spec 解析会得到 0 个事件（reader 找不到 `data:` 前缀、忽略所有行直至 EOF）。

```
{"type":"start"}
{"type":"start-step","request":{...}}
{"type":"reasoning-start","id":"reasoning-0",...}
{"type":"reasoning-delta","id":"reasoning-0","text":"The"}
{"type":"text-delta","id":"txt-0","text":"p"}
{"type":"finish","finishReason":"stop","totalUsage":{...}}
```

实现建议：scanner 同时接受两种 framing —— 行首是 `{` 当 JSON 事件直接处理；否则走标准 SSE `data:` 路径。这样以后 CC 切回真 SSE 也兼容。

#### 事件类型

下表标了实测核心事件。还有若干辅助事件（`start-step`, `finish-step`, `reasoning-start`, `text-start`, `text-end`, `text`, `provider-metadata`）proxy 可安全忽略。

| event.type | 字段 | 说明 |
|---|---|---|
| `start` | — | 流开始（首事件） |
| `text-delta` | `text` | 文本片段，append 到 content |
| `reasoning-delta` | `text` | thinking/reasoning 片段 |
| `reasoning-end` | — | 一段 reasoning 结束 |
| `tool-call` | `toolCallId`, `toolName`, `input`/`args`/`arguments` | 完整工具调用（不是 delta） |
| `finish` | `finishReason`, `totalUsage` | 流结束，带使用统计 |
| `error` | `error: {type, message, statusCode, isRetryable}` | 流内错误 |

#### `finish` 事件 `totalUsage` 结构

```json
{
  "inputTokens": 7424,
  "inputTokenDetails": {
    "noCacheTokens": 113,
    "cacheReadTokens": 7424,
    "cacheWriteTokens": 0
  },
  "outputTokens": 128,
  "outputTokenDetails": {
    "textTokens": 100,
    "reasoningTokens": 28
  },
  "totalTokens": 7552,
  "reasoningTokens": 28,
  "cachedInputTokens": 7424
}
```

字段语义：
- `inputTokens` = `noCacheTokens` + `cacheReadTokens`（不含 cacheWrite 的"写入溢价" tokens）
- `cacheReadTokens` 计费按 cache read 价（约 input 的 10%）
- `cacheWriteTokens` 计费按 cache write 价（约 input 的 125%）；首次写入的"超出 input 部分"会计入 `noCacheTokens`

#### `finish.finishReason` 可能值

- `"stop"` — 正常结束
- `"tool-calls"` — 因为工具调用结束
- `"length"` / `"max_tokens"` / `"max-tokens"` / `"max_output_tokens"` — 达到 max_tokens
- 其它 → 当 stop 处理

#### `error` 事件示例

```json
{
  "type": "error",
  "error": {
    "type": "server_error",
    "message": "Service temporarily unavailable. Please try again shortly.",
    "statusCode": 503,
    "isRetryable": true
  }
}
```

```json
{
  "type": "error",
  "error": {
    "type": "server_error",
    "message": "Network connection lost."
  }
}
```

### 错误：HTTP 403 MODEL_NOT_IN_PLAN

请求 Anthropic / OpenAI / Gemini 模型时（Go 套餐没权限）：

```json
{
  "success": false,
  "error": {
    "code": "FORBIDDEN",
    "status": 403,
    "message": "MODEL_NOT_IN_PLAN: Claude Haiku 4.5 available in Pro and above plans or extra on demand usage",
    "docs": "https://commandcode.ai/docs/reference/errors/forbidden"
  }
}
```

---

## 5. OAuth 流程

CC 没有传统 OAuth client_id/secret 模式，而是一个 web-based "CLI 登录" 流程。Studio 网页签发 apikey 后 POST 给本地 callback。

### Auth URL

```
https://commandcode.ai/studio/auth/cli?callback=<URL>&state=<token>
```

参数：
- `callback`：CC Studio 在用户登录后 POST 的目标 URL。**必须 localhost / 客户端可达**，CORS 限制见下
- `state`：32 字节 base64url 随机 token，CSRF 防护，用户客户端校验回传

### Callback POST

CC Studio 发：

```
POST <callback>
Content-Type: application/json
Origin: https://commandcode.ai
```

Body（成功）：

```json
{
  "apiKey": "user_...",
  "state": "<同样的 token>",
  "userId": "...",
  "userName": "...",
  "keyName": "..."
}
```

Body（失败）：

```json
{
  "error": "access_denied",
  "error_description": "Authorization was denied by the user"
}
```

### Callback Server 的 CORS

CC Studio 的浏览器端发请求时，**callback server 必须正确响应 CORS preflight**：

```
OPTIONS <callback>
  → 200
  Access-Control-Allow-Origin: https://commandcode.ai
  Access-Control-Allow-Methods: POST, OPTIONS
  Access-Control-Allow-Headers: Content-Type
  Access-Control-Allow-Private-Network: true     ← Chrome PNA，localhost 必需

POST <callback>
  → 200
  Access-Control-Allow-Origin: https://commandcode.ai
  Content-Type: application/json
  body: {"success": true}
```

允许的 origin：
- `https://commandcode.ai`
- `https://staging.commandcode.ai`
- `http://localhost:3000`（CC 开发环境）

### Studio 兜底页面

如果浏览器无法 POST 到 callback（如 callback 在 VPS 但浏览器在本地、callback 端口被防火墙挡），Studio 会显示 "Copy your API key" 让用户手动复制。这是 paste-key 兜底模式的基础。

---

## 6. Plan 矩阵（来自 docs，2026-05 实测）

### Individual Plans

| Plan | Price/mo | Credits/mo | Models |
|---|---:|---:|---|
| **Go** | $1 | $10 | **Open-source only** |
| Pro | $15 | $30 | open + Anthropic/OpenAI/Gemini |
| Max | $100 | $150 | 同上 |
| Ultra | $200 | $300 | 同上 |

Go 套餐通过反代调用 Anthropic / OpenAI / Gemini 模型 → HTTP 403 MODEL_NOT_IN_PLAN（服务端 plan-level 拦截）。

### Go-eligible 模型清单（14 个）

| Model ID | In/Out/Cache /M | 备注 |
|---|---|---|
| `stepfun/Step-3.5-Flash` | $0.10 / $0.30 / $0.02 | 最便宜 |
| `deepseek/deepseek-v4-pro` | $0.435 / $0.87 / $0.003625 | **75% off 永久**（4× 用量）|
| `deepseek/deepseek-v4-flash` | $0.14 / $0.28 / $0.01 | 偶发 503 retryable |
| `xiaomi/mimo-v2.5` | $0.14 / $0.28 / $0.0028 | **~98% off 永久**（小米合作）|
| `xiaomi/mimo-v2.5-pro` | $0.435 / $0.87 / $0.0036 | **~99% off 永久**（小米合作）|
| `MiniMaxAI/MiniMax-M2.5` | $0.27 / $0.95 / $0.03 | |
| `MiniMaxAI/MiniMax-M2.7` | $0.30 / $1.20 / $0.06 | |
| `Qwen/Qwen3.6-Plus` | $0.50 / $3.00 / $0.10 | |
| `moonshotai/Kimi-K2.5` | $0.60 / $3.00 / $0.10 | |
| `moonshotai/Kimi-K2.6` | $0.95 / $4.00 / $0.16 | |
| `zai-org/GLM-5` | $1.00 / $3.20 / $0.20 | |
| `zai-org/GLM-5.1` | $1.40 / $4.40 / $0.26 | |
| `Qwen/Qwen3.7-Max` | $1.25 / $3.75 / $0.25 | **50% off 至 2026-06-22**，之后翻倍 |
| `Qwen/Qwen3.6-Max-Preview` | $1.30 / $7.80 / $0.26 | |

### Cache 行为分类（实测）

| 类型 | 模型 | 行为 |
|---|---|---|
| **Auto-cache** | DeepSeek V4 Pro/Flash | 全局 cache，无需 cache_control，跨 SID 命中 |
| **Hint-cache** | Kimi K2.5/K2.6, GLM-5/5.1, Qwen 3.6 Plus/Max-Preview/3.7-Max, Step 3.5 Flash | 需要 `cache_control: {type:"ephemeral"}` 激活，激活后跨 SID 命中 |
| **No-cache** | MiniMax M2.5/M2.7 | 上游不支持，cache_control 也无效 |
| **Unverified** | MiMo V2.5/V2.5 Pro | 上游列出 cache 价格，但 Auto vs Hint 未实测 — cmdgo 默认注入 `cache_control: ephemeral`，命中率待跑 `/alpha/usage/summary` 确认 |

### CC 后端自动注入的 systemPrompt

每个 `/alpha/generate` 请求 CC 后端会在 user 提供的 system 之后强制注入约 **9247 chars** 的 cmd CLI agentic 上下文（包含工具使用规范、Skill 引用、Markdown 规则等）。响应 header 有：

```
x-system-prompt-breakdown: {"systemPrompt":9247,"memory":0,"taste":0}
```

这是固定开销，反代无法关闭。**第一次请求会被计入 noCacheTokens（约 7200-7600）；后续相同 model + apikey 启用 cache_control 后会被缓存为 cacheReadTokens（约 10% 单价）。**

---

## 7. 反代必备的端点最小集

构建反代时只需要以下 4 个端点：

| 端点 | 用途 | 频率 |
|---|---|---|
| `POST /alpha/generate` | 所有客户端请求转发的目标 | 高频 |
| `GET /alpha/whoami` | OAuth 后验证 apikey + 拿账号身份 | 添加账号时 1 次 |
| `GET /alpha/billing/credits` | 健康判定 / dashboard 显示 | 每 60s 1 次 |
| `GET /alpha/usage/summary` | dashboard 详细统计 | 用户打开 dashboard 时按需 |

---

## 8. 参考实现

- TypeScript 参考实现：`pi-commandcode-provider/src/core.ts`（请求构造 + SSE 解析）
- TypeScript OAuth 实现：`pi-commandcode-provider/src/oauth.ts` + `src/auth-server.ts`（CORS / PNA / state 校验）
- 验证脚本：`pi-commandcode-provider/scripts/verify-cc.ts`、`scripts/cache-probe.ts`、`scripts/cache-probe-auto.ts`
- 模型元数据（TS）：`pi-commandcode-provider/scripts/cc-go-models.ts`
