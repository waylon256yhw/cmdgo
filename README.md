# cmdgo

A reverse proxy that exposes [Command Code](https://commandcode.ai) Go-tier accounts (`$1/mo, $10 credits, open-source models only`) as OpenAI- and Anthropic-compatible APIs, with a self-contained HTMX dashboard for account management.

```
client (OpenAI/Anthropic SDK, Cherry Studio, Claude Code, …)
       │ /v1/chat/completions │ /v1/messages
       ▼
  ┌────────────┐    multi-account pool + affinity routing
  │   cmdgo    │ ── pre-flush retry on transient upstream errors
  └─────┬──────┘    automatic cache hint, 60s credit sync
        │ /alpha/generate
        ▼
   api.commandcode.ai
```

## Why

Command Code's Go tier doesn't ship a native API endpoint — only the official `cmd` CLI and the Studio web app. But the CLI's OAuth flow issues a real `user_...` apikey that works against `POST /alpha/generate`. cmdgo wraps that with:

- **OpenAI-compatible** `POST /v1/chat/completions` + `GET /v1/models`
- **Anthropic-compatible** `POST /v1/messages` (text, thinking, tool use, cache usage)
- Multi-account pool with affinity routing and health-aware failover
- Automatic prompt-cache injection (`cache_control: {type: "ephemeral"}`)
- Embedded web dashboard — HTMX 2 + Alpine 3 + Tailwind v4, all CDN, no build step
- Single Go binary, ~13 MiB, no external runtime
- Light + dark theme

## Scope

Only the Go tier. The 12 open-source models on Go (DeepSeek V4 Pro/Flash, MiniMax M2.5/M2.7, Qwen 3.6 Plus / 3.6 Max-Preview / 3.7 Max, Kimi K2.5/K2.6, GLM-5/5.1, Step 3.5 Flash) are whitelisted; Anthropic / OpenAI / Gemini models are server-side plan-locked and return `MODEL_NOT_IN_PLAN`. If you have Pro/Max/Ultra, use the native API.

## Quick start

### Pre-built binary

Grab the latest release from [GitHub Releases](../../releases) for your OS/arch, then:

```bash
chmod +x cmdgo
./cmdgo
# → listens on 127.0.0.1:8080
# → prints a one-time `pcc_...` proxy token to stderr; save it
```

Open `http://127.0.0.1:8080/?token=pcc_…` in a browser, click **+ Add account**, follow the prompts.

### Docker

```bash
docker run -d --name cmdgo \
  -p 8080:8080 \
  -v cmdgo-data:/data \
  ghcr.io/waylon256yhw/cmdgo:latest
docker logs cmdgo 2>&1 | grep pcc_   # grab the proxy token
```

### From source

```bash
git clone https://github.com/waylon256yhw/cmdgo
cd cmdgo
go build -o cmdgo .
./cmdgo
```

Requires Go 1.26+.

## Configuration

All flags also accept the corresponding `CMDGO_*` env var.

| Flag | Env | Default | Notes |
|---|---|---|---|
| `--listen` | `CMDGO_LISTEN` | `127.0.0.1:8080` | bind address; use `0.0.0.0:8080` to expose publicly |
| `--data` | `CMDGO_DATA` | `~/.cmdgo/state.json` | JSON state file (atomic write, mode `0600`) |
| `--public-url` | `CMDGO_PUBLIC_URL` | derived from `--listen` | externally-visible URL; shown in the endpoint cheat-sheet |
| `--cc-base-url` | `CMDGO_CC_BASE_URL` | `https://api.commandcode.ai` | override Command Code base URL (testing only) |

## Usage

### Add an account

Two paths, surfaced as one modal in the dashboard:

1. **OAuth callback (automatic)** — works when your browser can reach `http://localhost:<port>/callback` on the same machine cmdgo is running, or via SSH tunnel.
2. **Manual paste** — when CC Studio shows you the apikey instead of redirecting (typical when cmdgo isn't on your local machine), paste it into the textarea in the same modal.

Multiple accounts deduplicate by Command Code user ID; re-adding the same account refreshes the apikey + credits in place.

### Use from a client

Point any OpenAI- or Anthropic-compatible client at your cmdgo URL with the proxy token (`pcc_…`) as the API key. The middleware accepts the token in any of three forms:

- `Authorization: Bearer pcc_…` (OpenAI SDKs, raw curl)
- `x-api-key: pcc_…` (Anthropic SDK, Cherry Studio's Anthropic mode)
- `?token=pcc_…` query param (browser `EventSource` for dashboard SSE)

Example:

```bash
curl -N http://127.0.0.1:8080/v1/chat/completions \
  -H "Authorization: Bearer $CMDGO_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "deepseek/deepseek-v4-pro",
    "messages": [{"role":"user","content":"hi"}],
    "stream": true
  }'
```

### Dashboard

- Live accounts grid with credit progress, pause/resume/remove
- Rolling 50-row traffic log (SSE-driven, no polling)
- Endpoint cheat-sheet with the right `Base` URL pre-filled
- Token reveal/copy/rotate
- Light/dark toggle, persisted per browser

## Deployment

### Local (default)

`./cmdgo` listens on `127.0.0.1:8080` and writes state to `~/.cmdgo/state.json`. Suitable for one user on one machine.

### VPS — bare

```bash
./cmdgo --listen 0.0.0.0:8080 --public-url https://cmdgo.example.com
```

Expose port 8080 through your firewall. The proxy token gates all `/v1/*` and `/api/*` routes; the dashboard HTML at `/` is public (the token is the gate, not the page).

### VPS — behind a TLS-terminating reverse proxy (recommended)

```nginx
location / {
    proxy_pass http://127.0.0.1:8080;
    proxy_http_version 1.1;
    proxy_set_header Connection "";
    proxy_buffering off;            # required for SSE streaming
    proxy_read_timeout 1h;          # long-lived dashboard event stream
}
```

```bash
./cmdgo --listen 127.0.0.1:8080 --public-url https://cmdgo.example.com
```

### Docker compose

A ready-to-use `docker-compose.yml` ships in the repo root:

```bash
docker compose up -d
docker compose logs cmdgo 2>&1 | grep pcc_   # grab the proxy token
```

Set `CMDGO_PUBLIC_URL` in the compose file when fronting cmdgo behind TLS so the dashboard's endpoint cheat-sheet uses the right URL.

## Design

- [`docs/plan.md`](docs/plan.md) — full design doc (data model, pool routing, retry policy, protocol mapping)
- [`docs/cc-api.md`](docs/cc-api.md) — Command Code upstream API reference, compiled from empirical probing

## Disclaimer

Unofficial, community-built. Not affiliated with Command Code. Using it may violate Command Code's terms of service. Use at your own risk.

## License

[MIT](LICENSE)
