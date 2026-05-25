# cmdgo

A reverse proxy that exposes [Command Code](https://commandcode.ai) Go-tier (`$1/mo, $10 credits, open-source models only`) accounts as OpenAI / Anthropic compatible APIs.

> **Status:** design phase. See [`docs/plan.md`](docs/plan.md) for the full design and [`docs/cc-api.md`](docs/cc-api.md) for the underlying Command Code API reference (compiled from empirical probing).

## Why

Command Code's Go tier doesn't ship a native API endpoint — only the official `cmd` CLI and the Studio web app. But the CLI's OAuth flow issues a real apikey that works against `POST https://api.commandcode.ai/alpha/generate`. cmdgo wraps that with:

- OpenAI-compatible `POST /v1/chat/completions`
- Anthropic-compatible `POST /v1/messages`
- Multi-account pool with affinity routing and health-aware failover
- Automatic prompt cache injection (one-line, no message rewriting)
- Embedded web dashboard (HTMX + Alpine + Tailwind v4, all CDN)
- Single Go binary, ~12 MB, no external runtime

## Scope

Only the Go tier. If you have Pro/Max/Ultra you should use the native API; this project doesn't help you.

The 12 open-source models on Go (`stepfun/Step-3.5-Flash`, `deepseek/deepseek-v4-pro`, `deepseek/deepseek-v4-flash`, `MiniMaxAI/MiniMax-M2.5/M2.7`, `Qwen/Qwen3.6-Plus/Max-Preview/Qwen3.7-Max`, `moonshotai/Kimi-K2.5/K2.6`, `zai-org/GLM-5/5.1`) are all whitelisted. Anthropic / OpenAI / Gemini models are server-side plan-locked and not exposed.

## Roadmap

See [`docs/plan.md`](docs/plan.md) §15. Milestones M0 → M9, ~12h total.

## Disclaimer

This is an unofficial, community-built tool. Not affiliated with Command Code. Using it may violate Command Code's terms of service. Use at your own risk.

## License

[MIT](LICENSE)
