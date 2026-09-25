# simple-ai-router

A single-binary, OpenAI-compatible proxy that launches inference servers
(llama-server, vLLM, ...) on demand and swaps between them, similar to
[llama-swap](https://github.com/mostlygeek/llama-swap).

A request's `model` field selects the model. If a different model is running,
the router waits for its in-flight requests to finish, stops it (SIGTERM, then
SIGKILL), starts the requested one, waits for its health check, and proxies
the request. Only one model runs at a time.

## Usage

```sh
make build        # or: go build -o simple-ai-router ./cmd/simple-ai-router
cp config.example.yaml config.yaml   # edit models
./simple-ai-router                   # or: -config path/to/config.{yaml,json}
```

Config can be YAML or JSON (chosen by the `.json` extension); see
`config.example.yaml` and `config.example.json`. Without `-config`, the router
looks for `config.yaml`, `config.yml`, then `config.json`. Durations are
strings like `"30s"` or `"10m"` in both formats.

```sh
curl localhost:8080/v1/chat/completions \
  -d '{"model":"qwen-7b","messages":[{"role":"user","content":"hi"}],"stream":true}'
```

## Job-queue servers

Some servers accept work and finish it in the background (for example an image
server whose `POST /jobs` returns `202` right away). The router cannot see that
work from the requests alone, so give such a model a `busyCheck`:

```yaml
busyCheck:
  endpoint: /health            # GET, returns a JSON object
  fields: [running, queued]    # busy while any is truthy (not null/false/0/""/empty)
  grace: 15s                   # default; see below
drainTimeout: 30m              # per-model override of the global drainTimeout
```

Before a swap or `/unload` stops the model, the router waits until the model
is not busy **and** has received no requests for `grace`, so clients can still
poll and download results of finished jobs. While it waits, requests to that
model are still proxied; the request that asked for the other model waits.
`drainTimeout` caps the wait, after which the model is stopped anyway. The TTL
unload never stops a busy model.

## Routes

| Route | Description |
|---|---|
| `POST /v1/chat/completions`, `/v1/completions`, `/v1/embeddings`, `/v1/rerank`, `/v1/audio/speech`, `/v1/audio/transcriptions` | Routed by the `model` field (JSON or multipart) |
| `GET /v1/models` | Configured models and aliases |
| `/u/{model}/{path}` | Direct passthrough to a model's server (e.g. llama-server web UI) |
| `GET /running` | State, PID and in-flight count per model |
| `POST /unload[?model=name]` | Stop one model, or all |
| `GET /health` | Router health |

## Development

```sh
make test         # unit tests
make race         # tests with the race detector
make lint         # go vet + gofmt check
make dist         # cross-compile to dist/ for linux, darwin, windows
make run CONFIG=config.json
```

Tests launch the test binary itself as a fake inference server
(`internal/fakeserver`), so no GPU or model files are needed.
