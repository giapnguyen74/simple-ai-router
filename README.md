# simple-ai-router

A single-binary, OpenAI-compatible proxy that launches inference servers
(llama-server, vLLM, ...) on demand and swaps between them, similar to
[llama-swap](https://github.com/mostlygeek/llama-swap).

A request's `model` field selects the model. Only one model runs at a time:
work for the other models waits in the router, and a time-share scheduler
decides when to swap (stop the running model with SIGTERM, then SIGKILL, start
the next, wait for its health check). Models with no OpenAI API get an
asynchronous **job** API from the router: submit a request, poll, fetch the
file. See [docs/time-share-plan.md](docs/time-share-plan.md) for the design.

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

## Scheduling

Every request is a ticket in its model's FIFO queue. A model that is alone
keeps the GPU until its `ttl`. When several models have work, each gets a
slice of `timeShare.period` in proportion to its `share` (at least
`minSlice`); an idle model yields after `linger`. Per model, `concurrency`
caps what runs at once, `maxQueue` refuses more waiting requests with `429`,
and `queueTimeout` gives up with `503`.

```yaml
timeShare: {period: 10m, minSlice: 1m, linger: 2s}   # defaults
models:
  qwen-7b:
    share: 3
    concurrency: 4
    maxQueue: 100
    queueTimeout: 30m
```

## Jobs

A workload that answers synchronously (one request in, one file out) gets an
asynchronous API from the router:

```sh
curl -XPOST localhost:8080/jobs/qwen-image/generate -H 'content-type: application/json' \
     -d '{"prompt": "a lighthouse at dusk"}'              # 202 {"id": "e3969fd18827", "status": "queued", ...}
curl localhost:8080/jobs/e3969fd18827/wait?timeout=600    # long-poll until done
curl -o out.png localhost:8080/jobs/e3969fd18827/result   # the file the workload wrote
curl -XDELETE localhost:8080/jobs/e3969fd18827            # cancel
```

The router stores the request under `jobs.dir`, replays it to the model when
its turn comes with `X-Job-Id` and `X-Job-Dir` headers, and the workload
writes its output into that folder and answers `{"file": "image.png",
"result": {...}}`. Optional per-model `progress` and `validate` endpoints give
live progress in `GET /jobs/{id}` and early `422`s at submit. Finished jobs
are kept for `resultTTL` (default 30 days). The contract and the folder layout
are in [docs/time-share-plan.md](docs/time-share-plan.md#jobs).

```yaml
jobs: {dir: ./jobs, resultTTL: 720h, maxBodySize: 64MB}   # defaults
models:
  qwen-image:
    concurrency: 1
    maxQueue: 16
    jobTimeout: 1h
    progress: {endpoint: /progress, interval: 1s}
    validate: {endpoint: /validate}
```

## Job-queue servers (legacy)

Some servers accept work and finish it in the background themselves (an image
server whose own `POST /jobs` returns `202` right away). The router cannot see
that work from the requests alone, so give such a model a `busyCheck`:

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
| `POST /jobs/{model}/{path}` | Store the request as a job; `202` with its id |
| `GET /jobs/{id}`, `GET /jobs/{id}/wait?timeout=S` | Job status, or long-poll until it finishes |
| `GET /jobs/{id}/result` | The job's output file |
| `DELETE /jobs/{id}` | Cancel a job |
| `GET /jobs[?model=name]` | All jobs, newest first |
| `GET /running` | State, PID, in-flight count, queue length, slice and job counts per model |
| `POST /unload[?model=name]` | Stop the running model, or one by name |
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
