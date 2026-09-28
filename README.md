# simple-ai-router

A single-binary, OpenAI-compatible proxy that launches inference servers
(llama-server, vLLM, ...) on demand and swaps between them, similar to
[llama-swap](https://github.com/mostlygeek/llama-swap).

A request's `model` field selects the model. Only one model runs at a time:
work for the other models waits in the router, and a batch scheduler decides
when to swap (stop the running model with SIGTERM, then SIGKILL, start the
next, wait for its health check). Models with no OpenAI API get an
asynchronous **job** API from the router: submit a request, poll, fetch the
file. See [docs/time-share-v2-plan.md](docs/time-share-v2-plan.md) for the
scheduler and [docs/time-share-plan.md](docs/time-share-plan.md) for jobs.

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

Every request is a ticket, **booked into a batch** when it arrives. A batch is
shared, by `share`, among the models booked in it; each model runs its part in
one turn, so it is loaded at most once per batch, and the model already loaded
goes first in the next batch.

A batch's length **adapts** to what is booked in it: long enough that loading
its models takes at most `maxSwitchOverhead` of it, and that its longest job
fits, between `minCycle` and `maxCycle`. With loads of 39 s and 13 s at 10 %,
a batch of both is about 9 minutes; one model after another that loads in
13 s gets `minCycle`; a batch holding a 25-minute render stretches to it.

- A ticket costs its estimate: the workload's `eta_s` from `validate`, else
  the learned duration of its model and path, else `defaultEta`. What the
  router learns (durations, load times) is kept in `<jobs.dir>/.router/stats.json`.
- A newcomer takes its part of the next batch; a model over its part (a
  flood) has its last tickets moved to later batches.
- Tickets booked in the running batch always run in it. New work joins the
  running batch only for a model whose turn is not over and only in time no
  one booked; otherwise it books a later batch.
- A model that is alone is never stopped and admits everything at once.
- A turn that overruns its part is paid back from the model's next turn.

`GET /jobs/{id}` shows a queued job's `batch` (1 = running) and `starts_in_s`.
A `/v1` request that would wait longer than `syncMaxWait` gets `503`, and a job
predicted to start after `maxWait` gets `429`, both with `Retry-After`. Per
model, `concurrency` caps what runs at once, `maxQueue` refuses more waiting
requests with `429`, and `queueTimeout` gives up with `503`.

```yaml
batch: {minCycle: 2m, maxCycle: 30m, maxSwitchOverhead: 0.1, linger: 2s}   # defaults
models:
  qwen-7b:
    share: 3
    concurrency: 4
    syncMaxWait: 1m
  yue2:
    defaultEta: 5m      # until its paths' durations are learned
    loadTime: 60s       # until a load is measured
    maxWait: 2h
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

### References and pipelines

A job can use another job's output, or an uploaded file, without the client
downloading and uploading it again. The router puts the file in the request
when it sends it to the workload:

```sh
curl -XPOST localhost:8080/artifacts -H 'X-Filename: face.png' --data-binary @face.png
                                                          # 201 {"id": "sha256:9f86...", ...}; same bytes, same id
curl -XPOST localhost:8080/jobs/yue2/decode -F bundle@ref=job:e3969fd18827 -F format=mp3
curl -XPOST localhost:8080/jobs/avatar/render -H 'content-type: application/json' \
     -d '{"image": {"$ref": "artifact:9f86..."}, "audio": {"$ref": "job:7ab2c4e1d0f3"}}'
```

- multipart: a text part `<field>@ref` becomes a file part `<field>`;
- JSON: `{"$ref": ...}` becomes the file as base64 (up to `jobs.maxInlineRef`).

A reference to a job that is not done yet makes the new job `blocked` until it
is, and fails it if that job fails, so a whole pipeline can be submitted at
once; a blocked job is booked right after the job it waits for, in the same
turn when both are for the same model. A finished job's output also goes into
the artifact store (`sha256` in its view); nothing a queued or blocked job
refers to is removed by retention. Artifacts not used for `artifacts.ttl`, or
beyond `artifacts.maxSize`, are removed.

```yaml
jobs: {maxInlineRef: 32MB}                             # defaults
artifacts: {maxSize: 50GB, ttl: 720h, maxUpload: 1GB}  # ttl defaults to jobs.resultTTL
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
| `POST /artifacts` | Store an upload (raw body with `X-Filename`, or multipart `file`); `201` with its `sha256:` id |
| `GET /artifacts/{id}`, `DELETE /artifacts/{id}` | Download, or remove (`409` while a queued job refers to it) |
| `GET /running` | Per model: state, PID, in-flight count, queue length, turn, quota, load time, job counts; the booked batches, switch overhead and learned durations |
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
