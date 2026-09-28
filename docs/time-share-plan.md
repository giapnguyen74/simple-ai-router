# Time-share scheduling and jobs

Status: **jobs implemented; the slice scheduler is replaced** by the batch
scheduler of [time-share-v2-plan.md](time-share-v2-plan.md) (`timeShare` is
still read: `period` becomes `batch.cycle`). The jobs part below is current;
the workload side lives in `simple-ai-server/GUIDELINE.md`.

## Problem it solves

Before this, the router had no queue of its own. It swapped on demand and let
each server finish whatever it held, which caused:

| Problem | Cause |
|---|---|
| **Starvation** | While A drained, requests for A bypassed the lock and were still proxied. Clients could keep adding jobs to A, so B waited until A's queue happened to be empty or `drainTimeout` killed A mid-job. |
| **Thrashing** | When A went idle it was shut down for a single B request. If A and B requests alternated, every request paid a full model load. |
| **No fairness or order** | Waiting requests were blocked on a mutex: no FIFO order, no priority between models, no limit on how many waited. |
| **No visibility** | The work lived inside each server's private queue. The router could not show what was waiting, and `/unload` blocked behind a drain. |

## Design

- Work waits **in the router**, not in the model servers.
- Workloads are **synchronous workers**: one request in, one file out, no
  queue, no job ids of their own.
- The router owns job ids, job status and the output files.
- Each model gets a share of GPU time when several models have work.
- Without contention nothing changes: a model that is alone keeps running.

Non-goals: running several models at once, preempting a request that is
already running.

### Tickets

Every unit of work is a **ticket** in its model's FIFO queue. There are two
kinds, scheduled the same way:

| Kind | Created by | Client sees |
|---|---|---|
| **Sync** | `/v1/...` and `/u/{model}/...` requests | The HTTP request stays open until the ticket is admitted and the response is proxied |
| **Job** | `POST /jobs/{model}/{path}` | `202` with a job id at once; the result is fetched later |

A ticket leaves the queue when it is admitted, when it is cancelled (client
disconnect for sync, `DELETE` for jobs), or when `queueTimeout` elapses
(`503`, or a failed job). If the queue already holds `maxQueue` tickets, a new
request gets `429`.

### Scheduler

All decisions are taken under one lock by `scheduleLocked`
(`internal/router/router.go`), on every event: ticket added or removed,
request finished, model ready, timer. Nothing that blocks runs under the lock;
stopping a model is handed to a goroutine, and the scheduler runs again when
it is done. State:

- `active`: the model that holds the GPU (or none), and `yielding` while it
  is being drained and stopped;
- `sliceStart`: when the active model became ready (load time is not charged);
- per model: ticket queue, admitted count, `idleSince`, `lastHeld`.

Rules:

1. **No contention**: if no other model has tickets, the active model admits
   its tickets at once. Its slice never expires. It stays loaded until `ttl`.
2. **Slice**: if another model has tickets, the active model may keep
   admitting until its slice ends:

   ```
   slice = max(minSlice, period * share / sum(share of models with work))
   ```

   The slice is measured from `sliceStart`, so a model that ran alone for an
   hour yields as soon as a contender shows up (after in-flight work ends).
3. **Concurrency**: at most `concurrency` tickets of a model are admitted at
   the same time (0 = no cap). Job tickets run one at a time unless
   `concurrency` is above 1; a job waiting for the running job does not hold
   back the sync requests queued behind it.
4. **Yield early**: if the active model has no tickets and nothing in flight,
   and another model waits, it yields after `linger` (default `2s`). Linger
   only defers within the slice; it trades a short wait for fewer swaps when
   requests for two models alternate.
5. **Swap**: when the slice ends, the router stops admitting to the active
   model, waits for admitted work to finish (bounded by `drainTimeout`),
   stops it, and starts the next model. A model with a running job or an
   admitted request is never stopped before `drainTimeout`.
6. **Next model**: the model that has waited longest, measured by its oldest
   ticket or, if later, by when it last gave the GPU up. Shares control slice
   length, waiting time controls order, so no model starves on a long queue
   elsewhere.

Model load time is not counted in the slice. `period` should be much larger
than the load time, or most GPU time is spent swapping.

**Kept for legacy job servers:** a model with a `busyCheck` still lets
requests through while it drains, so clients can poll the queue inside that
server. Models without a `busyCheck` get no bypass: their new requests wait
for their next turn.

`/unload` goes through the scheduler: it waits for the model's admitted work
(bounded by `drainTimeout`), then stops it. Unloading a model that does not
hold the GPU returns at once. Waiting tickets stay queued and reload the model
when its turn comes.

### Example

`period: 10m`, `qwen-7b` share 3, `qwen-image` share 1, both with a steady
stream of work:

```
qwen-7b     |======= 7m30s =======|          |======= 7m30s =======|
qwen-image                         |= 2m30s =|
                                  ^ swap     ^ swap
```

If `qwen-image` runs out of tickets after 40s, `qwen-7b` gets the GPU back
after 40s plus `linger`, not after 2m30s.

## Jobs

The router gives every model an asynchronous job API on top of a synchronous
workload. It stores the request, replays it when the model's turn comes, and
serves the file the workload wrote into the job's folder. It never reads the
file and does not know what the workload does.

```
client                    router                              workload
  | POST /jobs/qwen-image/generate                                |
  |------------------------>| store request on disk              |
  |<-- 202 {id, position} --|                                    |
  |                         |   ... waits for its slice ...      |
  | GET /jobs/{id}          |                                    |
  |<-- {status: queued} ----|                                    |
  |                         |-- POST /generate, X-Job-Dir -----> | writes image.png
  |                         |   (polls GET /progress meanwhile)  | into the folder
  |                         |<-- 200 {"file": "image.png"} ----- |
  | GET /jobs/{id}/result   |                                    |
  |<-- 200 image/png -------|                                    |
```

### Routes

| Route | Description |
|---|---|
| `POST /jobs/{model}/{path}` | Store method, path, query, content type and body (streamed to disk, never buffered; `413` over `maxBodySize`). `202` `{id, model, status: "queued", position, eta_s, ...}`. `404` unknown model, `429` when `maxQueue` tickets wait |
| `GET /jobs/{id}` | The job view, below |
| `GET /jobs/{id}/wait?timeout=S` | Long-poll (default 300 s, 0-3600): the view as soon as the job is finished, or the current view on timeout |
| `GET /jobs/{id}/result` | The output file, content type from its extension, `Content-Disposition` with its name. `409` while not done |
| `DELETE /jobs/{id}` | Queued: cancelled at once. Running: the request to the workload is closed and the job is cancelled. Finished: `409` |
| `GET /jobs[?model=name]` | All jobs, newest first |

Errors from the router itself are `{"detail": "..."}`.

**Job view:** `id`, `model`, `path`, `status` (`queued`, `running`, `done`,
`failed`, `cancelled`), `position` (index among the model's queued jobs, 0 =
next, null when not queued), `eta_s`, `created`, `started`, `finished`,
`elapsed_s` (unix seconds), `progress` (the workload's last progress answer),
`result` (from the workload's answer), `file`, `error`, `upstream_status`.

**Validation at submit.** When the model has a `validate` endpoint and is
loaded right now, the router sends the request there first (10 s timeout).
A non-2xx answer is returned to the client as it is, and no job is created.
When the model is not loaded, or does not answer in time, the job is queued
unchecked; a bad request then fails as soon as it is admitted, with the
workload's `detail` as its `error`. A submit never starts or swaps a model.

**ETA.** A running job's `eta_s` is the workload's own `eta_s` from progress
when it gives one, else the average duration of that model and path's recent
jobs minus the time elapsed. A queued job's `eta_s` adds the remaining time of
the jobs ahead of it in the model's queue and its own average; it is null
without history, and does not include another model's slice or the load time.

### Workload contract

For a job the router calls the model with the stored request replayed to
`{proxy}/{path}?{query}`, the stored method and content type, and:

- `X-Job-Id`: the job id;
- `X-Job-Dir`: the absolute path of the job's folder,
  `<jobs.dir>/<model>/<id>/`, which the router created.

The workload:

1. **Answers synchronously.** It writes its one output file into the folder
   and answers `200` with `{"file": "image.png", "result": {...}}`. `file` is
   a plain file name (no slash, no `..`, not starting with `.`) of a regular
   file in the folder; `result` is copied into the job view.
2. **Refuses with `{"detail": ...}`** and any non-2xx status. The job becomes
   `failed`, with `error` = detail and `upstream_status` = the code.
3. **Stops on disconnect.** When the router closes the connection (job
   deleted, `jobTimeout`), the workload aborts the work. With a `progress`
   endpoint the router then waits until `busy` is false, for up to
   `stopTimeout`, and stops the model if it stays busy.
4. **Keeps no queue.** The router sends one job at a time (per `concurrency`).
5. **Progress** (optional): `GET progress.endpoint` answers
   `{"busy": true, "id": "<job id>", "phase": "...", "step": 3, "steps": 6, "eta_s": 11}`;
   the router polls it every `progress.interval` while a job runs and keeps
   the fields other than `busy` and `id` in the view. An answer whose `id`
   names another job is ignored.
6. **Validate** (optional): `POST validate.endpoint` with the same body,
   content type and query, plus `X-Job-Path: /<path>` so one endpoint can
   check for several run endpoints. `200` accepts; a non-2xx answer with a
   body is passed to the client.

Without a router (`curl`, tests) a workload gets no `X-Job-Dir`; what it does
then is its own business (the simple-ai-server workloads answer with the file).

### Storage

```
<jobs.dir>/<model>/<job id>/<file>                    the output; served by /result
<jobs.dir>/<model>/<job id>/.router/job.json          the record, written atomically
<jobs.dir>/<model>/<job id>/.router/request.body      until the job finishes
```

The layout is fixed so that an agent on the router's machine, told `jobs.dir`
beforehand, can poll `GET /jobs/{id}` and read the file itself once the status
is `done`. Cancelled and failed jobs keep only `job.json`.

- **Restart:** jobs on disk are loaded. Queued ones are queued again in
  creation order; one that was running is marked `failed` with
  "router restarted while the job was running"; finished ones are served as
  before. Shutdown does not change the records, so this holds across a
  restart of the router.
- **Retention:** a finished job is deleted `resultTTL` after it finished
  (default 30 days); `keepJobs` keeps only a model's newest N finished jobs.
  The sweep runs at start and hourly, and only touches folders named like a
  job id under a configured model's folder; such a folder without a record is
  removed once it is `resultTTL` old.

## Config

```yaml
timeShare:
  period: 10m        # one full rotation when every model has work
  minSlice: 1m       # a loaded model keeps the GPU at least this long while it has work
  linger: 2s         # how long an idle active model waits for more work before yielding

jobs:
  dir: ./jobs        # one folder per job; relative to the config file
  resultTTL: 720h    # delete a job this long after it finished
  maxBodySize: 64MB  # per stored request (bytes, or KB/MB/GB)

models:
  qwen-7b:
    share: 3           # default 1
    maxQueue: 100      # default 0 = unlimited
    queueTimeout: 30m  # default 0 = wait forever

  qwen-image:
    share: 1
    concurrency: 1     # default 0 = unlimited for sync requests; jobs run one at a time regardless
    maxQueue: 16
    jobTimeout: 1h     # default 0 = none
    resultTTL: 168h    # per-model overrides of the jobs section
    keepJobs: 200
    maxBodySize: 200MB
    linger: 5s
    progress: {endpoint: /progress, interval: 1s}
    validate: {endpoint: /validate}
```

All new fields are optional. With defaults (`share: 1` everywhere) the router
does plain round-robin with equal slices. `busyCheck` stays supported for
servers that keep their own queue.

`GET /running` reports per model: `queued`, `admitted`, `active`,
`sliceRemaining` (while another model waits) and `jobs` counts by status.

## Tests

`internal/router/router_test.go` covers the scheduler (alone model never
swapped, models alternate by share, a request for a yielding model waits its
turn, idle yield with and without linger, next model is the one waiting
longest, concurrency, cancel leaves the queue, `maxQueue`, `queueTimeout`,
unload). `internal/server/jobs_test.go` covers the jobs (submit, wait, result,
folder layout, `202` while another model is active and the result outliving
the model, cancel queued and running, `429`, `413`, validation only when
loaded, progress, workload failures and bad file names, `jobTimeout`, a stuck
workload after a cancel, restart recovery, retention and `keepJobs`,
`/running`). The fake server (`internal/fakeserver`) plays the workload.

## Open points

- **Slice accounting** ignores overrun: a request that runs past the slice
  end is not charged to the model's next slice.
- **Queue ETA** does not know about other models' slices or load time.
- **The old busyCheck bypass** stays for models that have a `busyCheck`; it
  goes when the last such server has moved to jobs.
