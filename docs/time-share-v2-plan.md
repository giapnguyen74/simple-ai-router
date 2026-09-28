# Time-share v2: batch scheduling, pipelines and an artifact cache

Status: **part A implemented** (phases 0-2: `internal/router/batch.go`, `stats.go`); parts B and
C are next. Builds on [time-share-plan.md](time-share-plan.md) (jobs). The workload side is fixed by `simple-ai-server/GUIDELINE.md`:
synchronous workers, one file per call, no queue, no stored results, and *"a later call that needs an
earlier output gets it in its request"*. That moves two jobs onto the router: the **job queue** (done)
and the **artifacts** that flow between calls (part B).

## Goal

One GPU, several heavy workloads (qwen-image, yue2, minimax-h3, longcat-avatar, LLMs), one model loaded
at a time. Minimise model switches while every model's work still waits a bounded, predictable time.
The wait is the price of sharing the GPU; the bound is what we control.

## What is wrong with v1 slices

| Gap | Effect |
|---|---|
| Slices are time-based, not work-based | A model can lose the GPU with work still queued and be loaded again in the same rotation |
| One ticket triggers a swap | A single short job for B ends A's slice; both pay a load |
| Slices ignore job length | A 20 min job admitted with 1 min of slice left overruns by 19 min, uncharged |
| Load time is unknown | `minSlice` is guessed per setup |

## Part A: batch scheduler

### A1. Batches are booked at arrival

Batches are numbered; batch 1 is the one running. Every ticket is **booked into a batch when it
arrives** (A3). There is no limit on how many batches are booked ahead (hours of renders are fine);
only the optional `maxQueue` and `maxWait` (A6) refuse work.

Each model in a batch is loaded at most once and runs its booked work to the end, so switches per
batch ≤ number of models in it.

If the GPU would otherwise sit idle (running batch done, loaded model has nothing), the next batch
starts at once: no waiting window. With only one model having work, nothing changes from today: it
never yields and stays until `ttl`. When another model starts to wait for a model that was alone, the
running batch starts over from that moment: the alone model keeps one cycle of its waiting work in
it, and the rest moves to the next batch.

### A2. Order within a batch

1. The **loaded model first**: the boundary between two batches costs no switch.
2. Then by oldest ticket (the model that has waited longest).
3. The **last** slot goes, if possible, to a model that has tickets booked in the following batch, so the
   next boundary is free too.

### A3. Booking to the cycle

`batch.cycle` (e.g. `15m`) is the target length of one batch. A batch is shared only among the models
that have tickets booked in it; nothing is reserved for models that are not there:

```
budget(k) = cycle − Σ loadTime(models in batch k that need a switch)
quota(m, k) = budget(k) × share(m) / Σ share(models booked in batch k)     max-min fair
```

Max-min fair: a model that needs less than its share keeps only what it needs, and the rest goes to the
others. A model alone in a batch has the whole cycle.

**Booking is by time, not by job count.** A ticket's cost e is its own estimate (A5): the workload's
`eta_s` when it gave one, else the learned average of its (model, path). A new ticket of model m books
the earliest batch k ≥ 2 (or the running batch by joining, A4) where m fits within quota(m, k) −
m's debt (A4), counting m itself as booked there, and **no earlier than m's last booked ticket**: a
model's tickets keep their arrival order, a small ticket never jumps into a gap ahead of a bigger one of
the same model. A ticket larger than its quota books alone into the first batch where m has nothing
booked, so a long render still runs; that batch overruns once.

**Newcomers get in, floods move back.** When a model joins a batch, the quotas in that batch shrink; each
model that is now over its quota has its **last** booked tickets moved to the next batch (and so on
down the chain). So in the example below, one B request arriving while A has booked all of batch 2
takes its share of batch 2 and A's surplus moves to batch 3. A ticket moves later only while its model
is over its fair share of that batch, so light models keep their place and heavy ones absorb the delay.
There is no hard promise: if many models join a batch, even a light model's ticket can move.

**Unknown ETA.** A ticket with no estimate counts as `defaultEta` (per model, default 1 min), and only
one unestimated ticket per (model, path) is booked into a batch, so the first run is measured before
more are booked on a guess.

**Estimates change, bookings do not.** When a (model, path) average moves, new bookings use it;
tickets already booked keep their batch. A batch that turns out overfull runs long and the model's debt
shortens its next quota; one that turns out underfull (work finished early) simply ends early and the
next batch starts.

Why not fixed job counts per batch ("4 A, 3 B, 1 C" from each model's average): one model serves jobs of
very different sizes (yue2 `generate` ~10 min vs `decode` ~30 s, qwen-image 4 vs 50 steps, longcat 1 vs
3 segments), so a count per model mis-sizes batches. Counting time with per-(model, path) averages keeps
the count's robustness and gets the size right. `/running` still shows batches as counts
("A: 4 jobs ≈ 5m").

### A4. Running a batch

- **Booked tickets are guaranteed.** Every ticket booked in a batch when it starts runs in that batch.
  Only its own model's overrun (below) can move it out, never another model's work.
- **The running batch stays open, in unbooked time only.** A new ticket of a model whose turn in the
  running batch is not over yet (the model on the GPU, or one whose turn is still ahead) joins it when

  ```
  time used so far + booked work not yet run (all models) + new ticket ≤ cycle
  ```

  Finished work does not count, only what is still to run. The joiner runs in its model's turn, so a
  new A can run before a booked B, but only in time no one booked: B still runs in this batch. Otherwise,
  and for models whose turn is over or that have nothing in the batch (joining would cost a switch),
  it books a later batch. Chat to the loaded model and agents iterating on it are served in the
  current turn while there is room.
- **No contention:** while no other model has anything booked, a new ticket of the running model is
  admitted at once (up to `concurrency`), as today.
- The model's quota is charged with the **actual** GPU time of admitted work, not the estimate. When a
  job runs longer than predicted the model's remaining quota shrinks; when the quota is used up, the
  model stops admitting (running work finishes), its remaining tickets move to the front of its
  bookings in the next batch, and the next model loads. Joiners that run longer than estimated make the
  batch overrun (later models start late but still in the batch) and become their model's debt.
- **Sync requests** (`/v1/...`) have no size: they are charged by their real duration, like jobs.
- **Overrun is paid back:** time a model uses beyond its quota (a job longer than predicted, or its
  ticket that was larger than its quota) is a debt taken off its quota in the next batch, capped at one `cycle`. Unused
  quota is not carried over. So a model gets its share per batch on average and cannot bank time.
- `concurrency`, `maxQueue`, `queueTimeout`, `drainTimeout` keep their v1 meaning.

### Fairness at the boundary

The last model of a batch runs first in the next one (A2), so it may run two turns back to back. That is
still one quota per batch, not extra time: joining uses only unbooked time, booking is bounded by the quota, and overrun
is paid back. The cost is only a longer gap for the others.

### A5. Learning ETAs

The plan is only as good as the estimates, so the router learns them from every finished job:

| Source | Used when |
|---|---|
| `eta_s` in the workload's `/validate` answer (computed from its timing file) | the model is loaded at submit |
| EWMA of actual durations per (model, path), planned at its p75 | otherwise |
| `defaultEta` | nothing learned yet (see A3) |

- A per-model **correction factor** (EWMA of actual / estimated) is applied to workload estimates, so a
  workload that is always optimistic is corrected.
- **Load time** per model is learned the same way (start → healthy), with `loadTime` in config as the
  first guess.
- Stored in `<jobs.dir>/.router/stats.json`, so a restart keeps what was learned.

After a few batches the estimates settle and batches land close to `cycle`. `/running` shows what is
needed to check that: per batch planned vs actual length and jobs per model; per model quota, used, debt, load time; per
(model, path) average duration and ETA error;
switches per hour and switch overhead (load time / wall time).

### A6. Reporting the wait, and busy answers

**Prediction.** The job view gains the booked batch and the expected start (the booked batches
replayed with learned ETAs and load times; recomputed on demand, cheap):

```json
{"status": "queued", "batch": 2, "position": 3, "starts_in_s": 840, "eta_s": 1020}
```

`batch` is 1 for the running batch; `starts_in_s` is the expected start; `eta_s` = `starts_in_s` + its
own run time. Both are estimates, not promises (A3).

**Jobs are never refused for being out of the batch**; they queue and report `starts_in_s`. Busy
answers only in the two cases below, and only on a wait the router is sure of: guessed costs
(`defaultEta`) and load times never measured count as nothing, and a model's own load is not a wait.

- **Sync requests** (`/v1/...`): served at once by joining the running batch or without contention; otherwise they wait if the predicted start
  is under `syncMaxWait` (per model, default `60s`), else `503` at once with `Retry-After` and
  `{"detail": "<model> is busy: next turn in ~14m", "starts_in_s": 840}`.
- **Jobs over `maxWait`** (per model, default off): a submit whose predicted start is later gets `429`
  with `Retry-After` and `starts_in_s`: a limit in time rather than in count, like `maxQueue`.

`GET /running` adds a `batches` view: the running batch (order, per-model quota and used, time left)
and the booked batches (tickets and booked time per model, expected start).

### Example

`cycle: 15m`, equal shares. Batch 1 runs A. A has booked 40 min of work: all 15 min of batch 2, then
batches 3 and 4. One B request (3 min) arrives.

```
before:   batch 2 = [A 15m]                 batch 3 = [A 15m]        batch 4 = [A 10m]
B books:  batch 2 now shared by A and B (7.5m each; B needs 3m, A gets the other 12m)
after:    batch 2 = [A 12m][B 3m]           batch 3 = [A 15m]        batch 4 = [A 13m]
          A loaded: runs first, no switch   A's last 3m moved down the chain
```

B runs in batch 2; the A flood absorbed the delay. Batch 3 starts with B loaded only if B has more
booked; otherwise the router switches back to A once.

## Part B: pipelines and the artifact cache

The router already stores every job's output. Part B lets a request **refer** to files the router holds
instead of carrying them, and wait for a job that is not done yet. Workloads do not change: at dispatch
the router puts the bytes in the request, exactly as a client would.

### B1. References

| Body | Reference | Replaced by, at dispatch |
|---|---|---|
| multipart | a text part `<field>@ref` = `job:<id>` or `artifact:<sha256>` | a file part `<field>` with the file's bytes, name and content type |
| JSON | a value `{"$ref": "job:<id>"}` anywhere | the file as base64 (what `decode_image` takes); only below `jobs.maxInlineRef` (default 32 MB) |

```sh
curl -XPOST :8080/jobs/yue2/decode -F bundle@ref=job:e3969fd18827 -F format=mp3
```

The stored body keeps the references; the resolved body is streamed to the workload (never buffered).
`maxBodySize` applies to what the client sent.

### B2. Dependencies

A reference to a job not yet `done` makes the new job **`blocked`**. It is **booked at submit**, like
any ticket, into the batch of its dependency when it fits there right after it (same model: the same
turn), else a later one; it is admitted only once every dependency is `done`. So a pipeline submitted at
once is already booked inside its batch: a blocked job is not new work. If its dependency moves to a
later batch, it moves with it. A failed or cancelled dependency fails it. The view shows `depends_on`. An agent can submit a
whole pipeline (`plan` → `generate` → `decode`) at once, and yue2's stages run in one turn.

### B3. Artifact store

```
<jobs.dir>/.artifacts/<sha256[:2]>/<sha256>          bytes
<jobs.dir>/.artifacts/<sha256[:2]>/<sha256>.json     name, content type, size, created, last used
```

- `POST /artifacts` (stream; same bytes → same id), `HEAD/GET /artifacts/{sha}`, `DELETE` (`409` while
  referenced by a queued or blocked job).
- Finished job outputs are hard-linked in (copy across filesystems), so `job:<id>` survives the job
  folder being swept.
- Pinned while a queued/blocked job refers to it; else LRU, evicted beyond `artifacts.maxSize` or after
  `artifacts.ttl`.

### B4. Result cache (opt-in)

Per model `cache: {paths: [...]}`: a submit with the same (model, path, content type, resolved body
hash) as a `done` job returns that job at once, without a ticket. Only for requests with an explicit
`seed`.

## Part C: simple-ai-server

- Workloads add `eta_s` to their `/validate` answer from their timing file (A5).
- `simple-ai-server` implements what the skills use from part B (`@ref`, `$ref`, `blocked`,
  `/artifacts`); skill clients switch from download-and-reupload to references.

## Phases

| # | Work | Done when |
|---|---|---|
| 0 | Commit v1 as it is | clean tree, tests green |
| 1 | A5 ETA and load-time learning, stats in `/running` (behaviour unchanged) | estimates and errors visible on the real GPU |
| 2 | A1-A6 batch scheduler replacing slices, wait prediction, busy answers | tests: loaded model first; each model loaded once per batch; a newcomer takes its share of the next batch and the over-share model's last tickets move back (B in batch 2 under an A flood); arrival order kept per model; a ticket larger than its quota books alone; booked tickets always run in their batch; a joiner runs in the current turn only in unbooked time (5 A + 1 B booked, new A runs before B, B still in the batch), else books later; a model whose turn is over books the next batch; a batch that finishes early starts the next; blocked jobs booked with their dependency; actual-time charging; single model never yields; sync `503` past `syncMaxWait`; `429` past `maxWait` |
| 3 | B1 references + B3 artifact store | yue2 `generate` → `decode` by reference; 200 MB streaming rewrite under `-race` |
| 4 | B2 dependencies | a pipeline submitted at once runs in one turn (switch count asserted) |
| 5 | Part C | skills tests green against both servers |
| 6 | B4 result cache | opt-in |
| later | adaptive cycle from load times, size-based ETA (`eta: {size: [...]}`), suspend/resume endpoints | only if phase 1-2 data asks for it |

## Config after v2

```yaml
batch:
  cycle: 15m          # target batch length
jobs:
  dir: ./jobs
  resultTTL: 720h
  maxInlineRef: 32MB
artifacts:
  maxSize: 50GB
  ttl: 720h
models:
  yue2:
    share: 1          # weight of the batch budget (v1 meaning kept)
    defaultEta: 5m    # first guess until learned
    loadTime: 60s     # first guess until learned
  qwen-7b:
    share: 1
```

`timeShare` is deprecated: a config that still has it loads with a warning, `period` becomes
`batch.cycle`, `linger` becomes `batch.linger` (how long an idle model waits for more work before its
turn ends, default `2s`), and `minSlice` is ignored.

## Risks

- Bad early estimates make early batches miss the cycle; bounded by at-least-one-ticket and actual-time
  charging, and corrected as jobs finish.
- A single job longer than the cycle (a long longcat render) makes that batch long; this is visible in
  `/running`, and `jobTimeout` bounds it.
- Hard links need one filesystem; fall back to copy.
