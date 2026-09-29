# Job inputs: the router owns a job's files at both ends

Status: **plan**, 2026-09-29. `simple-ai-server` (the router's job API in one process, in the
simple-ai-server repository) already does this, and its workloads (`yue2`, `longcat-avatar`) already
expect it; this is the router's side of the same change.

## Why

Workloads are local scripts on the router's machine, so a file never needs to travel to them over HTTP.
Today a multipart submit is stored, then replayed to the workload as multipart: the upload is copied
three times (router body, workload spool, workload folder), and every workload re-implements multipart
parsing, upload caps and name checks. The router already owns the output side (the workload writes one
file into `X-Job-Dir`); it now owns the input side too.

## The contract, as workloads see it

- The workload receives **JSON**. File inputs are paths relative to the job folder, under `inputs/`:

  ```json
  {"prompt": "...", "audio": "inputs/audio.mp3", "image": "inputs/image.png", "seconds": "12"}
  ```

- Text form fields stay strings (workloads coerce numbers). A field that appears several times becomes a
  list, in order. A JSON submit is passed on as is.
- `POST /validate` gets **the same body and headers as the run**: `Content-Type: application/json`,
  `X-Job-Path`, and now also `X-Job-Id` and `X-Job-Dir`, called **after** the inputs are saved, so it can
  check the files themselves (decode the audio, read the image).
- The workload reads the inputs in place and never writes into `inputs/`. It writes its one output file
  into `X-Job-Dir`, as today.

## What the router does

### Submit (`Manager.Submit`)

1. Store the body as today (`maxBodySize` still caps it: it is the upload).
2. If the content type is `multipart/form-data`, split it:
   - each **file part** → `<job>/inputs/<field><suffix>`: `<field>` must match
     `^[A-Za-z0-9_][A-Za-z0-9_-]{0,63}$` (else 422); `<suffix>` is the upload's extension, lower-cased,
     kept only if it matches `^\.[a-z0-9]{1,10}$`, else none. **Never the client's file name.** A repeated
     field gets `-2`, `-3`, ... (`inputs/extra.txt`, `inputs/extra-2`).
   - each **text part** → a JSON string value, except `<field>@ref` parts (below).
   - write the JSON as `.router/request.body`, set the job's content type to `application/json`, and
     record the saved names in `job.json` (`"inputs": ["audio.mp3", ...]`).
   - Limits as in simple-ai-server: at most 64 files and 256 fields; a bad multipart body is 400.
3. Validate (step below). On a 4xx, remove the whole job folder: the job was never created.

### References (`resolve`, `rewriteMultipart`)

- A multipart `<field>@ref` = `job:<id>` or `artifact:<sha256>` resolves to a **file in `inputs/`**
  instead of a file part: `<job>/inputs/<field><suffix of the referenced file>`, as a hard link to the
  referenced output or artifact when on the same filesystem (else a copy), and the JSON gets
  `"<field>": "inputs/<field><suffix>"`. No more `request.resolved` for multipart.
- JSON `{"$ref": ...}` values **stay as today** (inlined as base64, up to `jobs.maxInlineRef`), because
  qwen-image and minimax-h3 take images as base64 in JSON. When those workloads move to job inputs, the
  same `$ref` can become a path; that is a separate change.
- A job blocked on a reference is validated when it unblocks, after its references are resolved into
  `inputs/` (today's order), not at submit.

### Validate (`Manager.validate`)

Send the (JSON) body with `Content-Type`, `X-Job-Path`, `X-Job-Id` and `X-Job-Dir`.

### Run (`Manager.call`), finish, retention

- `call` is unchanged: it already sends `X-Job-Id` and `X-Job-Dir`, with the stored body.
- A failed or cancelled job keeps only `.router/` (as today), so `inputs/` goes with the rest.
- A done job keeps `inputs/` next to its output until retention removes the folder; the job view does
  not list them (`file` stays the one output).
- `pinnedLocked`, `Sweep` and artifact accounting: hard-linked inputs are ordinary files of the job's
  folder; an artifact's own file is unaffected by removing the link.

## Tests (Go, next to the existing job tests)

- multipart with two files and text fields → `inputs/audio.mp3`, `inputs/image.png`; the fake workload
  gets JSON with those paths and reads the bytes back; `job.json` lists the inputs.
- the client's file name is never used (`../../evil name.MP3` → `inputs/audio.mp3`); a bad field name → 422.
- a repeated field → a list of paths, `-2` suffix.
- `/validate` sees `X-Job-Dir` and the saved files; a 422 there leaves no job folder.
- `<field>@ref=job:<id>` → `inputs/<field><suffix>` linked to that job's output; a blocked job's files
  appear when it unblocks, before its validation.
- a JSON submit, and a JSON `$ref`, behave as today.
- a failed / cancelled job removes `inputs/`; a done one keeps it.

`internal/fakeserver` needs the matching change: read JSON, and read file fields from `X-Job-Dir`.

## Reference implementation

`simple_ai_server/jobs.py` in the simple-ai-server repository (`save_inputs`, `Jobs.submit`), with its
tests in `tests/test_simple_ai_server.py`; the workload side is `contract.job_input` in each workload's
`contract.py`. simple-ai-server has no references or artifacts, so the reference part is new here.
