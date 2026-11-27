# AGENTS.md

Mosquitto 2.1 broker plugin publishing per-client metrics (drops, queues, disconnect reasons). See README.md for usage.

## Working rules

- **Test-driven development.** Write or extend a failing check in `test/run-test.sh` first, watch it fail, then change `src/`. Every behaviour change needs a test that would catch its regression. Assert exact numbers, not "> 0", wherever the broker is deterministic.
- **Minimal, concise docs.** Update README.md only when user-facing behaviour, options or output change. No new doc files. Comment the *why* in code, not the *what*.
- **No helper scripts.** Put commands in README.md instead. `test/run-test.sh` is the test suite, not a helper.
- Run `pre-commit run --all-files` before committing.

## Commands

```sh
docker build -f docker/Dockerfile -t mosquitto-client-metrics:2.1.4 .   # build (compiles in-tree)
test/run-test.sh                                                         # test (needs the image)
```

## Constraints

- Single source file: `src/mosquitto_client_metrics.c`. C99, tabs, Mosquitto style.
- Code under `#ifdef CM_WITH_INTERNALS` reads broker-private `struct mosquitto`. It must keep working when that is compiled out, and new fields read from it must be covered by the runtime `layout_check()`.
- The ACL callback must always return `MOSQ_ERR_PLUGIN_IGNORE`. It must never affect access decisions.
- Never register for an event and return an error from it. Never block in callbacks: they run on the broker's only thread.
- To bump Mosquitto, change `MOSQUITTO_VERSION`, `MOSQUITTO_SHA256` and `ALPINE_VERSION` together in `docker/Dockerfile` to match upstream `docker/2.1-alpine/Dockerfile`.

## Git Flows

- Use worktrees, commit in the worktree.
- Do not push to any remotes.
- Merge to main from worktrees will be performed by user, not agent.
- When work is done, cleanup the worktrees (after assuring the changes have been incorporated in main).
