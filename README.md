# mosquitto-client-metrics

A Mosquitto 2.1 broker plugin that publishes **per-client** metrics, aimed at
finding which clients are losing messages and why.

For every client id it reports:

| Area | Metrics |
|---|---|
| Message loss | `dropped` (broker discarded the message while the client was connected), `dropped_disconnected` (persistent-session queue full), `deferred` (not written immediately: queued or dropped) |
| Backpressure | outgoing inflight / queued counts and bytes, `socket_packets_pending` (packets the broker can't write to the client's socket yet), `dropping` flag |
| Traffic | `msgs_in`/`bytes_in` (published by the client), `msgs_out`/`bytes_out` (sent to it), `routed` (messages the broker tried to deliver to it) |
| Connection health | connects, disconnects broken down by reason: `keepalive_timeout`, `conn_lost`, `protocol_error`, `session_taken_over`, `administrative`, `auth`, `clean` |

Totals are reported along with deltas for the last interval. A broker-wide
`summary` lists the worst clients, ranked by dropped messages and then by
abnormal disconnects.

## Quick start (Docker)

```sh
docker compose up --build          # broker on :1883 with the plugin loaded
mosquitto_sub -t '$SYS/broker/client-metrics/#' -v
```

Each value is its own retained topic, so tools such as mqtt-exporter can read
them directly:

```
$SYS/broker/client-metrics/clients/slow-client/connection/state     1
$SYS/broker/client-metrics/clients/slow-client/totals/dropped       430
$SYS/broker/client-metrics/clients/slow-client/queues/out_queued    60
$SYS/broker/client-metrics/clients/slow-client/disconnect_reasons/keepalive_timeout 0
$SYS/broker/client-metrics/summary/clients_connected                3
$SYS/broker/client-metrics/summary/worst_clients/0/client_id        slow-client
```

The topics follow Mosquitto's own `$SYS` conventions:
* They live under `$SYS/broker/`.
* Flags are `1`/`0`. `connection/state` matches Mosquitto's bridge and
  connection-state topics.
* Messages are retained and published at QoS 2.
* A value is only republished when it changes. The exception is
  `connection/state`, which is published the moment a client connects or
  disconnects.

Layout:
* `…/clients/<client id>/…` holds per-client values. Any `+` or `#` in a client
  id is replaced with `_`.
* `…/summary/…` holds broker totals and `worst_clients`, ranked from 0.
* With `plugin_opt_format json`, each base topic (`…/clients/<id>`,
  `…/summary`) instead holds one JSON document with exactly this structure;
  `both` publishes both. JSON documents are republished every interval. A JSON
  `null` (e.g. no username) has no topic.
* Values that stop applying are cleared, for example `connected_for_s` once a
  client disconnects.

The broker also logs a NOTICE line whenever a client drops messages:

```
client-metrics: client slow-client (connected) dropped 430 messages in the last 10 s (routed 500, sent 10).
```

## Reading the numbers

| You see | It usually means |
|---|---|
| `dropped` > 0, `out_inflight` = `out_inflight_max`, `out_queued` full | The client is too slow to acknowledge QoS 1/2. It's a slow consumer, or its acks are being lost. |
| `dropped` > 0 with high `socket_packets_pending` | The broker can't write to the socket fast enough: TCP backpressure, a slow link, or a stalled client. QoS 0 drops happen this way. |
| `dropped_disconnected` > 0 | A persistent session overflowed `max_queued_messages` while the client was disconnected. |
| `keepalive_timeout` / `conn_lost` disconnects climbing | The network path is losing packets or the client is hanging. |
| `routed` ≫ `msgs_out` without drops | Messages are still queued, waiting for the client. |

Drops happen when the broker's limits are reached (`max_inflight_messages`,
`max_queued_messages`, `max_queued_bytes`, …). Tuning those changes *when*
messages are dropped. It doesn't fix the client or link that's causing them.

## Configuration

```conf
# Must be listed BEFORE any auth/ACL plugin (see Caveats).
plugin /usr/lib/mosquitto_client_metrics.so
plugin_opt_interval 10                       # seconds between publishes (default 10)
plugin_opt_topic_prefix $SYS/broker/client-metrics  # default
plugin_opt_format topics                     # topics (default), json or both
plugin_opt_disconnected_retention 3600       # forget clients disconnected this long, clearing their retained topics
plugin_opt_retain true                       # publish metrics retained
plugin_opt_summary_top 10                    # clients listed in summary
plugin_opt_log_drops true                    # NOTICE log line per dropping client
plugin_opt_enabled true                      # false: load but collect nothing (zero overhead)
```

Options are read at startup only. Mosquitto 2.1 doesn't reload plugins or
`plugin_opt_*` on SIGHUP, so restart the broker to change them. The plugin
keeps running, with its counters intact, across a SIGHUP.

Use it as a global plugin. If you use `per_listener_settings true`, load it
with `global_plugin`.

## How it works

The 2.1 plugin API has no "message dropped" event, so the plugin works it out
from the events that do exist:

* Mosquitto runs an ACL READ check for a client right before it inserts a
  message into that client's queue. The plugin's callback returns
  `MOSQ_ERR_PLUGIN_IGNORE`, so it never affects access decisions, and it
  records the start of a routing.
* The broker is single-threaded, and the insert happens synchronously. By the
  time the next plugin event fires, the decision has already been made.
* The broker increments a per-client counter (`last_cmsg_id`) exactly once
  each time it accepts a message for that client. If the counter didn't move,
  the message was dropped.

Reading `last_cmsg_id` and the queue state means reading the broker's internal
`struct mosquitto`. That's why the Docker build compiles the plugin **inside the
Mosquitto 2.1.4 source tree**, using the same Alpine base, packages and CMake
flags as the official `eclipse-mosquitto:2.1.4-alpine` image. At startup the
plugin also checks its view of the struct against the broker's public
accessors. If anything doesn't line up, it logs a warning and turns off
`dropped`/`queues`, and the public-API metrics keep working.

### Upgrading Mosquitto

Bump `MOSQUITTO_VERSION`, `MOSQUITTO_SHA256` and `ALPINE_VERSION` together in
`docker/Dockerfile`. Match them to upstream's
`docker/2.1-alpine/Dockerfile` for that release. Re-pin `ALPINE_DIGEST` and
`MOSQUITTO_DIGEST` with `docker buildx imagetools inspect <image:tag>`, then
rerun the test.

### Building OCI images

The Dockerfile only uses instructions from the OCI image spec, so any builder
works. It sets the `org.opencontainers.image.*` labels, and the test checks them.
Pass the per-build values as build args:

```sh
ARGS="--build-arg REVISION=$(git rev-parse HEAD) --build-arg CREATED=$(date -u +%FT%TZ) --build-arg SOURCE_URL=<repo url>"

# Podman/Buildah: OCI format by default
podman build $ARGS -f docker/Dockerfile -t mosquitto-client-metrics:2.1.4 .

# Docker: the default store saves Docker media types. For OCI output, use a
# docker-container builder (or the containerd image store):
docker buildx create --name oci --driver docker-container
docker buildx build --builder oci $ARGS -f docker/Dockerfile \
  --output type=oci,dest=mosquitto-client-metrics.oci.tar .
# or push: --output type=image,name=<registry>/mosquitto-client-metrics:2.1.4,oci-mediatypes=true,push=true
```

Keep `PLUGIN_VERSION` in the Dockerfile in step with the source; the test
fails if they differ.

### Building without internals

`make MOSQUITTO_INCLUDE=/path/to/mosquitto-2.1.x/include` builds a portable
public-API-only plugin, or use `-DCLIENT_METRICS_WITH_INTERNALS=OFF` in-tree.
It needs the cJSON and uthash headers. That build has no `dropped` or `queues`
fields; `deferred` (queued *or* dropped) is the closest stand-in.

## Testing

```sh
docker build -f docker/Dockerfile -t mosquitto-client-metrics:2.1.4 .
test/run-test.sh
```

The test uses tiny limits (`test/mosquitto-test.conf`) and creates these clients:
a healthy subscriber, a frozen QoS 1 subscriber, a frozen QoS 0 subscriber
getting 64 KB messages, a disconnected persistent session (which then reconnects),
and a client that hits a keepalive timeout. It checks each client's exact
numbers. It also checks that the per-client drops add up to the broker's own
global `$SYS/broker/publish/messages/dropped`. That broker runs with
`plugin_opt_format both`, and the test checks that each topic tree matches its
JSON exactly, with no stale topics left behind. Finally it sends the broker a
SIGHUP. It then starts two more brokers: one with `plugin_opt_enabled false`
(`test/mosquitto-disabled.conf`), and one with the default format, topics only
(`test/mosquitto-topics.conf`).

## Caveats

* **Plugin order matters.** Mosquitto stops calling ACL callbacks at the first
  plugin that makes a decision. If an ACL plugin (dynamic-security, etc.) is
  loaded before this one, the plugin never sees deliveries. Put its `plugin`
  line first. The `acl_file`/`password_file` options are fine either way,
  because they're registered after `plugin` lines.
* Topics starting with `$` (including this plugin's own output) aren't counted.
* `routed` is counted before any later ACL plugin decides. A delivery that a
  later plugin denies shows up as routed/deferred, but not as dropped.
* `msgs_out` includes QoS 1/2 resends after a reconnect.
* Bridge connections bypass ACL checks, so they have no routed/dropped counts.
* Cosmetic: Mosquitto 2.1.4 logs the `client-offline` registration as
  `'connect' events`. That's a broker bug; the plugin is registered correctly.
