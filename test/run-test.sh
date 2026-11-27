#!/bin/sh
# End-to-end test: provokes known drop scenarios and checks the plugin's numbers.
# Usage: test/run-test.sh   (requires the image built by docker/Dockerfile)
set -eu
IMAGE=${IMAGE:-mosquitto-client-metrics:2.1.4}
NAME=cm-test-$$
NAME_OFF=cm-test-off-$$
NAME_TOPICS=cm-test-topics-$$
N=500
HERE=$(cd "$(dirname "$0")" && pwd)

# shellcheck disable=SC2329 # invoked via trap
cleanup() { docker rm -f "$NAME" "$NAME_OFF" "$NAME_TOPICS" >/dev/null 2>&1 || true; }
trap cleanup EXIT

docker run -d --name "$NAME" -v "$HERE/mosquitto-test.conf:/mosquitto/config/mosquitto.conf:ro" "$IMAGE" >/dev/null
sleep 1
x() { docker exec "$NAME" "$@"; }
xd() { docker exec -d "$NAME" "$@"; }

# Offline persistent session: subscribe once (QoS1, clean session off), then leave.
x mosquitto_sub -i offline-client -c -q 1 -t 'test/#' -E
# Healthy consumer.
xd mosquitto_sub -i fast-client -q 1 -t 'test/#'
# Stalled consumer: subscribes then gets frozen, never PUBACKs.
xd mosquitto_sub -i slow-client -q 1 -k 60 -t 'test/#'
# Client that will hit a keepalive timeout (frozen, keepalive 2 s).
xd mosquitto_sub -i keepalive-client -k 5 -t 'other/#'
sleep 1
for c in slow-client keepalive-client; do
	x sh -c "kill -STOP \$(pgrep -f '^mosquitto_sub -i $c ')"
done

x mosquitto_pub -i publisher -q 1 -t test/data -m hello --repeat $N
sleep 9   # > 1.5 x keepalive (7.5 s), and > publish interval

get() { x mosquitto_sub -t "\$SYS/client-metrics/$1" -C 1 -W 3; }
sysdropped=$(x mosquitto_sub -t "\$SYS/broker/publish/messages/dropped" -C 1 -W 3)

echo "--- per-client metrics ---"
for c in fast-client slow-client offline-client keepalive-client publisher; do
	echo "$c: $(get clients/$c)"
done
echo "--- summary ---"
get summary
echo "--- broker \$SYS dropped: $sysdropped"
echo "--- plugin log lines ---"
docker logs "$NAME" 2>&1 | grep client-metrics

# Assertions (JSON parsed on the host with python3)
fail=0
check() { # client section key expected
	v=$(get "clients/$1" | python3 -c "import json,sys; print(json.load(sys.stdin)['$2']['$3'])")
	if [ "$v" = "$4" ]; then echo "PASS $1 $2.$3=$v"; else echo "FAIL $1 $2.$3=$v (expected $4)"; fail=1; fi
}
# Online client: broker keeps max_inflight (10) in flight plus
# max_queued_messages (50) queued *beyond* the inflight window => 10 + 60.
check fast-client totals msgs_out $N
check fast-client totals dropped 0
check slow-client totals routed $N
check slow-client totals msgs_out 10
check slow-client queues out_inflight 10
check slow-client queues out_queued 60
check slow-client totals dropped $((N - 70))
# Offline persistent session: only max_queued_messages (50) are kept.
check offline-client totals routed_offline $N
check offline-client queues out_queued 50
check offline-client totals dropped_offline $((N - 50))
check keepalive-client disconnect_reasons keepalive_timeout 1
check publisher totals msgs_in $N

echo "--- phase 2: QoS0 socket backpressure + persistent-session reconnect ---"
# Frozen QoS 0 subscriber: nothing inflight/queued, the broker drops once its
# unsent packet backlog for the client reaches max_queued_messages.
xd mosquitto_sub -i qos0-client -q 0 -t 'big/#'
sleep 1
x sh -c "kill -STOP \$(pgrep -f '^mosquitto_sub -i qos0-client ')"
x sh -c 'head -c 65536 /dev/urandom > /tmp/big.bin && mosquitto_pub -i bigpub -q 0 -t big/data -f /tmp/big.bin --repeat 400'
# Reconnect the persistent session: it should receive its 50 queued messages,
# with no new routings or drops counted.
x mosquitto_sub -i offline-client -c -q 1 -t 'test/#' -C 50 -W 5 >/dev/null
sleep 3
sysdropped=$(x mosquitto_sub -t "\$SYS/broker/publish/messages/dropped" -C 1 -W 3)
q0=$(get clients/qos0-client | python3 -c "import json,sys; print(json.load(sys.stdin)['totals']['dropped'])")
echo "qos0-client: $(get clients/qos0-client | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['totals'], d['queues'])")"
if [ "$q0" -gt 0 ]; then echo "PASS qos0-client dropped=$q0 (>0)"; else echo "FAIL qos0-client dropped=$q0"; fail=1; fi
check offline-client totals routed_offline $N
check offline-client totals routed 0
check offline-client totals msgs_out 50
check offline-client totals dropped 0
check offline-client totals dropped_offline $((N - 50))
check offline-client totals connects 2

total=$(( (N - 70) + (N - 50) + q0 ))
if [ "$sysdropped" = "$total" ]; then echo "PASS broker global \$SYS dropped=$sysdropped == sum of per-client drops"; else echo "FAIL broker global dropped=$sysdropped, expected $total"; fail=1; fi

echo "--- phase 3: SIGHUP reload ---"
# Mosquitto 2.1 doesn't reload plugins or plugin options on SIGHUP. The plugin
# must keep running, keep its counters and keep publishing.
before=$(get summary | python3 -c "import json,sys; print(json.load(sys.stdin)['timestamp'])")
docker kill -s HUP "$NAME" >/dev/null
sleep 3
if docker logs "$NAME" 2>&1 | grep -q 'Reloading config'; then echo "PASS broker reloaded config"; else echo "FAIL broker did not reload"; fail=1; fi
after=$(get summary | python3 -c "import json,sys; print(json.load(sys.stdin)['timestamp'])")
if [ "$after" -gt "$before" ]; then echo "PASS still publishing after SIGHUP"; else echo "FAIL summary not updated after SIGHUP ($before -> $after)"; fail=1; fi
check fast-client totals msgs_out $N
check slow-client totals dropped $((N - 70))

echo "--- phase 4: plugin_opt_enabled false ---"
docker run -d --name "$NAME_OFF" -v "$HERE/mosquitto-disabled.conf:/mosquitto/config/mosquitto.conf:ro" "$IMAGE" >/dev/null
sleep 1
docker exec -d "$NAME_OFF" mosquitto_sub -i off-sub -t 'test/#'
sleep 1
docker exec "$NAME_OFF" mosquitto_pub -i off-pub -t test/data -m hello --repeat 10
sleep 3  # > plugin_opt_interval
if docker logs "$NAME_OFF" 2>&1 | grep -q 'client-metrics.*disabled'; then echo "PASS disabled plugin logged it"; else echo "FAIL no 'disabled' log line"; fail=1; fi
regs=$(docker logs "$NAME_OFF" 2>&1 | grep -c 'client-metrics has registered' || true)
if [ "$regs" = 0 ]; then echo "PASS disabled plugin registered no callbacks"; else echo "FAIL disabled plugin registered $regs callbacks"; fail=1; fi
pubs=$(docker exec "$NAME_OFF" mosquitto_sub -t "\$SYS/client-metrics/#" -C 1 -W 3 || true)
if [ -z "$pubs" ]; then echo "PASS disabled plugin publishes nothing"; else echo "FAIL disabled plugin published: $pubs"; fail=1; fi

echo "--- phase 5: OCI image annotations ---"
label() { docker image inspect "$IMAGE" --format "{{ index .Config.Labels \"org.opencontainers.image.$1\" }}"; }
plugin_version=$(docker logs "$NAME" 2>&1 | sed -n 's/.*client-metrics \([0-9][0-9.]*\): publishing.*/\1/p' | head -1)
if [ -n "$plugin_version" ] && [ "$(label version)" = "$plugin_version" ]; then echo "PASS version label=$plugin_version matches plugin"; else echo "FAIL version label='$(label version)', plugin reports '$plugin_version'"; fail=1; fi
for k in title description licenses base.name; do
	v=$(label "$k")
	if [ -n "$v" ] && [ "$v" != "<no value>" ]; then echo "PASS label $k=$v"; else echo "FAIL label $k missing"; fail=1; fi
done
case "$(label base.digest)" in sha256:*) echo "PASS base.digest pinned";; *) echo "FAIL base.digest not set"; fail=1;; esac
case "$(label title)" in mosquitto-client-metrics) ;; *) echo "FAIL title overridden by base image: $(label title)"; fail=1;; esac

echo "--- phase 6: topic tree mirrors JSON (plugin_opt_format both) ---"
# All retained topics under <base>/# must be exactly the flattened JSON at
# <base>: same paths, same values, nothing stale (e.g. connected_for_s of a
# client that went offline). null => no retained topic.
tree_matches_json() { # base topic
	x mosquitto_sub -t "$1" -t "$1/#" -v -W 2 2>/dev/null | python3 -c "
import json, sys
base = sys.argv[1]
doc, topics = None, {}
for line in sys.stdin.read().splitlines():
    t, _, v = line.partition(' ')
    if t == base: doc = json.loads(v)
    else: topics[t[len(base) + 1:]] = v
def flat(o, path, out):
    if isinstance(o, dict):
        for k, v in o.items(): flat(v, path + [k], out)
    elif isinstance(o, list):
        for i, v in enumerate(o): flat(v, path + [str(i)], out)
    elif o is not None:
        out['/'.join(path)] = str(o).lower() if isinstance(o, bool) else str(o)
    return out
want = flat(doc, [], {}) if doc is not None else None
if want is None: print('no JSON at', base); sys.exit(1)
if want != topics:
    for k in sorted(set(want) | set(topics)):
        if want.get(k) != topics.get(k): print(' ', k, 'json=%r topic=%r' % (want.get(k), topics.get(k)))
    sys.exit(1)
print(len(topics), 'topics')
" "$1"
}
for t in clients/fast-client clients/slow-client clients/offline-client clients/keepalive-client clients/publisher summary; do
	if out=$(tree_matches_json "\$SYS/client-metrics/$t"); then echo "PASS $t: $out match JSON"; else echo "FAIL $t topic tree != JSON:"; echo "$out"; fail=1; fi
done

echo "--- phase 7: default format is topics only ---"
docker run -d --name "$NAME_TOPICS" -v "$HERE/mosquitto-topics.conf:/mosquitto/config/mosquitto.conf:ro" "$IMAGE" >/dev/null
sleep 1
docker exec -d "$NAME_TOPICS" mosquitto_sub -i topics-sub -t 'test/#'
sleep 3  # > plugin_opt_interval
tx() { docker exec "$NAME_TOPICS" mosquitto_sub -t "\$SYS/client-metrics/$1" -C 1 -W 2 2>/dev/null || true; }
v=$(tx clients/topics-sub/online)
if [ "$v" = true ]; then echo "PASS clients/topics-sub/online=true"; else echo "FAIL clients/topics-sub/online='$v'"; fail=1; fi
v=$(tx clients/topics-sub/totals/connects)
if [ "$v" = 1 ]; then echo "PASS clients/topics-sub/totals/connects=1"; else echo "FAIL clients/topics-sub/totals/connects='$v'"; fail=1; fi
v=$(tx summary/clients_online)
if [ -n "$v" ]; then echo "PASS summary/clients_online=$v"; else echo "FAIL summary/clients_online missing"; fail=1; fi
v=$(tx clients/topics-sub)$(tx summary)
if [ -z "$v" ]; then echo "PASS no JSON documents in topics mode"; else echo "FAIL JSON published in topics mode: $v"; fail=1; fi

[ $fail = 0 ] && echo "ALL PASSED"
exit $fail
