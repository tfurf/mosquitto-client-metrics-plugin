#!/bin/sh
# End-to-end test: provokes known drop scenarios and checks the plugin's numbers.
# Usage: test/run-test.sh   (requires the image built by docker/Dockerfile)
set -eu
IMAGE=${IMAGE:-mosquitto-client-metrics:2.1.4}
NAME=cm-test-$$
N=500
HERE=$(cd "$(dirname "$0")" && pwd)

# shellcheck disable=SC2329 # invoked via trap
cleanup() { docker rm -f "$NAME" >/dev/null 2>&1 || true; }
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
[ $fail = 0 ] && echo "ALL PASSED"
exit $fail
