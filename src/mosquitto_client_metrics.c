/*
 * mosquitto_client_metrics - per-client metrics plugin for Mosquitto 2.1+
 *
 * Tracks, per client id:
 *   - connection lifecycle (connects, disconnect reasons such as keepalive
 *     timeouts / lost connections / protocol errors)
 *   - messages and payload bytes received from the client (MESSAGE_IN)
 *   - messages and payload bytes sent to the client (MESSAGE_OUT)
 *   - messages routed to the client by the broker (ACL READ checks) and how
 *     many of those were not written to the socket immediately ("deferred")
 *
 * When built with CM_WITH_INTERNALS (i.e. inside the Mosquitto source tree with
 * the broker's own compile definitions), it additionally reads the broker's
 * per-client state to report:
 *   - exact count of messages the broker dropped for the client
 *     (queue / inflight limits reached)
 *   - current outgoing/incoming inflight and queue depth, and the number of
 *     packets waiting to be written to the client's socket
 * The internal struct layout is verified at runtime against the public
 * accessor functions; on mismatch these extras are disabled automatically.
 *
 * Metrics are published as one JSON document per client to
 *   <topic_prefix>/clients/<client id>
 * plus a broker-wide summary (top offenders) at
 *   <topic_prefix>/summary
 *
 * Plugin options (all optional):
 *   plugin_opt_topic_prefix       default "$SYS/client-metrics"
 *   plugin_opt_interval           publish interval in seconds, default 10
 *   plugin_opt_offline_retention  seconds to keep offline clients, default 3600
 *   plugin_opt_retain             "true"/"false", default true
 *   plugin_opt_summary_top        number of clients in summary, default 10
 *   plugin_opt_log_drops          "true"/"false", log clients that dropped
 *                                 messages each interval, default true
 *
 * IMPORTANT: load this plugin BEFORE any authentication/ACL plugin. Mosquitto
 * stops calling ACL callbacks at the first plugin that returns a decision, so
 * routed/drop accounting only works if this plugin sees the check first. This
 * plugin always returns MOSQ_ERR_PLUGIN_IGNORE and never influences access.
 *
 * SPDX-License-Identifier: EPL-2.0 OR EDL-1.0
 */

#include <inttypes.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <time.h>

#ifdef CM_WITH_INTERNALS
   /* Must come before uthash.h: config.h sets uthash's allocator macros. */
#  include "mosquitto_internal.h"
#  ifndef INVALID_SOCKET
#    define INVALID_SOCKET (-1)
#  endif
#endif

#include <uthash.h>

#include "mosquitto.h"

#define PLUGIN_NAME "client-metrics"
#define PLUGIN_VERSION "1.0.0"

#ifndef UNUSED
#  define UNUSED(A) (void)(A)
#endif

MOSQUITTO_PLUGIN_DECLARE_VERSION(5);

/* ------------------------------------------------------------------------- */
/* Disconnect reason classification                                          */
/* ------------------------------------------------------------------------- */

enum dc_reason {
	DC_CLEAN = 0,       /* client sent DISCONNECT */
	DC_KEEPALIVE,       /* broker saw no traffic within 1.5x keepalive */
	DC_CONN_LOST,       /* socket closed / reset / errno / TLS failure */
	DC_PROTOCOL,        /* malformed / oversize / protocol violation */
	DC_TAKEN_OVER,      /* another connection with same client id */
	DC_ADMIN,           /* kicked by admin / plugin */
	DC_AUTH,            /* authentication / authorisation failure */
	DC_OTHER,
	DC_COUNT
};

static const char *dc_names[DC_COUNT] = {
	"clean", "keepalive_timeout", "conn_lost", "protocol_error",
	"session_taken_over", "administrative", "auth", "other"
};

static enum dc_reason classify_reason(int reason)
{
	switch(reason){
		case MOSQ_ERR_SUCCESS:
			return DC_CLEAN;
		case MOSQ_ERR_KEEPALIVE:
			return DC_KEEPALIVE;
		case MOSQ_ERR_CONN_LOST:
		case MOSQ_ERR_NO_CONN:
		case MOSQ_ERR_ERRNO:
		case MOSQ_ERR_TLS:
		case MOSQ_ERR_TLS_HANDSHAKE:
		case MOSQ_ERR_TIMEOUT:
			return DC_CONN_LOST;
		case MOSQ_ERR_PROTOCOL:
		case MOSQ_ERR_MALFORMED_PACKET:
		case MOSQ_ERR_OVERSIZE_PACKET:
		case MOSQ_ERR_PAYLOAD_SIZE:
		case MOSQ_ERR_DUPLICATE_PROPERTY:
		case MOSQ_ERR_RECEIVE_MAXIMUM_EXCEEDED:
		case MOSQ_ERR_TOPIC_ALIAS_INVALID:
		case MOSQ_ERR_PACKET_ID_IN_USE:
		case MOSQ_ERR_PACKET_ID_NOT_FOUND:
		case MOSQ_ERR_QOS_NOT_SUPPORTED:
		case MOSQ_ERR_PROXY:
			return DC_PROTOCOL;
		case MOSQ_ERR_SESSION_TAKEN_OVER:
			return DC_TAKEN_OVER;
		case MOSQ_ERR_ADMINISTRATIVE_ACTION:
			return DC_ADMIN;
		case MOSQ_ERR_AUTH:
		case MOSQ_ERR_ACL_DENIED:
		case MOSQ_ERR_BANNED:
		case MOSQ_ERR_BAD_USERNAME_OR_PASSWORD:
			return DC_AUTH;
		default:
			return DC_OTHER;
	}
}

/* ------------------------------------------------------------------------- */
/* Per-client state                                                          */
/* ------------------------------------------------------------------------- */

/* Counters that are also reported as per-interval deltas. */
struct counters {
	uint64_t msgs_in;
	uint64_t bytes_in;
	uint64_t msgs_out;
	uint64_t bytes_out;
	uint64_t routed;          /* routed to client while it was online */
	uint64_t routed_offline;  /* routed to client while it was offline */
	uint64_t deferred;        /* routed while online but not written immediately */
	uint64_t dropped;         /* [internals] broker discarded (limits reached) */
	uint64_t dropped_offline; /* [internals] discarded while offline (queue full) */
	uint64_t skipped_offline; /* [internals] not queued while offline (QoS 0) */
	uint64_t disconnects;
	uint64_t connects;
};

struct client_entry {
	char *id;                 /* hash key */
	char *topic;              /* full metrics topic for this client */
	char *username;
	char *address;
	int protocol_version;
	int keepalive;
	bool online;
	bool dirty;               /* changed since last publish */
	bool published;           /* has a retained message out there */
	time_t first_seen;
	time_t connected_at;
	time_t offline_since;
	int last_reason;          /* raw MOSQ_ERR_* code of last disconnect */
	uint64_t dc[DC_COUNT];
	struct counters c;
	struct counters prev;     /* snapshot at last publish, for deltas */
	UT_hash_handle hh;
};

/* A message routing in progress: set on ACL READ, resolved on the next event. */
struct pending_route {
	bool active;
	bool online;
	bool out_seen;
	uint8_t qos;
	struct mosquitto *ctx;
	struct client_entry *entry;
	const void *payload;
	uint32_t payloadlen;
#ifdef CM_WITH_INTERNALS
	uint64_t cmsg_before;
#endif
};

struct plugin_data {
	mosquitto_plugin_id_t *pid;
	char *prefix;
	char *summary_topic;
	int interval;
	int offline_retention;
	int summary_top;
	bool retain;
	bool log_drops;
	time_t next_publish;
	struct client_entry *clients;
	struct pending_route pending;
#ifdef CM_WITH_INTERNALS
	int layout_state;         /* 0 = unchecked, 1 = ok, -1 = mismatch */
#endif
};

static struct plugin_data *g_data = NULL;

/* ------------------------------------------------------------------------- */
/* Helpers                                                                   */
/* ------------------------------------------------------------------------- */

static char *xstrdup(const char *s)
{
	return s ? strdup(s) : NULL;
}

static void replace_str(char **dst, const char *src)
{
	if(*dst && src && !strcmp(*dst, src)){
		return;
	}
	free(*dst);
	*dst = xstrdup(src);
}

/* Build "<prefix>/clients/<id>" with MQTT wildcard characters replaced. */
static char *make_client_topic(const char *prefix, const char *id)
{
	size_t len = strlen(prefix) + strlen("/clients/") + strlen(id) + 1;
	char *t = malloc(len);
	if(!t){
		return NULL;
	}
	snprintf(t, len, "%s/clients/%s", prefix, id);
	for(char *p = t + strlen(prefix) + strlen("/clients/"); *p; p++){
		if(*p == '+' || *p == '#'){
			*p = '_';
		}
	}
	return t;
}

/* Small growable buffer for JSON output. */
struct buf {
	char *s;
	size_t len;
	size_t cap;
	bool err;
};

static void buf_printf(struct buf *b, const char *fmt, ...) __attribute__((format(printf, 2, 3)));

static void buf_printf(struct buf *b, const char *fmt, ...)
{
	va_list ap;
	int n;

	if(b->err){
		return;
	}
	for(;;){
		size_t avail = b->cap - b->len;
		va_start(ap, fmt);
		n = vsnprintf(b->s ? b->s + b->len : NULL, b->s ? avail : 0, fmt, ap);
		va_end(ap);
		if(n < 0){
			b->err = true;
			return;
		}
		if(b->s && (size_t)n < avail){
			b->len += (size_t)n;
			return;
		}
		size_t ncap = b->cap ? b->cap * 2 : 1024;
		while(ncap < b->len + (size_t)n + 1){
			ncap *= 2;
		}
		char *ns = realloc(b->s, ncap);
		if(!ns){
			b->err = true;
			return;
		}
		b->s = ns;
		b->cap = ncap;
	}
}

static void buf_json_str(struct buf *b, const char *s)
{
	if(!s){
		buf_printf(b, "null");
		return;
	}
	buf_printf(b, "\"");
	for(const unsigned char *p = (const unsigned char *)s; *p; p++){
		switch(*p){
			case '"':  buf_printf(b, "\\\""); break;
			case '\\': buf_printf(b, "\\\\"); break;
			case '\n': buf_printf(b, "\\n"); break;
			case '\r': buf_printf(b, "\\r"); break;
			case '\t': buf_printf(b, "\\t"); break;
			default:
				if(*p < 0x20){
					buf_printf(b, "\\u%04x", *p);
				}else{
					buf_printf(b, "%c", *p);
				}
		}
	}
	buf_printf(b, "\"");
}

/* ------------------------------------------------------------------------- */
/* Broker internals (optional)                                               */
/* ------------------------------------------------------------------------- */

#ifdef CM_WITH_INTERNALS
/* Verify our view of struct mosquitto matches the running broker by comparing
 * directly-read fields against the broker's own accessor functions. Fields are
 * chosen to bracket everything we read (last_cmsg_id ... subs_count). */
static bool layout_check(struct mosquitto *ctx)
{
	if(g_data->layout_state != 0){
		return g_data->layout_state == 1;
	}
	if(!ctx || !mosquitto_client_id(ctx)){
		return false;
	}
	bool ok = ctx->id == mosquitto_client_id(ctx)
			&& ctx->username == mosquitto_client_username(ctx)
			&& ctx->address == mosquitto_client_address(ctx)
			&& ctx->id_hashv == mosquitto_client_id_hashv(ctx)
			&& (int)ctx->keepalive == mosquitto_client_keepalive(ctx)
			&& ctx->subs_count == mosquitto_client_sub_count(ctx);

	g_data->layout_state = ok ? 1 : -1;
	if(ok){
		mosquitto_log_printf(MOSQ_LOG_INFO, PLUGIN_NAME ": broker internals layout verified, "
				"exact drop counts and queue depth enabled.");
	}else{
		mosquitto_log_printf(MOSQ_LOG_WARNING, PLUGIN_NAME ": broker internals layout MISMATCH "
				"(plugin built against a different Mosquitto version/config). "
				"Drop counts and queue depth disabled; public-API metrics continue.");
	}
	return ok;
}

static bool internals_ok(void)
{
	return g_data->layout_state == 1;
}
#endif

/* ------------------------------------------------------------------------- */
/* Client table                                                              */
/* ------------------------------------------------------------------------- */

static struct client_entry *entry_get(const char *id, bool create)
{
	struct client_entry *e = NULL;

	if(!id){
		return NULL;
	}
	HASH_FIND_STR(g_data->clients, id, e);
	if(e || !create){
		return e;
	}
	e = calloc(1, sizeof(*e));
	if(!e){
		return NULL;
	}
	e->id = strdup(id);
	e->topic = make_client_topic(g_data->prefix, id);
	if(!e->id || !e->topic){
		free(e->id);
		free(e->topic);
		free(e);
		return NULL;
	}
	e->first_seen = time(NULL);
	e->last_reason = -1;
	HASH_ADD_KEYPTR(hh, g_data->clients, e->id, strlen(e->id), e);
	return e;
}

static void entry_free(struct client_entry *e)
{
	free(e->id);
	free(e->topic);
	free(e->username);
	free(e->address);
	free(e);
}

static struct client_entry *entry_for_ctx(struct mosquitto *ctx, bool create)
{
	return entry_get(mosquitto_client_id(ctx), create);
}

/* ------------------------------------------------------------------------- */
/* Routing / drop detection                                                  */
/* ------------------------------------------------------------------------- */

/* Resolve the outcome of the previously routed message. Called at the start
 * of every callback: Mosquitto is single threaded and routes a message to a
 * client synchronously right after the ACL READ check, so by the time any
 * other event fires the previous routing decision has been made. */
static void pending_resolve(void)
{
	struct pending_route *p = &g_data->pending;
	struct client_entry *e = p->entry;

	if(!p->active){
		return;
	}
	p->active = false;

	if(p->out_seen){
		return; /* written to socket immediately */
	}

	e->dirty = true;
	if(p->online){
		e->c.deferred++;
	}

#ifdef CM_WITH_INTERNALS
	/* Resolution may happen on a later loop iteration (next tick), so make
	 * sure the context still exists before reading it. */
	if(internals_ok() && mosquitto_client(e->id) == p->ctx){
		/* last_cmsg_id is incremented exactly once each time the broker
		 * accepts a message for this client (inflight or queued). If it did
		 * not move, the message was not accepted. */
		if(p->ctx->last_cmsg_id == p->cmsg_before){
			if(p->online){
				e->c.dropped++;
			}else if(p->qos > 0 && p->ctx->is_dropping){
				e->c.dropped_offline++;
			}else{
				e->c.skipped_offline++;
			}
		}
	}
#endif
}

/* ------------------------------------------------------------------------- */
/* Callbacks                                                                 */
/* ------------------------------------------------------------------------- */

static int cb_acl_check(int event, void *event_data, void *userdata)
{
	struct mosquitto_evt_acl_check *ed = event_data;
	struct client_entry *e;

	UNUSED(event);
	UNUSED(userdata);

	pending_resolve();

	if(ed->access != MOSQ_ACL_READ || !ed->topic || ed->topic[0] == '$'){
		return MOSQ_ERR_PLUGIN_IGNORE;
	}
	e = entry_for_ctx(ed->client, true);
	if(!e){
		return MOSQ_ERR_PLUGIN_IGNORE;
	}

#ifdef CM_WITH_INTERNALS
	if(layout_check(ed->client)){
		/* On reconnect the broker re-checks ACLs on already-queued messages
		 * before the CONNECT event fires; the socket is already open then.
		 * Those are not new routings, so ignore them. */
		if(!e->online && ed->client->sock != INVALID_SOCKET){
			return MOSQ_ERR_PLUGIN_IGNORE;
		}
	}
#endif

	e->dirty = true;
	if(e->online){
		e->c.routed++;
	}else{
		e->c.routed_offline++;
	}

	struct pending_route *p = &g_data->pending;
	p->active = true;
	p->online = e->online;
	p->out_seen = false;
	p->qos = ed->qos;
	p->ctx = ed->client;
	p->entry = e;
	p->payload = ed->payload;
	p->payloadlen = ed->payloadlen;
#ifdef CM_WITH_INTERNALS
	if(internals_ok()){
		p->cmsg_before = ed->client->last_cmsg_id;
	}
#endif

	return MOSQ_ERR_PLUGIN_IGNORE;
}

static int cb_message_out(int event, void *event_data, void *userdata)
{
	struct mosquitto_evt_message *ed = event_data;
	struct pending_route *p = &g_data->pending;
	struct client_entry *e;

	UNUSED(event);
	UNUSED(userdata);

	if(!ed->topic || ed->topic[0] == '$'){
		return MOSQ_ERR_SUCCESS;
	}

	/* A MESSAGE_OUT for the same client and same stored payload during a
	 * pending routing means it went straight to the socket. Anything else
	 * (e.g. an older queued message being flushed) resolves the pending one. */
	if(p->active){
		if(p->ctx == ed->client && p->payload == ed->payload && p->payloadlen == ed->payloadlen){
			p->out_seen = true;
		}
	}

	e = (p->active && p->ctx == ed->client) ? p->entry : entry_for_ctx(ed->client, true);
	if(e){
		e->c.msgs_out++;
		e->c.bytes_out += ed->payloadlen;
		e->dirty = true;
	}
	return MOSQ_ERR_SUCCESS;
}

static int cb_message_in(int event, void *event_data, void *userdata)
{
	struct mosquitto_evt_message *ed = event_data;
	struct client_entry *e;

	UNUSED(event);
	UNUSED(userdata);

	pending_resolve();

	if(!ed->client || !ed->topic || ed->topic[0] == '$'){
		return MOSQ_ERR_SUCCESS;
	}
	e = entry_for_ctx(ed->client, true);
	if(e){
		e->c.msgs_in++;
		e->c.bytes_in += ed->payloadlen;
		e->dirty = true;
	}
	return MOSQ_ERR_SUCCESS;
}

static int cb_connect(int event, void *event_data, void *userdata)
{
	struct mosquitto_evt_connect *ed = event_data;
	struct client_entry *e;

	UNUSED(event);
	UNUSED(userdata);

	pending_resolve();

#ifdef CM_WITH_INTERNALS
	layout_check(ed->client);
#endif

	e = entry_for_ctx(ed->client, true);
	if(!e){
		return MOSQ_ERR_SUCCESS;
	}
	e->online = true;
	e->dirty = true;
	e->connected_at = time(NULL);
	e->offline_since = 0;
	e->c.connects++;
	e->protocol_version = mosquitto_client_protocol_version(ed->client);
	e->keepalive = mosquitto_client_keepalive(ed->client);
	replace_str(&e->username, mosquitto_client_username(ed->client));
	replace_str(&e->address, mosquitto_client_address(ed->client));
	return MOSQ_ERR_SUCCESS;
}

static void handle_offline(struct mosquitto *ctx, int reason)
{
	struct client_entry *e;

	pending_resolve();

	e = entry_for_ctx(ctx, false);
	if(!e){
		return;
	}
	/* Session takeover: the old connection's offline event arrives after the
	 * new connection is already registered under the same id. Only count it. */
	enum dc_reason r = classify_reason(reason);
	e->dc[r]++;
	e->c.disconnects++;
	e->last_reason = reason;
	e->dirty = true;
	if(r != DC_TAKEN_OVER){
		e->online = false;
		e->offline_since = time(NULL);
	}
}

static int cb_disconnect(int event, void *event_data, void *userdata)
{
	struct mosquitto_evt_disconnect *ed = event_data;
	UNUSED(event);
	UNUSED(userdata);
	handle_offline(ed->client, ed->reason);
	return MOSQ_ERR_SUCCESS;
}

static int cb_client_offline(int event, void *event_data, void *userdata)
{
	struct mosquitto_evt_client_offline *ed = event_data;
	UNUSED(event);
	UNUSED(userdata);
	handle_offline(ed->client, ed->reason);
	return MOSQ_ERR_SUCCESS;
}

/* ------------------------------------------------------------------------- */
/* Publishing                                                                */
/* ------------------------------------------------------------------------- */

#define D(f) (e->c.f - e->prev.f)

static void publish_client(struct client_entry *e, time_t now)
{
	struct buf b = {0};

	buf_printf(&b, "{\"client_id\":");
	buf_json_str(&b, e->id);
	buf_printf(&b, ",\"username\":");
	buf_json_str(&b, e->username);
	buf_printf(&b, ",\"address\":");
	buf_json_str(&b, e->address);
	buf_printf(&b, ",\"protocol_version\":%d,\"keepalive\":%d", e->protocol_version, e->keepalive);
	buf_printf(&b, ",\"online\":%s", e->online ? "true" : "false");
	buf_printf(&b, ",\"timestamp\":%lld", (long long)now);
	if(e->online){
		buf_printf(&b, ",\"connected_for_s\":%lld", (long long)(now - e->connected_at));
	}else if(e->offline_since){
		buf_printf(&b, ",\"offline_for_s\":%lld", (long long)(now - e->offline_since));
	}

	buf_printf(&b, ",\"totals\":{"
			"\"connects\":%" PRIu64 ",\"disconnects\":%" PRIu64
			",\"msgs_in\":%" PRIu64 ",\"bytes_in\":%" PRIu64
			",\"msgs_out\":%" PRIu64 ",\"bytes_out\":%" PRIu64
			",\"routed\":%" PRIu64 ",\"routed_offline\":%" PRIu64
			",\"deferred\":%" PRIu64,
			e->c.connects, e->c.disconnects,
			e->c.msgs_in, e->c.bytes_in, e->c.msgs_out, e->c.bytes_out,
			e->c.routed, e->c.routed_offline, e->c.deferred);
#ifdef CM_WITH_INTERNALS
	if(internals_ok()){
		buf_printf(&b, ",\"dropped\":%" PRIu64 ",\"dropped_offline\":%" PRIu64
				",\"skipped_offline_qos0\":%" PRIu64,
				e->c.dropped, e->c.dropped_offline, e->c.skipped_offline);
	}
#endif
	buf_printf(&b, "}");

	buf_printf(&b, ",\"interval\":{\"seconds\":%d"
			",\"msgs_in\":%" PRIu64 ",\"msgs_out\":%" PRIu64
			",\"routed\":%" PRIu64 ",\"deferred\":%" PRIu64
			",\"disconnects\":%" PRIu64,
			g_data->interval, D(msgs_in), D(msgs_out), D(routed), D(deferred), D(disconnects));
#ifdef CM_WITH_INTERNALS
	if(internals_ok()){
		buf_printf(&b, ",\"dropped\":%" PRIu64 ",\"dropped_offline\":%" PRIu64,
				D(dropped), D(dropped_offline));
	}
#endif
	buf_printf(&b, "}");

	buf_printf(&b, ",\"disconnect_reasons\":{");
	for(int i = 0; i < DC_COUNT; i++){
		buf_printf(&b, "%s\"%s\":%" PRIu64, i ? "," : "", dc_names[i], e->dc[i]);
	}
	buf_printf(&b, "}");
	if(e->last_reason >= 0){
		buf_printf(&b, ",\"last_disconnect\":{\"reason\":\"%s\",\"code\":%d}",
				dc_names[classify_reason(e->last_reason)], e->last_reason);
	}

#ifdef CM_WITH_INTERNALS
	if(internals_ok()){
		struct mosquitto *ctx = mosquitto_client(e->id);
		if(ctx){
			buf_printf(&b, ",\"queues\":{"
					"\"out_inflight\":%d,\"out_inflight_bytes\":%ld"
					",\"out_queued\":%d,\"out_queued_bytes\":%ld"
					",\"out_inflight_max\":%u"
					",\"in_inflight\":%d,\"in_queued\":%d"
					",\"socket_packets_pending\":%d"
					",\"dropping\":%s,\"broker_dropped_in\":%" PRIu64 "}",
					ctx->msgs_out.inflight_count, ctx->msgs_out.inflight_bytes,
					ctx->msgs_out.queued_count, ctx->msgs_out.queued_bytes,
					(unsigned)ctx->msgs_out.inflight_maximum,
					ctx->msgs_in.inflight_count, ctx->msgs_in.queued_count,
					ctx->out_packet_count,
					ctx->is_dropping ? "true" : "false",
					ctx->stats.messages_dropped);
		}
	}
#endif
	buf_printf(&b, "}");

	if(!b.err && b.s){
		mosquitto_broker_publish_copy(NULL, e->topic, (int)b.len, b.s, 0, g_data->retain, NULL);
		e->published = true;
	}
	free(b.s);
}

/* Summary ranking: messages lost first (exact drops if available, otherwise
 * deferred = queued-or-dropped), then abnormal disconnects. */
static uint64_t loss_score(const struct client_entry *e)
{
#ifdef CM_WITH_INTERNALS
	if(internals_ok()){
		return e->c.dropped + e->c.dropped_offline;
	}
#endif
	return e->c.deferred;
}

static uint64_t abnormal_disconnects(const struct client_entry *e)
{
	return e->dc[DC_KEEPALIVE] + e->dc[DC_CONN_LOST] + e->dc[DC_PROTOCOL];
}

static int cmp_score(const void *a, const void *b)
{
	const struct client_entry *ea = *(struct client_entry * const *)a;
	const struct client_entry *eb = *(struct client_entry * const *)b;
	uint64_t la = loss_score(ea), lb = loss_score(eb);
	if(la != lb){
		return (la < lb) - (la > lb);
	}
	uint64_t da = abnormal_disconnects(ea), db_ = abnormal_disconnects(eb);
	return (da < db_) - (da > db_);
}

static void publish_summary(time_t now)
{
	struct buf b = {0};
	struct client_entry *e, *tmp;
	unsigned count = HASH_COUNT(g_data->clients);
	unsigned online = 0;
	uint64_t routed = 0, deferred = 0, dropped = 0, keepalive = 0, lost = 0;
	struct client_entry **arr = NULL;
	unsigned n = 0;

	if(count){
		arr = calloc(count, sizeof(*arr));
	}
	HASH_ITER(hh, g_data->clients, e, tmp){
		online += e->online;
		routed += e->c.routed;
		deferred += e->c.deferred;
		dropped += e->c.dropped + e->c.dropped_offline;
		keepalive += e->dc[DC_KEEPALIVE];
		lost += e->dc[DC_CONN_LOST];
		if(arr && (loss_score(e) > 0 || abnormal_disconnects(e) > 0)){
			arr[n++] = e;
		}
	}
	if(arr && n){
		qsort(arr, n, sizeof(*arr), cmp_score);
	}

	buf_printf(&b, "{\"timestamp\":%lld,\"plugin_version\":\"%s\"", (long long)now, PLUGIN_VERSION);
#ifdef CM_WITH_INTERNALS
	buf_printf(&b, ",\"exact_drop_counts\":%s", internals_ok() ? "true" : "false");
#else
	buf_printf(&b, ",\"exact_drop_counts\":false");
#endif
	buf_printf(&b, ",\"clients_tracked\":%u,\"clients_online\":%u", count, online);
	buf_printf(&b, ",\"routed\":%" PRIu64 ",\"deferred\":%" PRIu64, routed, deferred);
#ifdef CM_WITH_INTERNALS
	if(internals_ok()){
		buf_printf(&b, ",\"dropped\":%" PRIu64, dropped);
	}
#endif
	buf_printf(&b, ",\"keepalive_timeouts\":%" PRIu64 ",\"conn_lost\":%" PRIu64, keepalive, lost);
	buf_printf(&b, ",\"worst_clients\":[");
	for(unsigned i = 0; i < n && i < (unsigned)g_data->summary_top; i++){
		e = arr[i];
		buf_printf(&b, "%s{\"client_id\":", i ? "," : "");
		buf_json_str(&b, e->id);
		buf_printf(&b, ",\"online\":%s,\"deferred\":%" PRIu64
				",\"keepalive_timeouts\":%" PRIu64 ",\"conn_lost\":%" PRIu64,
				e->online ? "true" : "false", e->c.deferred,
				e->dc[DC_KEEPALIVE], e->dc[DC_CONN_LOST]);
#ifdef CM_WITH_INTERNALS
		if(internals_ok()){
			buf_printf(&b, ",\"dropped\":%" PRIu64 ",\"dropped_offline\":%" PRIu64,
					e->c.dropped, e->c.dropped_offline);
		}
#endif
		buf_printf(&b, "}");
	}
	buf_printf(&b, "]}");
	free(arr);

	if(!b.err && b.s){
		mosquitto_broker_publish_copy(NULL, g_data->summary_topic, (int)b.len, b.s, 0, g_data->retain, NULL);
	}
	free(b.s);
}

static void publish_all(time_t now)
{
	struct client_entry *e, *tmp;

	HASH_ITER(hh, g_data->clients, e, tmp){
		/* Expire long-offline clients and clear their retained topic. */
		if(!e->online && e->offline_since
				&& g_data->offline_retention >= 0
				&& now - e->offline_since > g_data->offline_retention){
			if(e->published && g_data->retain){
				mosquitto_broker_publish_copy(NULL, e->topic, 0, NULL, 0, true, NULL);
			}
			if(g_data->pending.entry == e){
				g_data->pending.active = false;
				g_data->pending.entry = NULL;
			}
			HASH_DEL(g_data->clients, e);
			entry_free(e);
			continue;
		}

#ifdef CM_WITH_INTERNALS
		/* Queue depth can change without any event we count, so always
		 * publish online clients when internals are available. */
		if(internals_ok() && e->online){
			e->dirty = true;
		}
#endif
		if(e->dirty || !e->published){
			bool dropped_now = false;
			uint64_t drops = 0;
#ifdef CM_WITH_INTERNALS
			if(internals_ok()){
				drops = D(dropped) + D(dropped_offline);
				dropped_now = drops > 0;
			}
#endif
			if(g_data->log_drops && dropped_now){
				mosquitto_log_printf(MOSQ_LOG_NOTICE,
						PLUGIN_NAME ": client %s (%s) dropped %" PRIu64 " messages in the last %d s "
						"(routed %" PRIu64 ", sent %" PRIu64 ").",
						e->id, e->online ? "online" : "offline", drops, g_data->interval,
						D(routed) + D(routed_offline), D(msgs_out));
			}
			publish_client(e, now);
			e->prev = e->c;
			e->dirty = false;
		}
	}
	publish_summary(now);
}

static int cb_tick(int event, void *event_data, void *userdata)
{
	struct mosquitto_evt_tick *ed = event_data;
	time_t now = time(NULL);

	UNUSED(event);
	UNUSED(userdata);

	pending_resolve();

	if(now >= g_data->next_publish){
		publish_all(now);
		g_data->next_publish = now + g_data->interval;
	}
	ed->next_s = 1;
	ed->next_ms = 0;
	return MOSQ_ERR_SUCCESS;
}

/* ------------------------------------------------------------------------- */
/* Plugin entry points                                                       */
/* ------------------------------------------------------------------------- */

static bool parse_bool(const char *v, bool *out)
{
	if(!strcasecmp(v, "true") || !strcmp(v, "1") || !strcasecmp(v, "yes")){
		*out = true;
		return true;
	}
	if(!strcasecmp(v, "false") || !strcmp(v, "0") || !strcasecmp(v, "no")){
		*out = false;
		return true;
	}
	return false;
}

static bool parse_int(const char *v, int min, int *out)
{
	char *end;
	long l = strtol(v, &end, 10);
	if(*v == '\0' || *end != '\0' || l < min || l > 1000000000L){
		return false;
	}
	*out = (int)l;
	return true;
}

static void data_free(struct plugin_data *d)
{
	struct client_entry *e, *tmp;
	if(!d){
		return;
	}
	HASH_ITER(hh, d->clients, e, tmp){
		HASH_DEL(d->clients, e);
		entry_free(e);
	}
	free(d->prefix);
	free(d->summary_topic);
	free(d);
}

#define REG(EVT, FN) do { \
		int rc_ = mosquitto_callback_register(d->pid, (EVT), (FN), NULL, d); \
		if(rc_){ \
			mosquitto_log_printf(MOSQ_LOG_ERR, PLUGIN_NAME ": failed to register %s (%d).", #EVT, rc_); \
			return rc_; \
		} \
	} while(0)

int mosquitto_plugin_init(mosquitto_plugin_id_t *identifier, void **userdata, struct mosquitto_opt *opts, int opt_count)
{
	struct plugin_data *d;
	const char *prefix = "$SYS/client-metrics";

	if(g_data){
		mosquitto_log_printf(MOSQ_LOG_ERR, PLUGIN_NAME ": may only be loaded once (global plugin, not per listener).");
		return MOSQ_ERR_INVAL;
	}

	d = calloc(1, sizeof(*d));
	if(!d){
		return MOSQ_ERR_NOMEM;
	}
	d->pid = identifier;
	d->interval = 10;
	d->offline_retention = 3600;
	d->summary_top = 10;
	d->retain = true;
	d->log_drops = true;

	for(int i = 0; i < opt_count; i++){
		const char *k = opts[i].key, *v = opts[i].value;
		bool ok = true;
		if(!strcmp(k, "topic_prefix")){
			prefix = v;
			ok = v[0] != '\0' && !strpbrk(v, "+#");
		}else if(!strcmp(k, "interval")){
			ok = parse_int(v, 1, &d->interval);
		}else if(!strcmp(k, "offline_retention")){
			ok = parse_int(v, 0, &d->offline_retention);
		}else if(!strcmp(k, "summary_top")){
			ok = parse_int(v, 0, &d->summary_top);
		}else if(!strcmp(k, "retain")){
			ok = parse_bool(v, &d->retain);
		}else if(!strcmp(k, "log_drops")){
			ok = parse_bool(v, &d->log_drops);
		}else{
			mosquitto_log_printf(MOSQ_LOG_ERR, PLUGIN_NAME ": unknown option '%s'.", k);
			data_free(d);
			return MOSQ_ERR_INVAL;
		}
		if(!ok){
			mosquitto_log_printf(MOSQ_LOG_ERR, PLUGIN_NAME ": invalid value '%s' for option '%s'.", v, k);
			data_free(d);
			return MOSQ_ERR_INVAL;
		}
	}

	d->prefix = strdup(prefix);
	size_t slen = strlen(prefix) + strlen("/summary") + 1;
	d->summary_topic = malloc(slen);
	if(!d->prefix || !d->summary_topic){
		data_free(d);
		return MOSQ_ERR_NOMEM;
	}
	snprintf(d->summary_topic, slen, "%s/summary", prefix);
	d->next_publish = time(NULL) + d->interval;

	g_data = d;
	*userdata = d;

	mosquitto_plugin_set_info(identifier, PLUGIN_NAME, PLUGIN_VERSION);

	REG(MOSQ_EVT_ACL_CHECK, cb_acl_check);
	REG(MOSQ_EVT_MESSAGE_IN, cb_message_in);
	REG(MOSQ_EVT_MESSAGE_OUT, cb_message_out);
	REG(MOSQ_EVT_CONNECT, cb_connect);
	REG(MOSQ_EVT_DISCONNECT, cb_disconnect);
	REG(MOSQ_EVT_CLIENT_OFFLINE, cb_client_offline);
	REG(MOSQ_EVT_TICK, cb_tick);

	mosquitto_log_printf(MOSQ_LOG_INFO, PLUGIN_NAME " " PLUGIN_VERSION ": publishing to %s/... every %d s%s.",
			d->prefix, d->interval,
#ifdef CM_WITH_INTERNALS
			" (internals support compiled in)"
#else
			" (public API only: no exact drop counts)"
#endif
			);
	return MOSQ_ERR_SUCCESS;
}

int mosquitto_plugin_cleanup(void *userdata, struct mosquitto_opt *opts, int opt_count)
{
	UNUSED(opts);
	UNUSED(opt_count);
	/* Callbacks are unregistered automatically by the broker. */
	data_free(userdata);
	g_data = NULL;
	return MOSQ_ERR_SUCCESS;
}
