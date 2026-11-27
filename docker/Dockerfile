# Builds mosquitto_client_metrics.so inside the Mosquitto source tree using the
# same base image, packages and CMake invocation as the official
# eclipse-mosquitto image, so the plugin's view of broker internals matches the
# broker binary. The plugin also verifies this at runtime.
#
# Bump MOSQUITTO_VERSION + MOSQUITTO_SHA256 + ALPINE_VERSION together, matching
# https://github.com/eclipse-mosquitto/mosquitto/blob/master/docker/2.1-alpine/Dockerfile
# and re-pin both base image digests (OCI index digests):
#   docker buildx imagetools inspect <image:tag>
#
# The output is a standard OCI image (see README: Building OCI images).

ARG MOSQUITTO_VERSION=2.1.4
ARG ALPINE_VERSION=3.24
ARG ALPINE_DIGEST=sha256:f59abe656b692d901e184be3445e084786602108599d2a51d229c0f5a48c71bf
ARG MOSQUITTO_DIGEST=sha256:fa12172a051310efe5bcb7a6a11b77bd5faacb82cbacd7742aa758bf024fd8e4

FROM alpine:${ALPINE_VERSION}@${ALPINE_DIGEST} AS build
ARG MOSQUITTO_VERSION
ARG MOSQUITTO_SHA256=ca30b26ccd3041c0e11fc407fe4f2e62bc445e8d8dea94a083cc60c09bd5ea30

# Unpinned on purpose: must match the packages upstream's image was built with.
# hadolint ignore=DL3018
RUN apk --no-cache add \
        argon2-dev build-base cjson-dev cjson-static cmake cunit-dev gtest-dev \
        libedit-dev libmicrohttpd-dev linux-headers openssl-dev pkgconf samurai \
        sqlite-dev uthash-dev util-linux-dev

RUN wget -q https://mosquitto.org/files/source/mosquitto-${MOSQUITTO_VERSION}.tar.gz -O /tmp/mosq.tar.gz \
    && printf '%s  %s\n' "${MOSQUITTO_SHA256}" /tmp/mosq.tar.gz > /tmp/mosq.sha256 \
    && sha256sum -c /tmp/mosq.sha256 \
    && mkdir -p /build/mosq \
    && tar --strip=1 -xf /tmp/mosq.tar.gz -C /build/mosq

COPY src/ /build/mosq/plugins/client-metrics/
RUN echo 'add_subdirectory(client-metrics)' >> /build/mosq/plugins/CMakeLists.txt \
    && cmake -G Ninja \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX=/usr \
        -DHTTP_API_DIR='\"/usr/share/mosquitto/dashboard\"' \
        -DWITH_DOCS=OFF \
        -S /build/mosq -B /build/mosq/build \
    && cmake --build /build/mosq/build --target mosquitto_client_metrics \
    && strip /build/mosq/build/plugins/client-metrics/mosquitto_client_metrics.so

FROM eclipse-mosquitto:${MOSQUITTO_VERSION}-alpine@${MOSQUITTO_DIGEST}
ARG MOSQUITTO_VERSION
ARG MOSQUITTO_DIGEST
# Must match PLUGIN_VERSION in src/mosquitto_client_metrics.c (checked by the test).
ARG PLUGIN_VERSION=1.0.0
# Set per build, e.g. --build-arg REVISION=$(git rev-parse HEAD); empty by default
# so builds stay reproducible.
ARG REVISION=""
ARG CREATED=""
ARG SOURCE_URL=""

# OCI image-spec annotations. All base-image keys are overridden so the image
# doesn't claim to be plain eclipse-mosquitto.
LABEL org.opencontainers.image.title="mosquitto-client-metrics" \
      org.opencontainers.image.description="Eclipse Mosquitto ${MOSQUITTO_VERSION} with the client-metrics plugin (per-client drops, queues, disconnect reasons)" \
      org.opencontainers.image.version="${PLUGIN_VERSION}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.created="${CREATED}" \
      org.opencontainers.image.source="${SOURCE_URL}" \
      org.opencontainers.image.url="${SOURCE_URL}" \
      org.opencontainers.image.documentation="${SOURCE_URL}" \
      org.opencontainers.image.authors="" \
      org.opencontainers.image.licenses="EPL-2.0 OR BSD-3-Clause" \
      org.opencontainers.image.base.name="docker.io/library/eclipse-mosquitto:${MOSQUITTO_VERSION}-alpine" \
      org.opencontainers.image.base.digest="${MOSQUITTO_DIGEST}"

COPY --from=build /build/mosq/build/plugins/client-metrics/mosquitto_client_metrics.so /usr/lib/mosquitto_client_metrics.so
COPY docker/mosquitto.conf /mosquitto/config/mosquitto.conf
