# Builds mosquitto_client_metrics.so inside the Mosquitto source tree using the
# same base image, packages and CMake invocation as the official
# eclipse-mosquitto image, so the plugin's view of broker internals matches the
# broker binary. The plugin also verifies this at runtime.
#
# Bump MOSQUITTO_VERSION + MOSQUITTO_SHA256 + ALPINE_VERSION together, matching
# https://github.com/eclipse-mosquitto/mosquitto/blob/master/docker/2.1-alpine/Dockerfile

ARG MOSQUITTO_VERSION=2.1.4
ARG ALPINE_VERSION=3.24

FROM alpine:${ALPINE_VERSION} AS build
ARG MOSQUITTO_VERSION
ARG MOSQUITTO_SHA256=ca30b26ccd3041c0e11fc407fe4f2e62bc445e8d8dea94a083cc60c09bd5ea30
SHELL ["/bin/ash", "-eo", "pipefail", "-c"]

# Unpinned on purpose: must match the packages upstream's image was built with.
# hadolint ignore=DL3018
RUN apk --no-cache add \
        argon2-dev build-base cjson-dev cjson-static cmake cunit-dev gtest-dev \
        libedit-dev libmicrohttpd-dev linux-headers openssl-dev pkgconf samurai \
        sqlite-dev uthash-dev util-linux-dev

RUN wget -q https://mosquitto.org/files/source/mosquitto-${MOSQUITTO_VERSION}.tar.gz -O /tmp/mosq.tar.gz \
    && echo "${MOSQUITTO_SHA256}  /tmp/mosq.tar.gz" | sha256sum -c - \
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

FROM eclipse-mosquitto:${MOSQUITTO_VERSION}-alpine
COPY --from=build /build/mosq/build/plugins/client-metrics/mosquitto_client_metrics.so /usr/lib/mosquitto_client_metrics.so
COPY docker/mosquitto.conf /mosquitto/config/mosquitto.conf
