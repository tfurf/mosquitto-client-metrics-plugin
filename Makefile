# Standalone build (public plugin API only: no exact drop counts / queue depth).
# For the full-featured build use the Docker image (docker/Dockerfile), which
# compiles inside the Mosquitto source tree with the broker's own flags.
#
#   make MOSQUITTO_INCLUDE=/path/to/mosquitto-2.1.x/include
#
MOSQUITTO_INCLUDE ?= /usr/include
UTHASH_INCLUDE ?= /usr/include
CFLAGS ?= -O2 -g
CFLAGS += -Wall -Wextra -fPIC -std=gnu99 -I$(MOSQUITTO_INCLUDE) -I$(UTHASH_INCLUDE)
LDFLAGS += -shared

TARGET = mosquitto_client_metrics.so

all: $(TARGET)

$(TARGET): src/mosquitto_client_metrics.c
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $<

clean:
	rm -f $(TARGET)

.PHONY: all clean
