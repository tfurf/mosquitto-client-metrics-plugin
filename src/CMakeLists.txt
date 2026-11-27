# Built in-tree: this directory is copied to <mosquitto-src>/plugins/client-metrics
# and added with add_subdirectory() from plugins/CMakeLists.txt.
set(PLUGIN_NAME mosquitto_client_metrics)

add_mosquitto_plugin_no_install("${PLUGIN_NAME}" "${PLUGIN_NAME}.c" "${mosquitto_SOURCE_DIR}/deps" "")

option(CLIENT_METRICS_WITH_INTERNALS
	"Read broker-internal per-client state (exact drop counts, queue depth)" ON)

if(CLIENT_METRICS_WITH_INTERNALS)
	# Compile with exactly the broker's definitions and include paths so our
	# view of struct mosquitto matches the broker binary built from this tree.
	target_compile_definitions(${PLUGIN_NAME} PRIVATE
		CM_WITH_INTERNALS
		$<TARGET_PROPERTY:mosquitto,COMPILE_DEFINITIONS>
	)
	target_include_directories(${PLUGIN_NAME} PRIVATE
		$<TARGET_PROPERTY:mosquitto,INCLUDE_DIRECTORIES>
		"${mosquitto_SOURCE_DIR}/lib"
		"${mosquitto_SOURCE_DIR}/src"
	)
endif()
