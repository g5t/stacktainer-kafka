#!/bin/sh
# Start the KRaft broker.
#
# Shared by %runscript and %startscript in image.def so that 'apptainer run' and
# 'apptainer instance start' cannot drift apart. They used to be duplicated blocks,
# and the %runscript copy was dropped in af9b0a6 -- which left 'apptainer run' with
# no broker at all, despite the README documenting it.

# Fail loudly rather than starting a broker that will quietly misbehave.
set -e

# Defaults for anything not supplied via --env. /dev/shm is a tmpfs on every Linux
# host and Apptainer bind-mounts the host /dev by default, so this is RAM-backed
# storage with no extra launch flags and no apptainer.conf changes. Keyed by broker
# port so two instances on one host cannot collide.
: "${BROKER_PORT:=9092}"
: "${CONTROLLER_PORT:=9093}"
: "${KAFKA_STORAGE_DIR:=/dev/shm/kafka-${BROKER_PORT}}"
: "${KAFKA_RETENTION_MS:=300000}"
: "${KAFKA_RETENTION_BYTES:=268435456}"
: "${KAFKA_SEGMENT_MS:=30000}"

if [ -z "${KAFKA_STORAGE_DIR}" ]; then
  echo "KAFKA_STORAGE_DIR is empty -- refusing to start" >&2
  exit 1
fi

data_dir="${KAFKA_STORAGE_DIR}/data"
run_dir="${KAFKA_STORAGE_DIR}/run"
properties="${run_dir}/server.properties"

# Stream data here is ephemeral by definition, and kafka-storage refuses to format a
# directory that is already formatted. Start from nothing every time. Only the two
# subdirectories this script creates are removed, never KAFKA_STORAGE_DIR itself.
rm -rf "${data_dir}" "${run_dir}"
mkdir -p "${data_dir}" "${run_dir}"

# Keep the server's own log4j output beside the data instead of writing it into the
# image overlay.
export LOG_DIR="${run_dir}/logs"

sed -e "s/:9092/:${BROKER_PORT}/g" \
    -e "s/:9093/:${CONTROLLER_PORT}/g" \
    -e "s|^log.dirs=.*|log.dirs=${data_dir}|" \
    -e "s|^log.retention.ms=.*|log.retention.ms=${KAFKA_RETENTION_MS}|" \
    -e "s|^log.retention.bytes=.*|log.retention.bytes=${KAFKA_RETENTION_BYTES}|" \
    -e "s|^log.roll.ms=.*|log.roll.ms=${KAFKA_SEGMENT_MS}|" \
    /server.properties > "${properties}"

echo "Kafka storage: ${data_dir} ($(df -h -P "${data_dir}" | awk 'NR==2 {print $4}') available)"
echo "Kafka retention: ${KAFKA_RETENTION_MS} ms / ${KAFKA_RETENTION_BYTES} bytes per partition"

# One partition can hold KAFKA_RETENTION_BYTES plus the segment it is currently
# appending to, so warn if the filesystem cannot even cover a single busy topic.
# A tmpfs is normally half of host RAM, but containerised or memory-capped hosts
# often ship a 64 MiB /dev/shm -- in which case the broker dies with an opaque
# "No space left on device" partway through a run, which is exactly the class of
# failure this configuration exists to eliminate.
segment_bytes=$(awk -F= '/^log.segment.bytes=/ {print $2}' "${properties}")
available=$(df -P -k "${data_dir}" | awk 'NR==2 {print $4 * 1024}')
needed=$((KAFKA_RETENTION_BYTES + segment_bytes))
if [ "${available}" -lt "${needed}" ]; then
  echo "WARNING: ${data_dir} has only ${available} bytes free; one busy partition can" >&2
  echo "WARNING: reach ${needed} (KAFKA_RETENTION_BYTES=${KAFKA_RETENTION_BYTES} plus a" >&2
  echo "WARNING: ${segment_bytes} byte active segment). The segment size is pinned by" >&2
  echo "WARNING: message.max.bytes and cannot go far below it, so lowering" >&2
  echo "WARNING: KAFKA_RETENTION_BYTES alone will not clear this -- point" >&2
  echo "WARNING: KAFKA_STORAGE_DIR at a larger filesystem instead." >&2
fi

kafka-storage format -t "${CLUSTER_ID}" -c "${properties}"

# Run the broker in the background so this shell stays alive to reclaim the storage
# when 'apptainer instance stop' sends us SIGTERM.
kafka-server-start "${properties}" &
kafka_pid=$!
trap 'kill -TERM "${kafka_pid}" 2>/dev/null; wait "${kafka_pid}"; rm -rf "${data_dir}" "${run_dir}"; exit 0' INT TERM
set +e
wait "${kafka_pid}"
status=$?
rm -rf "${data_dir}" "${run_dir}"
exit ${status}


